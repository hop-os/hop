//! `hoplockserver-hopos`: de compare-and-swap-server als bewoner van HopOS.
//!
//! Hetzelfde protocol als de host-bin, byte voor byte (de toestandsmachine
//! en de leanhttp-naad van de bibliotheek), in een slot van HopOS: Hop
//! plaatst hem als job met een gepubliceerde poort, en de kern zet die poort
//! van de uplink door naar het slot (DNAT). Een cluster zonder host kan zo
//! zijn eigen lease-server draaien.
//!
//! Uit de env van het slot (de jobspec):
//!
//! - `ER_PORT_HTTP`: de poort, uit `"ports": {"http": 8090}` (standaard 8090);
//! - `HOPLOCK_API_KEY`: de verplichte `X-API-Key` (leeg: zonder, luid);
//! - `HOPLOCK_DATA`: de map van de store (standaard `/data`), bedoeld als het
//!   volume van de job (`"volumes": {"/volumes/hoplock": "/data"}`). Hop
//!   v3.0.0-alpha.10 weigert een job met volumes op HopOS nog (`START_SLOT`
//!   draagt geen mounts); zonder volume ligt `/data` in de eigen root van
//!   het slot, en die maakt de kern bij elke start leeg. Gemeten 30-09 op
//!   QEMU (tools/qemu-test.sh): alles groen, behalve een sleutel over een
//!   herstart van de job heen.
//!
//! De store staat op dat volume via de bestandscalls van de system-API
//! (`hoplockserver::volume`): stat, lezen, schrijven, verwijderen, lijsten.
//! De kern heeft geen fsync en geen rename; de commit van hopfs (elke 10 s en
//! bij de stop van het slot) is de fsync, en elk record draagt een
//! controlegetal zodat een half record nooit als lease terugkomt.
//!
//! De vorm (handboek §1 en §2):
//!
//! - **de acceptor** (`applibhttp` uit HopOS) geeft elke verbinding als
//!   waarde aan een vrije werker uit een vaste pool van [`WORKERS`], en een
//!   verbinding die na haar antwoord mag blijven (keep-alive), wacht in zijn
//!   park tot het volgende verzoek: niet bij een werker;
//! - **een werker** bedient één verzoek met leanhttp, en stuurt de
//!   store-operatie als bericht ([`Ask`]) naar de brievenbus van de store;
//!   de uitkomst komt terug op de antwoordplek van die werker
//!   ([`ANSWERS`], `Ctx::worker`);
//! - **de store-taak** is de enige eigenaar van de store en van de
//!   system-client: één operatie tegelijk, dus elke compare-and-swap is
//!   lineariseerbaar zonder slot.
//!
//! Markers op de console van de kern: `HOPOS_HOPLOCK_UP keys=<n>` als de
//! listener staat (n: de sleutels die het volume al had),
//! `HOPOS_HOPLOCK_NO_AUTH` zonder sleutel, `HOPOS_HOPLOCK_VOLUME` als het
//! volume niet te lezen was, `HOPOS_HOPLOCK_CORRUPT` bij een verminkt
//! record, `HOPOS_HOPLOCK_FAIL` bij een start die niet lukt, en om de
//! [`LOG_EVERY`] verzoeken `HOPOS_HOPLOCK_REQUESTS`.

#![cfg_attr(target_os = "none", no_std, no_main)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::string::ToString;
use core::sync::atomic::{AtomicU64, Ordering::Relaxed};
use core::time::Duration;

use applib::appnet::{self, SystemClient, TcpListener};
use applib::rt::Exec;
use applib::sys;
use applib::tcp::TcpConn;
use applib::{App, EXEC, log};
use applibhttp::Ctx;
use hoplockserver::http::{self, Unanswered};
use hoplockserver::server::{Done, Op, run};
use hoplockserver::volume::{Volume, VolumeError, VolumeStore};
use hoplockserver::{DEFAULT_PORT, Server};
use leanhttp::Exchange;
use sync::mpsc::Mailbox;
use sync::{Oneshot, oneshot};

applib::main!(hoplock);

/// Op de host bestaat dit image niet: daar is dit een lege binary, zodat
/// clippy de bewoner kan lezen.
#[cfg(not(target_os = "none"))]
fn main() {}

/// De map van de store zonder `HOPLOCK_DATA`.
const DEFAULT_DATA: &str = "/data";

/// De werkers: zoveel verzoeken tegelijk. Hop's client opent per
/// verzoek een verse verbinding en een verzoek duurt een paar
/// system-calls; vier is ruim voor een cluster van tientallen nodes, en de
/// rest wacht kort op de eerste die vrijkomt.
const WORKERS: usize = 4;

/// Zoveel stille keep-alive-verbindingen houdt het park; daarboven gaat een
/// verbinding na haar antwoord dicht. Hop's client houdt er hoogstens een
/// per node open.
const PARKED: usize = 32;

/// Om de zoveel verzoeken één logregel. Een lease wordt elke paar seconden
/// vernieuwd; per verzoek loggen zou de console vullen.
const LOG_EVERY: u64 = 1000;

/// Verzoeken sinds de start (een teller, dus een atomic).
static REQUESTS: AtomicU64 = AtomicU64::new(0);

/// De kap op elke leestermijn van een verzoek (kop en body): Go's
/// `ReadHeaderTimeout`. Tot 06-11-2026 ook de stilte tussen twee
/// verzoeken; die is nu van het park ([`IDLE`]).
const READ_CAP: Duration = Duration::from_secs(5);

/// Hoe lang een keep-alive-verbinding stil in het park mag staan: dezelfde
/// 5 s als toen de werker haar zelf vasthield ([`READ_CAP`]).
const IDLE: Duration = Duration::from_secs(5);

/// De server: acceptor, park en werkers (`applibhttp`).
static HTTP: applibhttp::Http<WORKERS, PARKED> = applibhttp::Http::new()
    .with_read_cap(READ_CAP)
    .with_idle(IDLE);

/// De brievenbus van de store-taak. Elke werker heeft hoogstens één vraag
/// uitstaan, dus [`WORKERS`] plaatsen is nooit vol.
static ASKS: Mailbox<Ask, WORKERS> = Mailbox::new();

/// Per werker (`Ctx::worker` van `applibhttp`) de plek voor het antwoord
/// van de store-taak. Een werker drijft zijn verzoek af tot het antwoord er
/// is (leanhttp breekt een handler niet af, en de vraag heeft geen
/// termijn), dus op een plek ligt nooit een antwoord van een eerder verzoek.
static ANSWERS: [Oneshot<Done>; WORKERS] = [const { Oneshot::new() }; WORKERS];

/// Een vraag aan de store-taak: de operatie en welke werker wacht.
struct Ask {
    worker: usize,
    op: Op,
}

/// De system-client als volume: de bestandscalls van de kern.
struct SysVolume(SystemClient);

/// Een fout van de system-client als tekst.
fn vol_err(e: &sys::Error) -> VolumeError {
    VolumeError(e.to_string())
}

impl Volume for SysVolume {
    const CHUNK: usize = sys::MAX_CHUNK;

    async fn size(&mut self, path: &str) -> Result<Option<u64>, VolumeError> {
        match self.0.stat(path).await {
            Ok(n) => Ok(Some(n)),
            Err(sys::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(vol_err(&e)),
        }
    }

    async fn read_at(
        &mut self,
        path: &str,
        off: u64,
        dst: &mut [u8],
    ) -> Result<usize, VolumeError> {
        self.0
            .read_into(path, off, dst)
            .await
            .map_err(|e| vol_err(&e))
    }

    async fn replace(&mut self, path: &str, data: &[u8]) -> Result<(), VolumeError> {
        self.0.write_file(path, data).await.map_err(|e| vol_err(&e))
    }

    async fn remove(&mut self, path: &str) -> Result<bool, VolumeError> {
        match self.0.remove(path).await {
            Ok(()) => Ok(true),
            Err(sys::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(vol_err(&e)),
        }
    }

    async fn list(&mut self, path: &str, dst: &mut [u8]) -> Result<Option<usize>, VolumeError> {
        match self.0.list(path, dst).await {
            Ok(n) => Ok(Some(n)),
            Err(sys::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(vol_err(&e)),
        }
    }
}

async fn hoplock(app: &'static App) {
    let exec: &'static Exec = EXEC.get();
    let port = port_of(app.env("ER_PORT_HTTP"));
    let data = app
        .env("HOPLOCK_DATA")
        .filter(|d| !d.is_empty())
        .unwrap_or(DEFAULT_DATA);
    let server = Server::new(app.env("HOPLOCK_API_KEY").unwrap_or(""));
    let net = match appnet::up(app) {
        Ok(n) => n,
        Err(e) => return fail(app, "network stack", &e).await,
    };
    let listener = match TcpListener::bind(port) {
        Ok(l) => l,
        Err(e) => return fail(app, "listen on ER_PORT_HTTP", &e).await,
    };
    let mut store = VolumeStore::new(SysVolume(net.system_client()), data);
    let keys = match store.count_keys().await {
        Ok(n) => n,
        Err(e) => {
            log!(
                "hoplockserver: cannot list {data}: {e}, starting as if empty HOPOS_HOPLOCK_VOLUME"
            );
            0
        }
    };
    if !server.has_auth() {
        log!("hoplockserver: no HOPLOCK_API_KEY, every request is allowed HOPOS_HOPLOCK_NO_AUTH");
    }
    let auth = if server.has_auth() { "on" } else { "off" };
    let key_len = server.key_len();
    let server: &'static Server = Box::leak(Box::new(server));
    if let Err(e) = exec.spawn(store_task(store)) {
        return fail(app, "spawn the store task", &e).await;
    }
    let [a, b, c, d] = net.ip();
    log!(
        "hoplockserver: serving on {a}.{b}.{c}.{d}:{port} (data={data}, auth={auth} ({key_len} chars), {WORKERS} workers) HOPOS_HOPLOCK_UP keys={keys}"
    );
    // Een verbinding die eindigt met een termijn of een reset is een client
    // die wegging; dat is geen logregel waard (applibhttp logt niet).
    let handler: &'static _ = Box::leak(Box::new(
        async |ex: &mut Exchange<'_, TcpConn>, ctx: &Ctx| {
            count_request();
            http::exchange(ex, server, |op| ask(ctx.worker, op)).await
        },
    ));
    // Keert alleen terug als er geen werker te spawnen was.
    HTTP.run_with(exec, listener, handler).await;
    fail(app, "spawn a worker", &"the task table is full").await;
}

/// Een start die niet lukt: één regel, en de app stopt met code 1.
async fn fail(app: &'static App, what: &str, e: &dyn core::fmt::Display) {
    log!("hoplockserver: {what}: {e} HOPOS_HOPLOCK_FAIL");
    app.shutdown(1).await;
}

/// De poort uit `ER_PORT_HTTP`, of [`DEFAULT_PORT`] zonder of bij onzin
/// (luid: een jobspec die iets anders bedoelde, moet dat kunnen zien).
fn port_of(env: Option<&str>) -> u16 {
    match env.map(str::parse::<u16>) {
        None => DEFAULT_PORT,
        Some(Ok(p)) if p != 0 => p,
        Some(_) => {
            log!(
                "hoplockserver: ER_PORT_HTTP={env:?} is not a port, using {DEFAULT_PORT} HOPOS_HOPLOCK_PORT"
            );
            DEFAULT_PORT
        }
    }
}

/// De store-taak: de enige eigenaar van de store en de system-client.
async fn store_task(mut store: VolumeStore<SysVolume>) {
    let mut corrupt_logged = 0u64;
    loop {
        let ask = ASKS.recv().await;
        let done = run(&mut store, ask.op).await;
        if let Some(key) = store.take_corrupt() {
            // Luid de eerste paar keer, daarna tellen we (handboek §6).
            corrupt_logged += 1;
            if corrupt_logged <= 3 {
                log!(
                    "hoplockserver: record {key} on {} is torn or corrupt, treated as absent ({} so far) HOPOS_HOPLOCK_CORRUPT",
                    store.root(),
                    store.corrupt()
                );
            }
        }
        if let Some(reply) = ANSWERS.get(ask.worker) {
            reply.put(done);
        }
    }
}

/// Stuurt `op` naar de store-taak en wacht op de uitkomst voor werker `worker`.
async fn ask(worker: usize, op: Op) -> Result<Done, Unanswered> {
    let reply = ANSWERS.get(worker).ok_or(Unanswered::Busy)?;
    oneshot::call(&ASKS, reply, Ask { worker, op })
        .await
        .map_err(|_| Unanswered::Busy)
}

/// Telt een verzoek, en logt er één regel over om de [`LOG_EVERY`].
fn count_request() {
    let n = REQUESTS.fetch_add(1, Relaxed).wrapping_add(1);
    if n.is_multiple_of(LOG_EVERY) {
        let st = applib::heap::HEAP.stats();
        log!(
            "hoplockserver: requests={n} heap_used={} heap_peak={} HOPOS_HOPLOCK_REQUESTS",
            st.used,
            st.peak
        );
    }
}
