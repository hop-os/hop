//! Hop als bewoner van HopOS: de binary die de kern in het slot met de bevoegdheid start.
//!
//! Bij start: de netstack (`appnet::up`), de config uit de env van het slot
//! (`HOPOS_*`, zie `agentd_hopos::env`), de system-client naar de kern, en
//! de [`Node`]: agent, leader (standalone, of via de verkiezing in een
//! cluster: `HOPOS_LOCK_*`, zie `agentd_hopos::lock`) en de
//! HopOS-runner. Daarna drie soorten taken op de executor van de app-core:
//!
//! - de eigenaar: bezit de [`Node`], handelt de verzoeken uit de [`Hub`]
//!   af en tikt elke seconde. Wat de node bij de kern vraagt (een slot, een
//!   brok image, een status, de staat bij de kern) is een `.await`: de
//!   eigenaar geeft dan de core terug, en de netstack en de
//!   verbindingstaken draaien door. Er wordt nergens een executor-ronde
//!   binnen een taak gedraaid (handboek §4);
//! - de downloadtaak (`agentd_hopos::download`): haalt de artifacts op en
//!   geeft de bytes als berichten aan de eigenaar, die ze brok voor brok de
//!   kern in stroomt en tussendoor de bus afhandelt. Een download van
//!   tientallen MB houdt de API zo niet meer vast (03-10);
//! - per poort één acceptor en een vaste pool van [`WORKERS`] werkers
//!   (agent op P, leader op P + 1000): de acceptor geeft elke verbinding als
//!   waarde aan een vrije werker ([`Handoff`]); de werker bedient er één
//!   verzoek op met leanhttp en stuurt het als bericht naar de eigenaar.
//!   Mag de verbinding blijven (keep-alive), dan gaat ze terug naar het
//!   park van de acceptor ([`Park`]), die haar bij het volgende verzoek
//!   weer aan een vrije werker geeft: een stille verbinding houdt zo geen
//!   werker vast. Een open stroom (`/v1/events`, een log-tail) houdt zijn
//!   werker vast en vraagt de eigenaar elke halve seconde wat er bij kwam.
//!
//! - de kloktaak: elk uur een SNTP-meting (`pool.ntp.org`) als bericht naar
//!   de eigenaar, die de klok van de kern zet met `SET_CLOCK` over zijn
//!   eigen verbinding. De eerste meting is een stap van de start, vóór alles
//!   wat naar buiten gaat (een keten heeft een datum nodig, een lease een
//!   tijd).
//!
//! Artifacts komen over `http://` en `https://` (`agentd_hopos::fetch`):
//! TLS met de Mozilla-wortels, hostnamen via de resolver.
//!
//! De start, in deze volgorde: de init-jobs en de staat van de vorige Hop
//! uit de kern (`read_state`, alleen gelezen), dan de klok (één meting en
//! `SET_CLOCK`, als stap; na een herstart zette de vorige Hop hem al), dan
//! de taken die naar buiten gaan (store, download, cluster), dan de overname
//! (`Node::restore`: de kooien die nog draaien worden van deze Hop), dan de
//! veger (wat daarna nog draait is van niemand), dan de cluster (de
//! boot-claim; de registratie bij een leader meldt de overgenomen taken al),
//! en alleen bij een schone start de init-jobs uit `HOPOS_INIT_JOBS` of
//! `/hop/init-jobs.json` (standalone; geclusterd zaait de leider op een
//! schone clusterstaat). Neemt de kern een herstart aan (`POST /restart`,
//! `agentd_hopos::restart`), dan wacht de eigenaar tot de werker die het
//! verzoek droeg zijn 202 geschreven en de verbinding gesloten heeft
//! (hoogstens `REPLY_GRACE`), legt de staat een laatste keer bij de kern
//! neer en keert terug; de main-schil van applib doet het net-afscheid en
//! de exit, en de apps draaien door. Luidt de kern eerst de stop-bel
//! (`app.stopped()`), dan dezelfde laatste staat en terug.
//!
//! Markers op het log: `HOP_UP`, `HOP_LEADER`, `HOP_JOB_PLACED slot=N`,
//! `HOP_CLOCK_SYNCED`, `HOP_INIT_SEEDED`, `HOP_ADOPTED n=N`,
//! `HOP_STRAY_STOPPED slot=N`, `HOP_RESTART`, `HOP_RESTART_ACCEPTED`,
//! `HOP_RESTART_LEAVING why=replied|grace|bell`, `HOP_STOPPING`,
//! `HOP_FATAL step=<stap>` (een start die niet lukt, of daarna een kern die
//! niet antwoordt: Hop stopt met een exitcode per soort uit abi,
//! `EXIT_KERNEL`, `EXIT_CONFIG`, `EXIT_INTERNAL` of `EXIT_CLOCK`, en de
//! watchdog reset de node; geen herhaallus), en de weigeringen en
//! degradaties `HOP_RESTART_FAIL`, `HOP_KERNEL_STATE_FAIL`, `HOP_ADOPT_SKIPPED`, `HOPOS_API_INSECURE`,
//! `HOP_SNTP_FAIL`, `HOP_TLS_ENTROPY_WEAK`, `HOP_TLS_ENTROPY_HW`,
//! `HOP_INIT_FAIL`, `HOP_JOB_REFUSED` (een job die de leader-API weigerde,
//! zoals een vaste poort die zou rollen) en `HOP_DOWNLOAD_QUEUE_FULL`. In
//! een cluster ook `HOP_CLUSTER`, `HOP_CLUSTER_JOIN`,
//! `HOP_LEADER_LOADING`, `HOP_STATE_LOADED`, `HOP_LEADER_STOPPED`, en de
//! weigeringen `HOP_LEASE_STORE`,
//! `HOP_LINK_FAIL`, `HOP_RELAY_REFUSED`, `HOP_RELAY_FULL`,
//! `HOP_STATE_SAVE_FAIL`.
//!
//! Canoniek gelinkt (applib/link.ld via build.rs), zoals appspike.
//!
//! Op QEMU: `tools/qemu-test-hop.sh` in de HopOS-repo boot de kern met deze
//! ELF in slot 1 (env, token, wandklok, poorten 8080 en 9080 doorgezet),
//! stuurt van buiten een jobspec en eist appspike in slot 2 (29-09 groen).
//! Wat daar nog niet is: health probes. De cluster over het LAN toetst
//! `tools/qemu-test-cluster.sh` in deze repo. Zonder tijdserver (geen DNS,
//! geen internet, en geen `hopos.ntp` op het LAN) start Hop niet: de eerste
//! SNTP-meting moet lukken (`HOP_FATAL step=clock`).

#![cfg_attr(target_os = "none", no_std, no_main)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::{Future, poll_fn};
use core::pin::{Pin, pin};
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use core::task::Poll;
use core::time::Duration;

use abi::hopabi::{EXIT_CLOCK, EXIT_CONFIG, EXIT_INTERNAL, EXIT_KERNEL};
use agentd_hopos::client::{Client, Idle};
use agentd_hopos::download::{self, Orders, Piece, PieceRx, Pieces};
use agentd_hopos::env::INIT_JOBS_FILE;
use agentd_hopos::forward::{self, Routed, STREAM_IDLE};
use agentd_hopos::lock::{self, ClusterConfig};
use agentd_hopos::mail::{DispatchQueue, Inbox, LeaseQueue, LinkQueue, Mail, Nap, StateQueue};
use agentd_hopos::restart;
use agentd_hopos::sntp::{self, NtpLink, PACKET, Sample};
use agentd_hopos::{
    Answer, BootConfig, ClusterParts, Connect, Fatal, Handoff, HttpImages, Hub, Images, Node, Park,
    Port, Question, Resolve,
};
use applib::appnet::{self, Endpoint, Net, NetError, SystemClient, TcpListener};
use applib::rand::{JITTER_ROUNDS, Rng};
use applib::rt::Exec;
use applib::{App, EXEC, log};
use discovery::Discovery;
use hop_http::{Ask, Chunk, Streams, TcpConn, Turn};
use hopos_runner::KernSys;
use leanhttp::{Kept, Outcome};
use sync::mpsc::Mailbox;
use types::Job;

applib::main!(resident);

/// Op de host bestaat deze bewoner niet: daar is dit een lege binary, zodat
/// de host-poort (clippy `--all-targets`) hem typecheckt zonder de
/// allocator en de paniekhaak van het slot.
#[cfg(not(target_os = "none"))]
fn main() {}

/// Het ritme van de eigenaar-taak: de tik van agent en leader.
const TICK: Duration = Duration::from_secs(1);

/// De kap op elke leestermijn van een verbinding (kop en body van een
/// verzoek): een pool van [`WORKERS`] per poort, dus een trage client mag
/// een werker niet lang ophouden. Zijn alle werkers bezet, dan sluit de
/// werker zijn verbinding na het antwoord (`Handoff::none_free`, zie
/// `forward::serve`).
const READ_CAP: Duration = Duration::from_secs(2);

/// Hoe lang een keep-alive-verbinding stil in het park mag staan voor de
/// acceptor haar sluit. Tot 06-11-2026 hield de werker haar zelf vast, met
/// [`READ_CAP`] als stilte; het park kost geen werker, maar elke verbinding
/// houdt wel een plek en budget in de netstack van het slot. Dus dezelfde
/// 2 s als voorheen: wat een client ziet, blijft gelijk.
const PARK_IDLE: Duration = READ_CAP;

/// Zoveel stille verbindingen houdt het park van één poort; een verbinding
/// die er niet meer bij past, gaat dicht (de client opent een nieuwe).
/// Ruim boven wat 2 s stilte bij de clients van één node oplevert, en klein
/// tegen het budget van de netstack (een verbinding kost minstens 20 KiB).
const PARK_CAP: usize = 32;

/// Werkers per poort. Een open stroom houdt er een vast; met hoogstens drie
/// stromen per node (`MAX_STREAMS` in de node) houdt elke poort er minstens
/// één vrij voor de CLI en de GUI.
const WORKERS: usize = 4;

/// Hoe lang een verbinding naar een artifact-server mag duren.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// Hoe lang één SNTP-vraag op antwoord wacht (Go: 3 s).
const NTP_TIMEOUT: Duration = Duration::from_secs(3);

/// Het ritme van SNTP na een gelukte synchronisatie (Go: elk uur).
const NTP_EVERY: Duration = Duration::from_secs(3600);

/// Na een mislukte uurmeting eerder opnieuw: de klok staat en loopt door,
/// maar een server die even weg was, hoeft geen uur te wachten.
const NTP_RETRY: Duration = Duration::from_secs(300);

/// Zoveel mislukkingen op rij krijgen een eigen regel; daarna één per
/// twaalf (eens per uur bij [`NTP_RETRY`]).
const NTP_LOUD: u32 = 3;

/// Het grootste init-jobs-bestand dat de bewoner leest.
const INIT_FILE_MAX: usize = 64 << 10;

/// De meting van de kloktaak naar de eigenaar, die er de klok van de kern
/// mee zet.
static CLOCK: Mailbox<Sample, 1> = Mailbox::new();

/// De rijen van de cluster (zie `agentd_hopos::mail`): statisch, want de
/// taken leven zo lang als de bewoner. Standalone blijven ze leeg.
static LEASE_Q: LeaseQueue = Mailbox::new();
/// De rij naar de link-taak.
static LINK_Q: LinkQueue = Mailbox::new();
/// De rij naar de staat-taak.
static STATE_Q: StateQueue = Mailbox::new();
/// De rij naar de dispatch-taak.
static DISPATCH_Q: DispatchQueue = Mailbox::new();
/// De brievenbus van de eigenaar.
static INBOX: Inbox = Mailbox::new();

/// Nieuws op de console, van de store-taak naar de eigenaar: de lange wacht
/// (`NEXT_STORE` met `NEXT_STORE_LOGS`) gaf `LOG_NEWS`, dus de eigenaar haalt
/// de console leeg (`Node::pump_logs`, besluit H8). Vol is al gemeld: de
/// eigenaar leest toch tot leeg.
static LOG_NEWS: Mailbox<(), 1> = Mailbox::new();

/// Stopt Hop hard: één regel met de reden en `HOP_FATAL step=<stap>`, het
/// net-afscheid en de exit met `code` (een `EXIT_*` per soort).
///
/// De enige weg uit een start die niet lukt (besluit S1, 01-11): een falende
/// Hop is een falende node. De kern laat hem liggen (`HOPOS_HOP_EXIT`) en de
/// watchdog reset de node. Geen `return` (exit 0), geen `pending()` (een
/// node die slaat maar niets doet), geen regel-en-door.
async fn fail(app: &App, code: u64, step: &str, why: impl core::fmt::Display) -> ! {
    log!("hop: {why}; stopping (exit {code}) HOP_FATAL step={step}");
    app.shutdown(code).await;
    // `shutdown` keert nooit terug (`App::exit` parkeert de core); dat staat
    // alleen niet in zijn type.
    match core::future::pending::<core::convert::Infallible>().await {}
}

/// Een [`Fatal`] van de node bij stap `step`: eerst zijn regels op het log,
/// dan [`fail`].
async fn fatal<S, I>(app: &App, node: &mut Node<S, I>, step: &str, f: Fatal) -> !
where
    S: runner::SystemApi + agent::Store,
    I: Images,
{
    flush(node);
    fail(app, f.code, step, f.why).await
}

/// De rij met opdrachten naar de downloadtaak.
static ORDERS: Orders = Mailbox::new();
/// De brokken van de downloadtaak naar de eigenaar.
static PIECES: Pieces = sync::spsc::Channel::new();
/// De download die de eigenaar nog wil (`Node::wanted`); één schrijver, de
/// eigenaar in [`flush`].
static WANTED: AtomicU64 = AtomicU64::new(0);

/// De kern nam een herstart aan: elke werker sluit zijn verbinding na het
/// lopende antwoord (`Connection: close`), zodat de werker van `POST
/// /restart` vrij is zodra zijn 202 geschreven is. Eén schrijver, de
/// eigenaar; een vlag, geen protocol.
static LEAVING: AtomicBool = AtomicBool::new(false);

/// Hoe lang een aanroep van de cluster (lease, staat, link, dispatch) na
/// zijn kop mag zwijgen: een lease of snapshot is klein.
const CLUSTER_IDLE: Duration = Duration::from_secs(20);

/// Nu in milliseconden op de wandklok van de kern: de lease vergelijkt
/// tijden van verschillende nodes. De start eist de wandklok (`fail`).
fn wall_ms() -> u64 {
    applib::app().and_then(App::wall_ns).unwrap_or_default() / 1_000_000
}

/// Nu in Unix-seconden: de klok van de S3-handtekening.
fn wall_secs() -> u64 {
    wall_ms() / 1000
}

/// Nu in Unix-seconden: de datum van een certificaatketen (leanhttps). De
/// start zette de klok vóór de eerste verbinding naar buiten, dus `None` is
/// alleen een kern zonder wandklok, en die stopt de start al.
fn chain_secs() -> Option<u64> {
    applib::app()
        .and_then(App::wall_ns)
        .map(|ns| ns / 1_000_000_000)
}

/// TCP-verbindingen van de cluster: een aanroep die na zijn kop zwijgt,
/// wordt na `idle` gesloten (`agentd_hopos::client::Idle`).
#[derive(Copy, Clone)]
struct ClusterConnect {
    net: &'static Net,
    exec: &'static Exec,
    idle: Duration,
}

impl Connect for ClusterConnect {
    type Conn = Idle<TcpConn>;

    async fn connect(&mut self, ip: [u8; 4], port: u16) -> Result<Self::Conn, String> {
        let s = self
            .net
            .tcp_connect_timeout(ip, port, DIAL_TIMEOUT)
            .await
            .map_err(|e| format!("{e}"))?;
        Ok(Idle::new(TcpConn::new(s, self.exec), self.idle))
    }
}

/// Slapen op het timerwiel van de app-core, voor de taken van de cluster.
#[derive(Copy, Clone)]
struct ExecNap(&'static Exec);

impl Nap for ExecNap {
    async fn nap(&self, d: Duration) {
        self.0.after(d).await;
    }
}

/// TCP-verbindingen naar artifact-servers over de netstack van het slot.
#[derive(Copy, Clone)]
struct SlotConnect {
    net: &'static Net,
    exec: &'static Exec,
}

impl Connect for SlotConnect {
    type Conn = TcpConn;

    async fn connect(&mut self, ip: [u8; 4], port: u16) -> Result<TcpConn, String> {
        let s = self
            .net
            .tcp_connect_timeout(ip, port, DIAL_TIMEOUT)
            .await
            .map_err(|e| format!("{e}"))?;
        Ok(TcpConn::new(s, self.exec))
    }
}

/// De resolver van de bewoner: `applib::appnet::resolve`, één A-vraag over
/// UDP naar `DNS` uit de env.
#[derive(Copy, Clone, Default)]
struct SlotResolver;

impl Resolve for SlotResolver {
    async fn resolve(&mut self, host: &str) -> Result<[u8; 4], String> {
        // Een naam die al een IP is gaat er meteen doorheen, de rest via de
        // DNS uit de env.
        applib::appnet::resolve(host)
            .await
            .map_err(|e| alloc::format!("{e}"))
    }
}

/// SNTP over een UDP-socket van het slot, naar `port` van de server.
struct UdpLink {
    net: &'static Net,
    port: u16,
}

impl NtpLink for UdpLink {
    async fn exchange(
        &mut self,
        server: [u8; 4],
        req: &[u8; PACKET],
        resp: &mut [u8],
    ) -> Result<usize, String> {
        let mut sock = self.net.udp_bind(0).map_err(|e| format!("bind: {e}"))?;
        sock.set_timeout(Some(NTP_TIMEOUT));
        let to = Endpoint {
            ip: server,
            port: self.port,
        };
        sock.send_to(to, req)
            .await
            .map_err(|e| format!("send: {e}"))?;
        loop {
            match sock.recv_from(resp).await {
                Ok((n, from)) if from == to => return Ok(n),
                // Een datagram van een ander adres is niet het antwoord.
                Ok(_) => {}
                Err(NetError::Timeout) => {
                    return Err(format!("no answer within {} s", NTP_TIMEOUT.as_secs()));
                }
                Err(e) => return Err(format!("recv: {e}")),
            }
        }
    }
}

/// Eén SNTP-meting: de naam opzoeken en tot [`sntp::ATTEMPTS`] keer vragen,
/// elk met [`NTP_TIMEOUT`].
async fn measure(
    net: &'static Net,
    exec: &'static Exec,
    server: &str,
    port: u16,
    seq: &mut u64,
) -> Result<Sample, sntp::SntpError> {
    let mut link = UdpLink { net, port };
    let nonce = || {
        *seq = seq.wrapping_add(1);
        exec.now() ^ seq.rotate_left(48)
    };
    sntp::sync(&mut SlotResolver, &mut link, server, || exec.now(), nonce).await
}

/// De kloktaak: na `wait` (een uur: de start zette de klok al), en daarna elk
/// uur (na een mislukte meting na [`NTP_RETRY`]), één SNTP-meting als bericht naar de eigenaar ([`CLOCK`]).
/// De eigenaar zet de klok van de kern met `SET_CLOCK` over de
/// hoofdverbinding ([`set_clock`]).
///
/// Geen eigen system-verbinding (besluit O9, 01-11): Hop houdt er twee vast,
/// de eigenaar en de store-taak, en de derde plaats van `MAX_HOP_CONNS` is
/// voor de ene herverbinding van applib, zoals bij een app. Het net (DNS en
/// UDP) wacht hier, niet in de eigenaar.
///
/// Een uurmeting die mislukt, is niet fataal (anders dan de eerste, bij de
/// start): de klok van de kern staat dan al en loopt door, een uur drift is
/// voor een certificaat en een lease van 30 s niets, en een NTP-server die
/// even weg is, is het net en geen kapotte node. Luid, drie keer, dan één op
/// twaalf, en na [`NTP_RETRY`] opnieuw.
async fn clock_task(
    net: &'static Net,
    exec: &'static Exec,
    server: String,
    port: u16,
    mut wait: Duration,
) {
    let mut fails: u32 = 0;
    let mut seq: u64 = 0;
    loop {
        exec.after(wait).await;
        wait = match measure(net, exec, &server, port, &mut seq).await {
            Ok(sample) => {
                fails = 0;
                // Vol kan niet: de eigenaar leest elke ronde, en de volgende
                // meting komt pas over een uur.
                let _ = CLOCK.try_send(sample);
                NTP_EVERY
            }
            Err(e) => {
                fails = fails.saturating_add(1);
                if fails <= NTP_LOUD || fails.is_multiple_of(12) {
                    log!(
                        "hop: SNTP {server}:{port} failed ({fails}x): {e}; the clock stays as it is HOP_SNTP_FAIL"
                    );
                }
                NTP_RETRY
            }
        };
    }
}

/// Wat een `SET_CLOCK` deed ([`sntp::apply`]): bij de start als stap in de
/// reeks, daarna voor de metingen van de kloktaak. Een kern die `SET_CLOCK`
/// weigert of er niet op antwoordt, is een kapotte node: [`fail`] (besluit
/// H1, ook na de start).
async fn clock_set(app: &App, set: Result<u64, runner::SysError>, sample: &Sample, server: &str) {
    match set {
        Ok(unix_ns) => log!(
            "hop: clock set from {server} (stratum {}, delay {} us) to {} s HOP_CLOCK_SYNCED",
            sample.stratum,
            sample.delay_ns / 1000,
            unix_ns / 1_000_000_000
        ),
        Err(e) => fail(app, EXIT_KERNEL, "clock", format_args!("SET_CLOCK: {e}")).await,
    }
}

/// De object-store van de apps (`agentd_hopos::objstore`): haalt de
/// store-calls van de apps op bij de kern (`NEXT_STORE`, een lange wacht),
/// doet de S3-kant en meldt af. Dezelfde wacht komt terug met nieuws op de
/// console (`NEXT_STORE_LOGS`, besluit H8): dat gaat als [`LOG_NEWS`] naar de
/// eigenaar, die de console leeg haalt over de hoofdverbinding. Zo pollt
/// niemand `NEXT_LOG` per tik, en kost de console geen derde verbinding.
/// Een eigen system-verbinding, blijvend: de kern geeft het slot van Hop er
/// één meer dan een app (`MAX_HOP_CONNS`). Zonder `HOPOS_S3_*` weigert hij elke call luid, zodat een app hoort
/// waarom zijn pull faalt.
async fn store_task(
    app: &'static App,
    net: &'static Net,
    exec: &'static Exec,
    s3: Option<agentd_hopos::env::S3Config>,
    cluster: String,
) {
    use agentd_hopos::objstore::{Served, Service, bucket};
    let bucket = s3.map(|c| {
        let cfg = bucket::Config {
            endpoint: c.endpoint,
            bucket: c.bucket,
            region: c.region,
            key: c.key,
            secret: c.secret,
            path_style: c.path_style,
        };
        // Een eigen DRBG: de handshakes van de store delen geen staat met
        // die van de downloader (de regel over de bron gaf entropy()).
        let b = bucket::S3Bucket::new(
            &cfg,
            SlotConnect { net, exec },
            SlotResolver,
            quiet_rng(app, 6),
            wall_secs,
            chain_secs,
        );
        if b.is_plain_http() {
            log!(
                "hop: object store {} is plain http: requests are signed but readable on the way; use https outside a test HOP_STORE_PLAIN_HTTP",
                cfg.endpoint
            );
        }
        log!(
            "hop: object store for apps: {} bucket {} under apps/{cluster}/ HOP_STORE_UP",
            cfg.endpoint,
            cfg.bucket
        );
        b
    });
    if bucket.is_none() {
        log!(
            "hop: no object store configured (hopos.s3.*); app store calls are refused HOP_STORE_NONE"
        );
    }
    let mut svc = Service::new(bucket, &cluster);
    let mut sys = KernSys::new(net.system_client(), 1);
    loop {
        match svc.serve_one(&mut sys).await {
            // Geen regel hier: elke regel op de console wekt deze wacht.
            Ok(Served::Logs) => {
                let _ = LOG_NEWS.try_send(());
            }
            // Een pull van iets dat er niet is, is een vraag, geen fout.
            Ok(Served::Done(o))
                if !matches!(
                    o.status,
                    runner::StoreStatus::Ok | runner::StoreStatus::NotFound
                ) =>
            {
                log!(
                    "hop: store {} {} for slot {}: {} HOP_STORE_FAIL",
                    o.task.op.name(),
                    o.task.key,
                    o.task.slot.0,
                    o.why
                )
            }
            Ok(Served::Done(o)) if !o.delivered => log!(
                "hop: store {} {} for slot {}: the task was gone, result dropped HOP_STORE_GONE",
                o.task.op.name(),
                o.task.key,
                o.task.slot.0
            ),
            Ok(_) => {}
            // Een kern die op de lange wacht niet antwoordt, is een kapotte
            // node (besluit H1), geen lus die het over een seconde opnieuw
            // vraagt.
            Err(e) => fail(app, EXIT_KERNEL, "store", format_args!("NEXT_STORE: {e}")).await,
        }
    }
}

/// De willekeur voor TLS: de DRBG van applib (`applib::rand::Rng`), gezaaid
/// uit het zaad dat de kern op de control-page legt en de jitter van de
/// teller, en bij elke trekking herzaaid als de kern een nieuw zaad legde.
/// Eén regel over de bron: `HOP_TLS_ENTROPY_HW` als dat zaad uit een
/// hardware-RNG komt, anders `HOP_TLS_ENTROPY_WEAK` met de reden.
fn entropy(app: &App) -> Rng {
    let rng = Rng::open(app);
    match rng.origin() {
        o if o.is_hardware() => log!(
            "hop: TLS randomness from the kernel seed ({o}, hardware) mixed with timer jitter ({JITTER_ROUNDS} samples) HOP_TLS_ENTROPY_HW source={o}"
        ),
        o => log!(
            "hop: TLS randomness from the kernel seed and timer jitter ({JITTER_ROUNDS} samples), but the kernel seeds itself from {o}: this node has no hardware RNG HOP_TLS_ENTROPY_WEAK"
        ),
    }
    rng
}

/// Een eigen DRBG voor een client van de cluster, de flip of de store
/// (`tag` mengt de eigenaar erin); dezelfde bron, zonder de regel opnieuw.
fn quiet_rng(app: &App, tag: u8) -> Rng {
    let mut rng = Rng::open(app);
    rng.stir(&[tag]);
    rng
}

/// Een HTTP-client van de cluster over de netstack van het slot.
fn cluster_client(
    app: &App,
    net: &'static Net,
    exec: &'static Exec,
    tag: u8,
    idle: Duration,
) -> Client<ClusterConnect, SlotResolver> {
    Client::new(
        ClusterConnect { net, exec, idle },
        SlotResolver,
        quiet_rng(app, tag),
        chain_secs,
    )
}

/// Start de cluster: de boot-claim, en de taken van lease, staat, link en
/// dispatch. Geeft de onderdelen voor [`Node::new_clustered`].
async fn start_cluster(
    app: &'static App,
    net: &'static Net,
    exec: &'static Exec,
    cfg: &BootConfig,
    cc: ClusterConfig,
    init_jobs: Option<Vec<Job>>,
) -> Result<ClusterParts, String> {
    let owner = format!("{}:{}", cfg.node_ip, cfg.leader_port());
    let disc = Discovery::new(owner, cc.ttl_ms);
    // De boot-claim doet de node zelf zodra de klok gezet is (een lease is
    // een tijd op de wandklok), via de lease-taak: `Node::cluster_boot`.
    let lease = lock::open_lease(
        &cc,
        cluster_client(app, net, exec, 1, CLUSTER_IDLE),
        wall_secs,
    );
    let state = lock::open_state(
        &cc,
        cluster_client(app, net, exec, 2, CLUSTER_IDLE),
        wall_secs,
    );
    log!(
        "hop: cluster state in {} HOP_STATE_STORE",
        lock::StateBackend::describe(&state)
    );
    let nap = ExecNap(exec);
    let key = cfg.api_key.clone();
    exec.spawn(agentd_hopos::lease::lease_task(
        disc, lease, &LEASE_Q, &INBOX, wall_ms, nap,
    ))
    .map_err(|e| format!("lease task: {e}"))?;
    exec.spawn(agentd_hopos::lease::state_task(
        state, &STATE_Q, &INBOX, nap,
    ))
    .map_err(|e| format!("state task: {e}"))?;
    exec.spawn(agentd_hopos::link::link_task(
        cluster_client(app, net, exec, 3, CLUSTER_IDLE),
        key.clone(),
        &LINK_Q,
        &INBOX,
        nap,
    ))
    .map_err(|e| format!("link task: {e}"))?;
    exec.spawn(agentd_hopos::relay::dispatch_task(
        cluster_client(app, net, exec, 4, CLUSTER_IDLE),
        key,
        &DISPATCH_Q,
        &INBOX,
        nap,
    ))
    .map_err(|e| format!("dispatch task: {e}"))?;
    Ok(ClusterParts {
        cfg: cc,
        lease: &LEASE_Q,
        link: &LINK_Q,
        state: &STATE_Q,
        dispatch: &DISPATCH_Q,
        wall_ms,
        init_jobs,
        node_dead: config::Config::default().timeouts.node_dead_threshold,
    })
}

/// Waarvoor de eigenaar wakker werd.
enum Woke {
    /// Een bericht van de cluster.
    Mail(Mail),
    /// Een stap van de lopende download.
    Piece(u64, Piece),
    /// Nieuws op de console ([`LOG_NEWS`]).
    Logs,
    /// De bus of de tik: de lus kijkt zelf.
    Look,
    /// De stop-bel van de kern.
    Stop,
}

/// Wacht op een verzoek in de bus, een bericht van de cluster, een brok van
/// de downloadtaak, nieuws op de console, de tik, of de stop-bel.
///
/// De bel gaat vóór alles: wat na hem nog binnenkomt, kent de volgende Hop
/// niet, dus de eigenaar handelt het niet meer af.
///
/// Een brok gaat vóór de bus: de lus handelt de bus bovenaan toch helemaal
/// af, dus zo krijgen ze om de beurt een ronde (één brok, dan alle
/// verzoeken), en een drukke API houdt een download niet tegen.
async fn wake(
    hub: &Hub,
    inbox: &Inbox,
    pieces: &mut PieceRx<'_>,
    tick: impl Future<Output = ()>,
    stop: impl Future<Output = ()>,
) -> Woke {
    let mut a = pin!(hub.wait());
    let mut b = pin!(inbox.recv());
    let mut d = pieces.recv();
    let mut l = pin!(LOG_NEWS.recv());
    let mut c = pin!(tick);
    let mut s = pin!(stop);
    poll_fn(|cx| {
        if s.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Woke::Stop);
        }
        if let Poll::Ready(m) = b.as_mut().poll(cx) {
            return Poll::Ready(Woke::Mail(m));
        }
        if let Poll::Ready((seq, p)) = core::pin::Pin::new(&mut d).poll(cx) {
            return Poll::Ready(Woke::Piece(seq, p));
        }
        if l.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Woke::Logs);
        }
        if a.as_mut().poll(cx).is_ready() || c.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Woke::Look);
        }
        Poll::Pending
    })
    .await
}

/// De init-jobs van deze boot: uit de env, of anders uit [`INIT_JOBS_FILE`]
/// in het volume, getoetst. `None` als er geen zijn.
///
/// Over de verbinding van de eigenaar-taak (`c`): een eigen verbinding zou
/// er een te veel zijn (`MAX_HOP_CONNS`). Geen bestand is geen init-jobs;
/// zonder schijf zegt de kern hetzelfde (`NotFound`, "no storage on this
/// node", hop-os 4dc42af). Init-jobs die er zijn maar niet kloppen (te groot, geen
/// UTF-8, een spec die de leader niet neemt) zijn een node zonder zijn
/// basis: [`fail`] (besluit H7, 01-11), net als een kern die op de stat of
/// de lees niet antwoordt.
async fn init_specs(app: &App, cfg: &BootConfig, c: &mut SystemClient) -> Option<Vec<Job>> {
    let specs = match &cfg.init_jobs {
        Some(j) => j.clone(),
        None => match read_init_file(c).await {
            Ok(Some(s)) => s,
            Ok(None) => return None,
            Err((code, why)) => {
                fail(app, code, "init", format_args!("{INIT_JOBS_FILE}: {why}")).await
            }
        },
    };
    match agentd_hopos::init_jobs(&specs) {
        Ok(jobs) => Some(jobs),
        Err(e) => fail(app, EXIT_CONFIG, "init", e).await,
    }
}

/// Leest [`INIT_JOBS_FILE`]; `None` zonder bestand, een fout met zijn
/// exitcode.
async fn read_init_file(c: &mut SystemClient) -> Result<Option<String>, (u64, String)> {
    use applib::sys::Error;
    let kern = |e: Error| (EXIT_KERNEL, format!("{e}"));
    let size = match c.stat(INIT_JOBS_FILE).await {
        Ok(s) => usize::try_from(s).unwrap_or(usize::MAX),
        Err(Error::NotFound { .. }) => return Ok(None),
        Err(e) => return Err(kern(e)),
    };
    if size > INIT_FILE_MAX {
        return Err((EXIT_CONFIG, format!("{size} bytes, limit {INIT_FILE_MAX}")));
    }
    let mut buf = alloc::vec![0u8; size];
    let mut at = 0;
    while at < size {
        let dst = buf.get_mut(at..).unwrap_or_default();
        match c
            .read_into(INIT_JOBS_FILE, at as u64, dst)
            .await
            .map_err(kern)?
        {
            0 => break,
            n => at += n,
        }
    }
    buf.truncate(at);
    String::from_utf8(buf)
        .map(Some)
        .map_err(|_| (EXIT_CONFIG, String::from("not UTF-8")))
}

/// Nu in Unix-nanoseconden: de wandklok van de kern (de start eist hem).
fn now(app: &App) -> u64 {
    app.wall_ns().unwrap_or_default()
}

/// Zet de regels van de node op het log, en een download die klaarligt in
/// de rij van de downloadtaak.
fn flush<S, I>(node: &mut Node<S, I>)
where
    S: runner::SystemApi + agent::Store,
    I: Images,
{
    for line in node.take_lines() {
        log!("{line}");
    }
    if let Some(order) = node.take_order() {
        let seq = order.seq;
        if let Err(sync::Full(back)) = ORDERS.try_send(order) {
            // Kan alleen met afgebroken opdrachten die de taak nog niet
            // oversloeg; de volgende flush probeert het opnieuw.
            node.order_back(&back);
            log!(
                "hop: download {seq} waits: the download queue is full ({}) HOP_DOWNLOAD_QUEUE_FULL",
                download::ORDERS
            );
        }
    }
    WANTED.store(node.wanted(), Relaxed);
}

/// De keep-alive-verbindingen die de werkers van één poort teruggeven aan
/// het park van hun acceptor. Elke werker geeft er hoogstens één per
/// verzoek; past er geen meer bij, dan sluit de werker haar.
type Back = Mailbox<Kept<TcpConn>, WORKERS>;

/// De wekker van het park: de future van `Exec::until`, met zijn tijdstip.
type Alarm = (u64, Pin<Box<dyn Future<Output = ()>>>);

/// De acceptor van één poort, met het park: nieuwe verbindingen en
/// verbindingen uit het park waar een volgend verzoek binnenkomt, gaan als
/// waarde naar een vrije werker; stille wachten in het [`Park`] tot
/// [`PARK_IDLE`].
///
/// De listener is al gebonden vóór de spawn: zodra `HOP_UP` op het log
/// staat, neemt de stack een SYN aan, ook als deze taak nog geen ronde had.
///
/// Eén taak voor de listener, de terugkeer van de werkers en het park, in
/// één `poll_fn` zoals `applibhttp` in HopOS: een stille verbinding kost
/// geen taak en geen timer; het park houdt één timer voor de vroegste
/// stilte, en alleen zolang er iets staat.
async fn accept(
    listener: TcpListener,
    exec: &'static Exec,
    pool: &'static Handoff<Turn<TcpConn>>,
    back: &'static Back,
    number: u16,
) {
    let mut park: Park<TcpConn> = Park::new(PARK_CAP, PARK_IDLE);
    let mut alarm: Option<Alarm> = None;
    let mut due: Vec<Turn<TcpConn>> = Vec::new();
    let mut ready: Vec<Kept<TcpConn>> = Vec::new();
    let mut gone: Vec<Kept<TcpConn>> = Vec::new();
    loop {
        let mut failed: Option<NetError> = None;
        poll_fn(|cx| {
            let mut moved = false;
            // Terug van de werkers: het park in; vol gaat dicht.
            if park.take_back(cx, back, exec.now(), &mut gone) {
                moved = true;
            }
            // Nieuwe verbindingen.
            loop {
                match pin!(listener.accept()).poll(cx) {
                    Poll::Ready(Ok(s)) => {
                        moved = true;
                        due.push(Turn::New(TcpConn::new(s, exec).with_read_cap(READ_CAP)));
                    }
                    Poll::Ready(Err(e)) => {
                        moved = true;
                        failed = Some(e);
                        break;
                    }
                    Poll::Pending => break,
                }
            }
            // Het park: leesbaar gaat naar een werker, te lang stil dicht.
            if park.sweep(cx, exec.now(), &mut ready, &mut gone) {
                moved = true;
            }
            due.extend(ready.drain(..).map(Turn::Again));
            // Eén timer: de vroegste stilte.
            match park.next_until() {
                None => alarm = None,
                Some(at) => {
                    if alarm.as_ref().is_none_or(|(t, _)| *t != at) {
                        alarm = Some((at, Box::pin(exec.until(at))));
                    }
                    if let Some((_, a)) = alarm.as_mut()
                        && a.as_mut().poll(cx).is_ready()
                    {
                        alarm = None;
                        moved = true;
                    }
                }
            }
            if moved {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        for kept in gone.drain(..) {
            kept.close().await;
        }
        // Alle werkers bezig: wachten op de eerste die vrijkomt (een bel).
        // Het park en de listener wachten zolang in de stack; wat werkers
        // intussen teruggeven, gaat meteen het park in.
        for job in due.drain(..) {
            park.place(pool, back, job, || exec.now(), &mut gone).await;
        }
        if let Some(e) = failed {
            log!("hop: accept on :{number}: {e}");
            exec.after(Duration::from_millis(100)).await;
        }
    }
}

/// De naden van een open stroom voor één werker: de bus naar de eigenaar
/// en het timerwiel van de app-core.
struct HubStreams {
    hub: &'static Hub,
    exec: &'static Exec,
    slot: usize,
}

impl Streams for HubStreams {
    async fn poll(&mut self, ask: Ask) -> Chunk {
        self.hub.poll(self.slot, ask).await
    }

    async fn nap(&mut self, d: Duration) {
        self.exec.after(d).await;
    }

    fn now(&self) -> u64 {
        self.exec.now()
    }

    fn done(&mut self) {
        self.hub.stream_done(self.slot);
    }
}

/// Eén werker: wacht op een verbinding, bedient één verzoek met leanhttp
/// (als bericht naar de eigenaar met vak `slot` in de bus), en geeft een
/// verbinding die mag blijven terug aan het park van de acceptor. Een
/// doorgifte naar een andere node (de leader, of een agent als deze node
/// leidt) voert de werker zelf uit met zijn eigen client
/// (`agentd_hopos::forward`).
#[expect(
    clippy::too_many_arguments,
    reason = "de vaste naden van een werker, één keer bij de spawn"
)]
async fn work(
    pool: &'static Handoff<Turn<TcpConn>>,
    back: &'static Back,
    i: usize,
    exec: &'static Exec,
    hub: &'static Hub,
    slot: usize,
    port: Port,
    mut client: Client<ClusterConnect, SlotResolver>,
) {
    let mut streams = HubStreams { hub, exec, slot };
    loop {
        let mut turn = pool.take(i).await;
        loop {
            let out = forward::serve(
                turn,
                async |req| hub.ask_routed(slot, port, req).await,
                &mut streams,
                &mut client,
                port,
                || pool.none_free() || LEAVING.load(Relaxed),
            )
            .await;
            match out {
                // Al gepijplijnd: wachten op leesbaar zou nooit eindigen.
                Ok(Outcome::Kept(k)) if k.has_buffered() => turn = Turn::Again(k),
                Ok(Outcome::Kept(k)) => {
                    if let Err(sync::Full(k)) = back.try_send(k) {
                        k.close().await;
                    }
                    break;
                }
                // Geen route neemt de verbinding over; komt er toch een
                // terug, dan gaat ze hier dicht.
                Ok(Outcome::Hijacked(h)) => {
                    let mut conn = h.conn;
                    let _ = leanhttp::close(&mut conn).await;
                    break;
                }
                // Een verbinding die eindigt met een termijn of een reset is
                // gewoon een client die wegging; dat is geen logregel waard.
                Ok(Outcome::Closed) | Err(_) => break,
            }
        }
        pool.free(i);
    }
}

/// Vertrekt na een aangenomen herstart: wacht tot de werker in vak `slot`
/// zijn antwoord geschreven en zijn verbinding gesloten heeft (hoogstens
/// `REPLY_GRACE`, of tot de stop-bel), en legt dan de staat een laatste
/// keer bij de kern neer. De aanroeper keert daarna terug uit main.
async fn leave<S, I>(
    node: &mut Node<S, I>,
    app: &'static App,
    exec: &'static Exec,
    agent_pool: Option<&'static Handoff<Turn<TcpConn>>>,
    slot: usize,
) where
    S: runner::SystemApi + agent::Store,
    I: Images,
{
    // De agent-poort heeft de vakken 0..WORKERS, één per werker.
    let replied = async move {
        match agent_pool {
            Some(pool) if slot < WORKERS => pool.freed(slot).await,
            _ => core::future::pending::<()>().await,
        }
    };
    let why = restart::depart(replied, exec.after(restart::REPLY_GRACE), app.stopped()).await;
    node.leave(now(app), why).await;
    flush(node);
}

async fn resident(app: &'static App) {
    let exec: &'static Exec = EXEC.get();
    log!("hop: resident starting in slot {} HOP_BOOT", app.slot());
    let net = match appnet::up(app) {
        Ok(n) => n,
        Err(e) => {
            fail(
                app,
                EXIT_INTERNAL,
                "net",
                format_args!("no network, no API: {e}"),
            )
            .await
        }
    };
    let [a, b, c, d] = net.ip();
    let slot_ip = format!("{a}.{b}.{c}.{d}");
    // Alle config eerst, vóór de eerste spawn: een config die niet klopt, is
    // een node die niet start (besluit H2, 01-11), niet een halve met een
    // klok en een store-dienst maar zonder API. Fail closed, zoals de
    // Go-kern: zonder sleutel geen API. Een lock die niet klopt: een node die
    // stil standalone draait naast zijn cluster, is een tweede leader.
    let env = |k: &str| app.env(k).map(String::from);
    let cfg = match BootConfig::from_env(env, app.slot(), &slot_ip) {
        Ok(c) => c,
        Err(e) => fail(app, EXIT_CONFIG, "config", e).await,
    };
    let cluster_cfg = match ClusterConfig::from_env(env, &cfg.cluster, cfg.s3.as_ref()) {
        Ok(c) => c,
        Err(e) => fail(app, EXIT_CONFIG, "config", e).await,
    };
    // Het adres zoals de andere nodes deze node zien (achter een NAT anders
    // dan het slot-adres); de listeners blijven op de poorten van `cfg`.
    let seen = match lock::advertised(&cfg, env) {
        Ok(c) => c,
        Err(e) => fail(app, EXIT_CONFIG, "config", e).await,
    };
    if cfg.insecure {
        log!("hop: WARNING API authentication is OFF (HOPOS_INSECURE=1) HOPOS_API_INSECURE");
    }
    if cfg.insecure_ignored {
        log!("hop: HOPOS_INSECURE=1 ignored: HOPOS_APIKEY is set, the API authenticates");
    }
    if app.wall_ns().is_none() {
        // De kern zet de wandklok vóór de eerste instructie van Hop
        // (HOPOS_CLOCK_FIXED tot SNTP); zonder is hij kapot.
        fail(app, EXIT_KERNEL, "clock", "no wall clock from the kernel").await;
    }

    let mut sys = KernSys::new(net.system_client(), cfg.cores);
    // De init-jobs nu al, over de eigen verbinding: geclusterd zaait de
    // leider ze, alleen gebruikt een schone staat ze (hieronder).
    let mut specs = init_specs(app, &cfg, sys.conn_mut()).await;
    // De staat van de vorige Hop uit de kern, eerst alleen gelezen: hij zegt
    // of die de klok al zette (besluit H5). Overnemen komt na de klok.
    let state = match agentd_hopos::read_state(&mut sys).await {
        Ok(s) => s,
        Err(f) => fail(app, f.code, "restore", f.why).await,
    };
    // De klok, als stap in de reeks (besluit O9, 01-11): één meting, en
    // SET_CLOCK over de hoofdverbinding. Vóór alles wat naar buiten gaat:
    // de store-taak, de downloads (een restore die een taak opnieuw plaatst,
    // is meteen een download), de cluster. `https` dateert een keten op de
    // wandklok, en een lease is een tijd op de wandklok; daarna staat de klok
    // dus altijd, en weigert niemand meer iets "tot de klok staat" (vraag 19).
    // De tijdserver: `HOPOS_NTP` (`host` of `host:poort`), anders
    // pool.ntp.org.
    //
    // Die meting moet lukken, standalone en geclusterd ("zonder tijd geen
    // PC", Derek 01-11): lukt hij niet, dan fail(clock) en reset de watchdog
    // de node; de koude boot is de enige herkansing. De termijn is die van
    // één meting: de DNS-vraag en hoogstens `sntp::ATTEMPTS` (3) vragen van
    // `NTP_TIMEOUT` (3 s), dus rond de tien seconden. Meer vragen is het niet:
    // UDP herhaalt zelf niets, en drie is wat een verloren datagram nodig
    // heeft (Go deed hetzelfde); langer wachten maakt een node zonder
    // tijdserver alleen trager kapot.
    //
    // Na een herstart zette de vorige Hop de klok al (de staat zegt het): die
    // vertrouwt deze Hop, en hij meet pas over een uur zelf (besluit H5,
    // 01-11). Een herstart hangt zo niet af van een server buiten het LAN.
    let (ntp_host, ntp_port) = sntp::server_from(app.env(sntp::ENV_NTP));
    if state.as_ref().is_some_and(|h| h.clock) {
        log!(
            "hop: the previous Hop set the kernel's clock; trusting it HOP_CLOCK_SYNCED from=state"
        );
    } else {
        match measure(net, exec, &ntp_host, ntp_port, &mut 0).await {
            Ok(sample) => {
                let set = sntp::apply(&mut sys, &sample, exec.now()).await;
                clock_set(app, set, &sample, &ntp_host).await;
            }
            Err(e) => {
                let why = format!("SNTP {ntp_host}:{ntp_port}: {e}; no time, no node");
                fail(app, EXIT_CLOCK, "clock", why).await
            }
        }
    }

    if let Err(e) = exec.spawn(store_task(
        app,
        net,
        exec,
        cfg.s3.clone(),
        cfg.cluster.clone(),
    )) {
        fail(
            app,
            EXIT_INTERNAL,
            "spawn",
            format_args!("the store task: {e}"),
        )
        .await;
    }
    let images = HttpImages::new(
        SlotConnect { net, exec },
        SlotResolver,
        chain_secs,
        wall_secs,
        entropy(app),
    );
    // De artifacts haalt de downloadtaak; de node houdt een eigen
    // downloader voor de kernbundel van een flip (zeldzaam, en die wacht op
    // de kern zelf, crate::flip).
    let Some((pieces_tx, mut pieces)) = PIECES.split() else {
        fail(
            app,
            EXIT_INTERNAL,
            "spawn",
            "the download queue was already taken",
        )
        .await
    };
    if let Err(e) = exec.spawn(download::download_task(images, &ORDERS, pieces_tx, &WANTED)) {
        fail(
            app,
            EXIT_INTERNAL,
            "spawn",
            format_args!("the download task: {e}"),
        )
        .await;
    }
    let images = HttpImages::new(
        SlotConnect { net, exec },
        SlotResolver,
        chain_secs,
        wall_secs,
        quiet_rng(app, 5),
    );
    let clustered = cluster_cfg.is_some();
    let mut node = match cluster_cfg {
        None => Node::new(&seen, sys, images, now(app)),
        Some(cc) => {
            // Geclusterd: de init-jobs zaait wie leider wordt op een schone
            // clusterstaat, niet de boot van deze node.
            match start_cluster(app, net, exec, &seen, cc, specs.take()).await {
                Ok(parts) => Node::new_clustered(&seen, sys, images, now(app), parts),
                Err(e) => {
                    fail(
                        app,
                        EXIT_INTERNAL,
                        "spawn",
                        format_args!("the cluster: {e}"),
                    )
                    .await
                }
            }
        }
    };
    // Dan de overname: wat nog draait, wordt van deze Hop, vóór de veger en
    // vóór de eerste registratie bij een leader. Een blob in kerngeheugen
    // sterft met de boot, dus geen spook van een vorige (7726782). Wat
    // daarna nog draait, is van niemand.
    let kept = match node.restore(state, now(app)).await {
        Ok(k) => k,
        Err(f) => fatal(app, &mut node, "restore", f).await,
    };
    if let Err(f) = node.sweep_strays().await {
        fatal(app, &mut node, "sweep", f).await;
    }
    flush(&mut node);
    let clock = clock_task(net, exec, ntp_host.clone(), ntp_port, NTP_EVERY);
    if let Err(e) = exec.spawn(clock) {
        fail(
            app,
            EXIT_INTERNAL,
            "spawn",
            format_args!("the clock task: {e}"),
        )
        .await;
    }
    flush(&mut node);
    if clustered {
        // De boot-claim: raak, dan leidt deze node nu (de staat laadt eerst).
        node.cluster_boot(now(app));
        flush(&mut node);
    }
    if !clustered
        && kept.is_clean()
        && let Some(jobs) = specs
    {
        if let Err(f) = node.seed_init_jobs(jobs, now(app)).await {
            fatal(app, &mut node, "init", f).await;
        }
        flush(&mut node);
    }

    // Eén bus voor het leven van de bewoner, met een vak per werker; de
    // taken krijgen `&'static`.
    let hub: &'static Hub = Box::leak(Box::new(Hub::new(2 * WORKERS)));
    // De listeners binden hier, vóór de spawn: een spawn krijgt zijn slot pas
    // in de volgende ronde van de executor, en zo hoeft niemand daarop te
    // wachten. Een poort die niet bindt, is een node zonder API: hard.
    let ports = [
        (0, Port::Agent, cfg.port),
        (WORKERS, Port::Leader, cfg.leader_port()),
    ];
    // De pool van de agent-poort: daar komt `POST /restart` binnen, en de
    // eigenaar wacht na een aangenomen herstart op de werker die hem droeg.
    let mut agent_pool: Option<&'static Handoff<Turn<TcpConn>>> = None;
    for (first_slot, port, number) in ports {
        let l = match net.tcp_listen(number) {
            Ok(l) => l,
            Err(e) => {
                fail(
                    app,
                    EXIT_INTERNAL,
                    "listen",
                    format_args!("port :{number}: {e}"),
                )
                .await
            }
        };
        let pool: &'static Handoff<Turn<TcpConn>> = Box::leak(Box::new(Handoff::new(WORKERS)));
        let back: &'static Back = Box::leak(Box::new(Mailbox::new()));
        if port == Port::Agent {
            agent_pool = Some(pool);
        }
        let mut spawned = exec.spawn(accept(l, exec, pool, back, number));
        for i in 0..WORKERS {
            if spawned.is_err() {
                break;
            }
            let tag = u8::try_from(16 + first_slot + i).unwrap_or(u8::MAX);
            let client = cluster_client(app, net, exec, tag, STREAM_IDLE);
            spawned = exec.spawn(work(pool, back, i, exec, hub, first_slot + i, port, client));
        }
        if let Err(e) = spawned {
            fail(
                app,
                EXIT_INTERNAL,
                "spawn",
                format_args!("the listeners: {e}"),
            )
            .await;
        }
    }
    log!(
        "hop: agent up node={} cluster={} agent=:{} leader=:{} cores={} system={} HOP_UP",
        cfg.node_id,
        cfg.cluster,
        cfg.port,
        cfg.leader_port(),
        cfg.cores,
        u8::from(cfg.system_core)
    );

    let tick_ns = u64::try_from(TICK.as_nanos()).unwrap_or(u64::MAX);
    let mut next_tick = now(app);
    loop {
        // Eerst de bus leeg (level-triggered): wat binnenkwam terwijl de
        // eigenaar op de kern wachtte, ligt er nog en wordt nu afgehandeld.
        while let Some((slot, q)) = hub.next() {
            match q {
                Question::Http(port, req) => {
                    let answer = match node.handle_routed(port, &req, now(app)).await {
                        Ok(Routed::Reply(r)) => Answer::Reply(r),
                        Ok(Routed::Forward(f)) => Answer::Forward(f),
                        Err(f) => fatal(app, &mut node, "api", f).await,
                    };
                    let leaving = node.take_leaving();
                    if leaving {
                        // Vóór het antwoord: de werker leest de vlag zodra
                        // hij het heeft, en sluit erna.
                        LEAVING.store(true, Relaxed);
                    }
                    hub.answer(slot, answer);
                    if leaving {
                        flush(&mut node);
                        leave(&mut node, app, exec, agent_pool, slot).await;
                        return;
                    }
                }
                Question::Poll(ask) => {
                    let chunk = node.poll(&ask, now(app));
                    hub.answer(slot, Answer::Chunk(chunk));
                }
                Question::StreamDone => node.stream_done(),
            }
            flush(&mut node);
        }
        // Een meting van de kloktaak: SET_CLOCK over de hoofdverbinding.
        if let Some(sample) = CLOCK.try_recv() {
            let set = node.set_clock(&sample, exec.now()).await;
            clock_set(app, set, &sample, &ntp_host).await;
        }
        // De antwoorden van de taken van de cluster (lease, link, staat,
        // dispatch); standalone blijft de inbox leeg.
        while let Some(m) = INBOX.try_recv() {
            node.on_mail(m, now(app));
            flush(&mut node);
        }
        let t = now(app);
        if t >= next_tick {
            if let Err(f) = node.tick(t).await {
                fatal(app, &mut node, "tick", f).await;
            }
            next_tick = t.saturating_add(tick_ns);
            flush(&mut node);
        }
        let wait = Duration::from_nanos(next_tick.saturating_sub(now(app)));
        // Hoogstens één brok per ronde: daarna eerst weer de bus (bovenaan).
        match wake(hub, &INBOX, &mut pieces, exec.after(wait), app.stopped()).await {
            Woke::Mail(m) => node.on_mail(m, now(app)),
            Woke::Piece(seq, p) => {
                if let Err(f) = node.on_piece(seq, p, now(app)).await {
                    fatal(app, &mut node, "start", f).await;
                }
            }
            Woke::Logs => {
                if let Err(f) = node.pump_logs(now(app)).await {
                    fatal(app, &mut node, "logs", f).await;
                }
            }
            Woke::Look => {}
            Woke::Stop => {
                // De laatste staat, dan terug: applib doet het net-afscheid
                // en de exit, binnen de gratie van de kern.
                node.on_stop(now(app)).await;
                flush(&mut node);
                return;
            }
        }
        flush(&mut node);
    }
}
