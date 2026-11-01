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
//!   waarde aan een vrije werker ([`Handoff`]); de werker draait leanhttp
//!   en stuurt elk verzoek als bericht naar de eigenaar. Een open stroom
//!   (`/v1/events`, een log-tail) houdt zijn werker vast en vraagt de
//!   eigenaar elke halve seconde wat er bij kwam.
//!
//! - de kloktaak: elk uur een SNTP-meting (`pool.ntp.org`) als bericht naar
//!   de eigenaar, die de klok van de kern zet met `SET_CLOCK` over zijn
//!   eigen verbinding. Pas na de eerste vertrouwen de downloader en de
//!   cluster de wandklok (een keten heeft een datum nodig, een lease een
//!   tijd).
//!
//! Artifacts komen over `http://` en `https://` (`agentd_hopos::fetch`):
//! TLS met de Mozilla-wortels, hostnamen via de resolver.
//!
//! De start, in deze volgorde: de staat van de vorige Hop uit de kern
//! (`Node::restore`: de kooien die nog draaien worden van deze Hop), dan de
//! veger (wat daarna nog draait is van niemand), dan de klok (één meting en
//! `SET_CLOCK`, als stap), dan de cluster (de
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
//! `HOP_FATAL step=<stap>` (een start die niet lukt: Hop stopt met een
//! exitcode per soort, `EXIT_KERNEL`, `EXIT_CONFIG` of `EXIT_INTERN`, en de
//! watchdog reset de node; geen herhaallus), en de weigeringen en
//! degradaties `HOP_RESTART_FAIL`, `HOP_KERNEL_STATE_FAIL`, `HOP_ADOPT_SKIPPED`, `HOPOS_API_INSECURE`,
//! `HOP_SNTP_FAIL`, `HOP_CLOCK_FAIL`, `HOP_TLS_ENTROPY_WEAK`, `HOP_TLS_ENTROPY_HW`,
//! `HOP_INIT_FAIL`, `HOP_JOB_REFUSED` (een job die de leader-API weigerde,
//! zoals een vaste poort die zou rollen) en `HOP_DOWNLOAD_QUEUE_FULL`. In
//! een cluster ook `HOP_CLUSTER`, `HOP_CLUSTER_JOIN`,
//! `HOP_LEADER_LOADING`, `HOP_STATE_LOADED`, `HOP_LEADER_STOPPED`, en de
//! weigeringen `HOP_CLUSTER_NO_CLOCK`, `HOP_LEASE_STORE`,
//! `HOP_LINK_FAIL`, `HOP_RELAY_REFUSED`, `HOP_RELAY_FULL`,
//! `HOP_STATE_SAVE_FAIL`.
//!
//! Canoniek gelinkt (applib/link.ld via build.rs), zoals appspike.
//!
//! Op QEMU: `tools/qemu-test-hop.sh` in de HopOS-repo boot de kern met deze
//! ELF in slot 1 (env, token, wandklok, poorten 8080 en 9080 doorgezet),
//! stuurt van buiten een jobspec en eist appspike in slot 2 (29-09 groen).
//! Wat daar nog niet is: health probes. De cluster over het LAN toetst
//! `tools/qemu-test-cluster.sh` in deze repo. Zonder DNS-server in de env
//! en zonder internet faalt de SNTP-stap met één regel (`HOP_SNTP_FAIL`) en
//! draait Hop door; `http://` naar een adres werkt dan gewoon.

#![cfg_attr(target_os = "none", no_std, no_main)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use core::task::Poll;
use core::time::Duration;

use agentd_hopos::client::{Client, Idle};
use agentd_hopos::download::{self, Orders, Piece, PieceRx, Pieces};
use agentd_hopos::env::INIT_JOBS_FILE;
use agentd_hopos::forward::{self, Routed, STREAM_IDLE};
use agentd_hopos::lock::{self, ClusterConfig};
use agentd_hopos::mail::{DispatchQueue, Inbox, LeaseQueue, LinkQueue, Mail, Nap, StateQueue};
use agentd_hopos::restart;
use agentd_hopos::sntp::{self, NtpLink, PACKET, Sample};
use agentd_hopos::{
    Answer, BootConfig, ClusterParts, Connect, EXIT_CONFIG, EXIT_INTERN, EXIT_KERNEL, Handoff,
    HttpImages, Hub, Images, Node, Port, Question, Resolve,
};
use applib::appnet::{self, Endpoint, Net, NetError, SystemClient, TcpListener, TcpStream};
use applib::rand::{JITTER_ROUNDS, Rng};
use applib::rt::Exec;
use applib::{App, EXEC, log};
use discovery::Discovery;
use hop_http::{Ask, Chunk, Streams, TcpConn};
use hopos_runner::KernSys;
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

/// De langste stilte op een verbinding: een pool van [`WORKERS`] per poort,
/// dus een keep-alive-client mag een werker niet lang ophouden. Zijn alle
/// werkers bezet, dan wacht een nieuwe verbinding niet eens zo lang: de
/// werker sluit zijn verbinding dan na het antwoord
/// (`Handoff::none_free`, zie `forward::serve`).
const READ_CAP: Duration = Duration::from_secs(2);

/// Werkers per poort. Een open stroom houdt er een vast; met hoogstens drie
/// stromen per node (`MAX_STREAMS` in de node) houdt elke poort er minstens
/// één vrij voor de CLI en de GUI.
const WORKERS: usize = 4;

/// Hoe vaak de acceptor kijkt of er een werker vrij is als ze alle bezig
/// zijn. Een koud pad (een vierde gelijktijdige verbinding), dus pollen is
/// eenvoudiger dan een bel.
const BUSY_POLL: Duration = Duration::from_millis(5);

/// Hoe lang een verbinding naar een artifact-server mag duren.
const DIAL_TIMEOUT: Duration = Duration::from_secs(10);

/// Hoe lang één SNTP-vraag op antwoord wacht (Go: 3 s).
const NTP_TIMEOUT: Duration = Duration::from_secs(3);

/// Het ritme van SNTP na een gelukte synchronisatie (Go: elk uur).
const NTP_EVERY: Duration = Duration::from_secs(3600);

/// Na een mislukte synchronisatie eerder opnieuw: een node die boot voor
/// zijn uplink er is, hoeft geen uur op een klok te wachten.
const NTP_RETRY: Duration = Duration::from_secs(300);

/// Zoveel mislukkingen op rij krijgen een eigen regel; daarna één per
/// twaalf (eens per uur bij [`NTP_RETRY`]).
const NTP_LOUD: u32 = 3;

/// Het grootste init-jobs-bestand dat de bewoner leest.
const INIT_FILE_MAX: usize = 64 << 10;

/// Of SNTP in deze boot gelukt is: pas dan is de wandklok van de kern
/// vertrouwd genoeg voor een certificaatdatum. Eén schrijver (de eigenaar,
/// [`set_clock`]), gelezen door de downloader en de clients van de cluster;
/// een vlag, geen protocol.
static CLOCK_SYNCED: AtomicBool = AtomicBool::new(false);

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

/// De eigen regels van Hop die niet uit de [`Node`] komen (de taken van de
/// binary: klok, store, de start). [`say!`] zet ze op de console én hier; de
/// eigenaar zet ze in de ring van de systeemtaak `hop` (`/logs/hop/stdout`),
/// in het proces en niet via de console van de kern. Vol is droppen: een
/// logregel mag de node nooit ophouden.
static HOP_LOG: Mailbox<String, HOP_LOG_Q> = Mailbox::new();

/// Zoveel regels wachten hoogstens op de eigenaar: de start zegt er een stuk
/// of tien voordat de node bestaat.
const HOP_LOG_Q: usize = 32;

/// `applib::log!` met een kopie in [`HOP_LOG`]: een regel van Hop op de
/// console en in de ring van `hop`.
macro_rules! say {
    ($($arg:tt)*) => {{
        let line = format!($($arg)*);
        log!("{line}");
        let _ = HOP_LOG.try_send(line);
    }};
}

/// Stopt Hop hard: één regel met de reden en `HOP_FATAL step=<stap>`, het
/// net-afscheid en de exit met `code` (een `EXIT_*` per soort).
///
/// De enige weg uit een start die niet lukt (besluit S1, 01-11): een falende
/// Hop is een falende node. De kern laat hem liggen (`HOPOS_HOP_EXIT`) en de
/// watchdog reset de node. Geen `return` (exit 0), geen `pending()` (een
/// node die slaat maar niets doet), geen regel-en-door.
async fn fail(app: &App, code: u64, step: &str, why: impl core::fmt::Display) -> ! {
    say!("hop: {why}; stopping (exit {code}) HOP_FATAL step={step}");
    app.shutdown(code).await;
    // `shutdown` keert nooit terug (`App::exit` parkeert de core); dat staat
    // alleen niet in zijn type.
    match core::future::pending::<core::convert::Infallible>().await {}
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

/// Of SNTP in deze boot lukte: pas dan doet een geclusterde node mee.
fn clock_ok() -> bool {
    CLOCK_SYNCED.load(Relaxed)
}

/// Nu in Unix-seconden, alleen na SNTP: de datum van een certificaatketen.
fn trusted_secs() -> Option<u64> {
    CLOCK_SYNCED.load(Relaxed).then(wall_secs)
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

/// De kloktaak: na `wait`, en daarna elk uur (na een mislukte meting na
/// [`NTP_RETRY`]), één SNTP-meting als bericht naar de eigenaar ([`CLOCK`]).
/// De eigenaar zet de klok van de kern met `SET_CLOCK` over de
/// hoofdverbinding ([`set_clock`]).
///
/// Geen eigen system-verbinding (besluit O9, 01-11): Hop houdt er twee vast,
/// de eigenaar en de store-taak, en de derde plaats van `MAX_HOP_CONNS` is
/// voor de ene herverbinding van applib, zoals bij een app. Het net (DNS en
/// UDP) wacht hier, niet in de eigenaar. Een NTP-server die niet antwoordt is
/// het net, geen fout van de node: luid, drie keer, dan één op twaalf.
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
                    say!(
                        "hop: SNTP {server}:{port} failed ({fails}x): {e}; the clock stays as it is HOP_SNTP_FAIL"
                    );
                }
                NTP_RETRY
            }
        };
    }
}

/// Zet de klok van de kern uit een meting, over de hoofdverbinding: bij de
/// start als stap in de reeks (`started` onwaar), daarna voor de metingen
/// van de kloktaak. Een kern die `SET_CLOCK` weigert, of bij de start niet
/// antwoordt, is een kapotte node: [`fail`].
async fn set_clock<S, I>(
    app: &App,
    exec: &Exec,
    node: &mut Node<S, I>,
    sample: &Sample,
    server: &str,
    started: bool,
) where
    S: runner::SystemApi + agent::Store,
    I: Images,
{
    match node.set_clock(sample, exec.now(), started).await {
        sntp::ClockStep::Synced(unix_ns) => {
            CLOCK_SYNCED.store(true, Relaxed);
            say!(
                "hop: clock set from {server} (stratum {}, delay {} us) to {} s HOP_CLOCK_SYNCED",
                sample.stratum,
                sample.delay_ns / 1000,
                unix_ns / 1_000_000_000
            );
        }
        sntp::ClockStep::Later(e) => say!(
            "hop: SET_CLOCK: {e}; the clock stays as it was until the next sync HOP_CLOCK_FAIL"
        ),
        sntp::ClockStep::Fatal(e) => {
            fail(app, EXIT_KERNEL, "clock", format_args!("SET_CLOCK: {e}")).await
        }
    }
}

/// De object-store van de apps (`agentd_hopos::objstore`): haalt de
/// store-calls van de apps op bij de kern (`NEXT_STORE`, een lange wacht),
/// doet de S3-kant en meldt af. Een eigen system-verbinding, blijvend: de
/// kern geeft het slot van Hop er één meer dan een app (`MAX_HOP_CONNS`).
/// Zonder `HOPOS_S3_*` weigert hij elke call luid, zodat een app hoort
/// waarom zijn pull faalt.
async fn store_task(
    app: &'static App,
    net: &'static Net,
    exec: &'static Exec,
    s3: Option<agentd_hopos::env::S3Config>,
    cluster: String,
) {
    use agentd_hopos::objstore::{Service, bucket};
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
            trusted_secs,
        );
        if b.is_plain_http() {
            say!(
                "hop: object store {} is plain http: requests are signed but readable on the way; use https outside a test HOP_STORE_PLAIN_HTTP",
                cfg.endpoint
            );
        }
        say!(
            "hop: object store for apps: {} bucket {} under apps/{cluster}/ HOP_STORE_UP",
            cfg.endpoint,
            cfg.bucket
        );
        b
    });
    if bucket.is_none() {
        say!(
            "hop: no object store configured (hopos.s3.*); app store calls are refused HOP_STORE_NONE"
        );
    }
    let mut svc = Service::new(bucket, &cluster);
    let mut sys = KernSys::new(net.system_client(), 1);
    let mut refused: u32 = 0;
    loop {
        match svc.serve_one(&mut sys).await {
            // Een pull van iets dat er niet is, is een vraag, geen fout.
            Ok(Some(o))
                if !matches!(
                    o.status,
                    runner::StoreStatus::Ok | runner::StoreStatus::NotFound
                ) =>
            {
                say!(
                    "hop: store {} {} for slot {}: {} HOP_STORE_FAIL",
                    o.task.op.name(),
                    o.task.key,
                    o.task.slot.0,
                    o.why
                )
            }
            Ok(Some(o)) if !o.delivered => say!(
                "hop: store {} {} for slot {}: the task was gone, result dropped HOP_STORE_GONE",
                o.task.op.name(),
                o.task.key,
                o.task.slot.0
            ),
            Ok(_) => refused = 0,
            Err(e) => {
                // Een kern zonder verbinding: luid, de eerste paar keer, dan
                // eens per minuut.
                refused = refused.saturating_add(1);
                if refused <= 3 || refused.is_multiple_of(60) {
                    say!("hop: NEXT_STORE failed ({refused}x): {e} HOP_STORE_KERNEL");
                }
                exec.after(Duration::from_secs(1)).await;
            }
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
        o if o.is_hardware() => say!(
            "hop: TLS randomness from the kernel seed ({o}, hardware) mixed with timer jitter ({JITTER_ROUNDS} samples) HOP_TLS_ENTROPY_HW source={o}"
        ),
        o => say!(
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
        trusted_secs,
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
    say!(
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
        clock_ok,
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
    /// De bus of de tik: de lus kijkt zelf.
    Look,
    /// De stop-bel van de kern.
    Stop,
}

/// Wacht op een verzoek in de bus, een bericht van de cluster, een brok van
/// de downloadtaak, de tik, of de stop-bel.
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
/// er een te veel zijn (`MAX_HOP_CONNS`). Geen bestand, of geen schijf (de
/// kern weigert de stat dan: "no storage layer on board"), is geen
/// init-jobs. Init-jobs die er zijn maar niet kloppen (te groot, geen
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

/// Leest [`INIT_JOBS_FILE`]; `None` zonder bestand of zonder schijf, een
/// fout met zijn exitcode.
async fn read_init_file(c: &mut SystemClient) -> Result<Option<String>, (u64, String)> {
    use applib::sys::Error;
    let kern = |e: Error| (EXIT_KERNEL, format!("{e}"));
    let size = match c.stat(INIT_JOBS_FILE).await {
        Ok(s) => usize::try_from(s).unwrap_or(usize::MAX),
        Err(Error::NotFound { .. } | Error::Call { .. }) => return Ok(None),
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
            say!(
                "hop: download {seq} waits: the download queue is full ({}) HOP_DOWNLOAD_QUEUE_FULL",
                download::ORDERS
            );
        }
    }
    WANTED.store(node.wanted(), Relaxed);
}

/// De acceptor van één poort: elke verbinding als waarde naar een vrije werker.
///
/// De listener is al gebonden vóór de spawn: zodra `HOP_UP` op het log
/// staat, neemt de stack een SYN aan, ook als deze taak nog geen ronde had.
async fn accept(
    listener: TcpListener,
    exec: &'static Exec,
    pool: &'static Handoff<TcpStream>,
    number: u16,
) {
    loop {
        let mut stream = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                say!("hop: accept on :{number}: {e}");
                exec.after(Duration::from_millis(100)).await;
                continue;
            }
        };
        // Alle werkers bezig: kort wachten tot er een vrijkomt.
        while let Err(back) = pool.give(stream) {
            stream = back;
            exec.after(BUSY_POLL).await;
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

/// Eén werker: wacht op een verbinding, leanhttp, elk verzoek als bericht
/// naar de eigenaar met vak `slot` in de bus. Een doorgifte naar een andere
/// node (de leader, of een agent als deze node leidt) voert de werker zelf
/// uit met zijn eigen client (`agentd_hopos::forward`).
async fn work(
    pool: &'static Handoff<TcpStream>,
    i: usize,
    exec: &'static Exec,
    hub: &'static Hub,
    slot: usize,
    port: Port,
    mut client: Client<ClusterConnect, SlotResolver>,
) {
    let mut streams = HubStreams { hub, exec, slot };
    loop {
        let stream = pool.take(i).await;
        let conn = TcpConn::new(stream, exec).with_read_cap(READ_CAP);
        // Een verbinding die eindigt met een termijn of een reset is gewoon
        // een client die wegging; dat is geen logregel waard.
        let _ = forward::serve(
            conn,
            async |req| hub.ask_routed(slot, port, req).await,
            &mut streams,
            &mut client,
            port,
            || pool.none_free() || LEAVING.load(Relaxed),
        )
        .await;
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
    agent_pool: Option<&'static Handoff<TcpStream>>,
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
    say!("hop: resident starting in slot {} HOP_BOOT", app.slot());
    let net = match appnet::up(app) {
        Ok(n) => n,
        Err(e) => {
            fail(
                app,
                EXIT_INTERN,
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
        say!("hop: WARNING API authentication is OFF (HOPOS_INSECURE=1) HOPOS_API_INSECURE");
    }
    if cfg.insecure_ignored {
        say!("hop: HOPOS_INSECURE=1 ignored: HOPOS_APIKEY is set, the API authenticates");
    }
    if app.wall_ns().is_none() {
        // De kern zet de wandklok vóór de eerste instructie van Hop
        // (HOPOS_CLOCK_FIXED tot SNTP); zonder is hij kapot.
        fail(app, EXIT_KERNEL, "clock", "no wall clock from the kernel").await;
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
            EXIT_INTERN,
            "spawn",
            format_args!("the store task: {e}"),
        )
        .await;
    }

    let mut sys = KernSys::new(net.system_client(), cfg.cores);
    // De init-jobs nu al, over de eigen verbinding: geclusterd zaait de
    // leider ze, alleen gebruikt een schone staat ze (hieronder).
    let mut specs = init_specs(app, &cfg, sys.conn_mut()).await;
    let images = HttpImages::new(
        SlotConnect { net, exec },
        SlotResolver,
        trusted_secs,
        wall_secs,
        entropy(app),
    );
    // De artifacts haalt de downloadtaak; de node houdt een eigen
    // downloader voor de kernbundel van een flip (zeldzaam, en die wacht op
    // de kern zelf, crate::flip).
    let Some((pieces_tx, mut pieces)) = PIECES.split() else {
        fail(
            app,
            EXIT_INTERN,
            "spawn",
            "the download queue was already taken",
        )
        .await
    };
    if let Err(e) = exec.spawn(download::download_task(images, &ORDERS, pieces_tx, &WANTED)) {
        fail(
            app,
            EXIT_INTERN,
            "spawn",
            format_args!("the download task: {e}"),
        )
        .await;
    }
    let images = HttpImages::new(
        SlotConnect { net, exec },
        SlotResolver,
        trusted_secs,
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
                Err(e) => fail(app, EXIT_INTERN, "spawn", format_args!("the cluster: {e}")).await,
            }
        }
    };
    // Eerst de staat van de vorige Hop uit de kern: wat nog draait, wordt
    // van deze, vóór de veger en vóór de eerste registratie bij een leader.
    // Een blob in kerngeheugen sterft met de boot, dus geen spook van een
    // vorige (7726782). Wat daarna nog draait, is van niemand.
    while let Some(l) = HOP_LOG.try_recv() {
        node.hop_line(&l);
    }
    let kept = match node.restore(now(app)).await {
        Ok(k) => k,
        Err(f) => {
            flush(&mut node);
            fail(app, f.code, "restore", f.why).await
        }
    };
    if let Err(f) = node.sweep_strays().await {
        flush(&mut node);
        fail(app, f.code, "sweep", f.why).await;
    }
    flush(&mut node);
    // De klok, als stap in de reeks (besluit O9, 01-11): één meting, en
    // SET_CLOCK over de hoofdverbinding. Vóór het zaaien en de cluster: een
    // artifact over https vertrouwt pas een gezette klok (F8), en een lease is
    // een tijd op de wandklok. De tijdserver: `HOPOS_NTP` (`host` of
    // `host:poort`), anders pool.ntp.org.
    //
    // Na een herstart zette de vorige Hop de klok al (de staat zegt het): die
    // vertrouwt deze Hop, en hij meet pas over een uur zelf (besluit H5,
    // 01-11). Een herstart hangt zo niet af van een server buiten het LAN.
    let (ntp_host, ntp_port) = sntp::server_from(app.env(sntp::ENV_NTP));
    let next_sync = if node.clock() {
        CLOCK_SYNCED.store(true, Relaxed);
        say!(
            "hop: the previous Hop set the kernel's clock; trusting it HOP_CLOCK_SYNCED from=state"
        );
        NTP_EVERY
    } else {
        match measure(net, exec, &ntp_host, ntp_port, &mut 0).await {
            Ok(sample) => {
                set_clock(app, exec, &mut node, &sample, &ntp_host, false).await;
                NTP_EVERY
            }
            Err(e) => {
                say!(
                    "hop: SNTP {ntp_host}:{ntp_port} failed: {e}; the wall clock is not synced, https and the cluster wait for it HOP_SNTP_FAIL"
                );
                NTP_RETRY
            }
        }
    };
    let clock = clock_task(net, exec, ntp_host.clone(), ntp_port, next_sync);
    if let Err(e) = exec.spawn(clock) {
        fail(
            app,
            EXIT_INTERN,
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
        if let Err(e) = node.seed_init_jobs(jobs, now(app)).await {
            fail(app, EXIT_INTERN, "init", e).await;
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
    let mut agent_pool: Option<&'static Handoff<TcpStream>> = None;
    for (first_slot, port, number) in ports {
        let l = match net.tcp_listen(number) {
            Ok(l) => l,
            Err(e) => {
                fail(
                    app,
                    EXIT_INTERN,
                    "listen",
                    format_args!("port :{number}: {e}"),
                )
                .await
            }
        };
        let pool: &'static Handoff<TcpStream> = Box::leak(Box::new(Handoff::new(WORKERS)));
        if port == Port::Agent {
            agent_pool = Some(pool);
        }
        let mut spawned = exec.spawn(accept(l, exec, pool, number));
        for i in 0..WORKERS {
            if spawned.is_err() {
                break;
            }
            let tag = u8::try_from(16 + first_slot + i).unwrap_or(u8::MAX);
            let client = cluster_client(app, net, exec, tag, STREAM_IDLE);
            spawned = exec.spawn(work(pool, i, exec, hub, first_slot + i, port, client));
        }
        if let Err(e) = spawned {
            fail(
                app,
                EXIT_INTERN,
                "spawn",
                format_args!("the listeners: {e}"),
            )
            .await;
        }
    }
    say!(
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
                        Routed::Reply(r) => Answer::Reply(r),
                        Routed::Forward(f) => Answer::Forward(f),
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
            set_clock(app, exec, &mut node, &sample, &ntp_host, true).await;
        }
        // De regels van de andere taken van Hop naar de ring van `hop`.
        while let Some(l) = HOP_LOG.try_recv() {
            node.hop_line(&l);
        }
        // De antwoorden van de taken van de cluster (lease, link, staat,
        // dispatch); standalone blijft de inbox leeg.
        while let Some(m) = INBOX.try_recv() {
            node.on_mail(m, now(app));
            flush(&mut node);
        }
        let t = now(app);
        if t >= next_tick {
            node.tick(t).await;
            next_tick = t.saturating_add(tick_ns);
            flush(&mut node);
        }
        let wait = Duration::from_nanos(next_tick.saturating_sub(now(app)));
        // Hoogstens één brok per ronde: daarna eerst weer de bus (bovenaan).
        match wake(hub, &INBOX, &mut pieces, exec.after(wait), app.stopped()).await {
            Woke::Mail(m) => node.on_mail(m, now(app)),
            Woke::Piece(seq, p) => node.on_piece(seq, p, now(app)).await,
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
