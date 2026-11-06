//! hopprom as a HopOS resident: polls Hop from its slot and serves
//! `/metrics` and `/health` on the port of its jobspec.
//!
//! Twee taken op de executor van de app-core (handboek §2), geen mutex:
//!
//! - **poll** (de app-taak zelf): bezit de [`Collector`] en de
//!   [`SlotSource`]. Elke `HOPPROM_INTERVAL` één ronde; een geslaagde ronde
//!   schrijft zijn [`Metrics`] in [`LATEST`].
//! - **server**: `applibhttp` uit HopOS op `ER_PORT_METRICS` (standaard
//!   9090; de kern zet de poort van de jobspec op de uplink door naar dit
//!   slot): een acceptor met een park voor stille keep-alive-verbindingen
//!   (een scraper die zijn verbinding tussen twee scrapes openhoudt, houdt
//!   zo geen werker vast) en [`WORKERS`] werkers. Een scrape leent
//!   [`LATEST`] binnen één synchrone render en schrijft daarna pas.
//!
//! De env: `HOPPROM_AGENT` (standaard `http://HOP:8080`, met `HOP` voor het
//! slot van Hop), `HOPPROM_INTERVAL` (een Go-duur, standaard `5s`),
//! `HOPPROM_API_KEY` (de clustersleutel; `HOP_API_KEY` van de Go-generatie
//! telt ook).
//!
//! Markers: `HOPOS_HOPPROM_UP port=<n>` als de listener staat,
//! `HOPOS_HOPPROM_POLL agents=<n> tasks=<n>` na de eerste geslaagde ronde
//! en telkens als een van de twee getallen verandert, en
//! `HOPOS_HOPPROM_POLL_FAIL` als een ronde faalt (één keer per nieuwe
//! reden). Per ronde niets: vijf seconden is te vaak voor de console.

#![cfg_attr(target_os = "none", no_std, no_main)]
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::indexing_slicing
    )
)]

extern crate alloc;

use alloc::boxed::Box;
use core::cell::RefCell;
use core::time::Duration;

use applib::appnet::{self, TcpListener};
use applib::rt::Exec;
use applib::tcp::TcpConn;
use applib::{App, EXEC, log};
use hoplib::Reason;
use hopprom::hopos::SlotSource;
use hopprom::http::{self, Route};
use hopprom::{Collector, Metrics, config};
use leanhttp::{Exchange, Mux};
use sync::Local;
use types::Time;

applib::main!(hopprom_main);

/// Op de host bestaat dit image niet: daar is dit een lege binary, zodat
/// clippy de rest kan lezen.
#[cfg(not(target_os = "none"))]
fn main() {}

/// De werkers van de server. Tot 06-11-2026 bediende één taak de
/// verbindingen één voor één; twee laten een health-check door terwijl een
/// trage scraper leest.
const WORKERS: usize = 2;

/// Zoveel stille keep-alive-verbindingen houdt het park (een paar
/// Prometheus-servers en een health-check); daarboven gaat een verbinding
/// na haar antwoord dicht.
const PARKED: usize = 16;

/// De kap op elke leestermijn van een verzoek (kop en body): een client die
/// langzaam druppelt, houdt een werker hooguit zo lang vast. Tot 06-11-2026
/// ook de stilte tussen twee verzoeken; die is nu van het park ([`IDLE`]).
const READ_CAP: Duration = Duration::from_secs(5);

/// Hoe lang een keep-alive-verbinding stil in het park mag staan: dezelfde
/// 5 s als toen de werker haar zelf vasthield ([`READ_CAP`]).
const IDLE: Duration = Duration::from_secs(5);

/// De server: acceptor, park en werkers (`applibhttp`).
static HTTP: applibhttp::Http<WORKERS, PARKED> = applibhttp::Http::new()
    .with_read_cap(READ_CAP)
    .with_idle(IDLE);

/// De metrics van de laatste geslaagde ronde. Een leesbare tabel (handboek
/// §1.1): de poll-taak schrijft, de server leest, beide op deze executor en
/// nooit over een `.await` geleend.
static LATEST: Local<RefCell<Option<Metrics>>> = Local::new(RefCell::new(None));

#[expect(
    clippy::expect_used,
    reason = "de start van de bin: zonder netstack, poort of server is er niets te exporteren, en een luide paniek met reden is het goede einde"
)]
async fn hopprom_main(app: &'static App) {
    let exec: &'static Exec = EXEC.get();
    let port = config::port(app.env("ER_PORT_METRICS")).unwrap_or_else(|p| {
        log!("hopprom: ER_PORT_METRICS is not a port, using {p} HOPOS_HOPPROM_PORT");
        p
    });
    let interval = config::interval(app.env("HOPPROM_INTERVAL")).unwrap_or_else(|d| {
        log!(
            "hopprom: HOPPROM_INTERVAL={:?} is not a duration above zero, using {}s HOPOS_HOPPROM_INTERVAL",
            app.env("HOPPROM_INTERVAL"),
            d.as_secs()
        );
        d
    });
    let agent = app.env("HOPPROM_AGENT").unwrap_or(config::DEFAULT_AGENT);
    let key = app
        .env("HOPPROM_API_KEY")
        .or_else(|| app.env("HOP_API_KEY"));
    let src =
        SlotSource::new(agent, key, exec).expect("hopprom: HOPPROM_AGENT is not a usable URL");

    let net = appnet::up(app).expect("hopprom: network stack");
    let listener = TcpListener::bind(port).expect("hopprom: listen on ER_PORT_METRICS");
    let mux: &'static Mux<Route> = Box::leak(Box::new(http::mux().expect("hopprom: routes")));
    exec.spawn(server(listener, mux, exec, app))
        .expect("hopprom: spawn server");
    let [a, b, c, d] = net.ip();
    log!(
        "hopprom {}: serving /metrics on {a}.{b}.{c}.{d}:{port}, polling {} every {}ms HOPOS_HOPPROM_UP port={port}",
        env!("CARGO_PKG_VERSION"),
        src.base(),
        interval.as_millis()
    );
    poll_loop(src, interval, exec, app).await;
}

/// De wandklok, als de kern hem al synct.
fn wall(app: &App) -> Option<Time> {
    app.wall_ns().map(Time)
}

/// De poll-taak: eigenaar van de collector.
async fn poll_loop(mut src: SlotSource, interval: Duration, exec: &'static Exec, app: &App) {
    let mut collector = Collector::new();
    let mut shown: Option<(usize, usize)> = None;
    let mut failing: Option<Reason> = None;
    let mut missing: Option<Reason> = None;
    loop {
        let started = exec.now();
        match hopprom::poll(&mut src).await {
            Ok((p, misses)) => {
                // Een best-effort-misser: één regel per nieuwe reden.
                let first = misses.first().map(|m| Reason::of(m));
                if first.is_some() && first != missing {
                    for m in &misses {
                        log!("hopprom: {m}, keeping the last value HOPOS_HOPPROM_MISS");
                    }
                }
                missing = first;
                match collector.observe(&p, wall(app)) {
                    Ok(m) => {
                        LATEST.get().replace(Some(m));
                        let counts = (p.agents.len(), p.task_count());
                        if shown != Some(counts) || failing.is_some() {
                            log!(
                                "hopprom: polled {}: HOPOS_HOPPROM_POLL agents={} tasks={}",
                                src.base(),
                                counts.0,
                                counts.1
                            );
                            shown = Some(counts);
                        }
                        failing = None;
                    }
                    Err(e) => log!("hopprom: collect: {e} HOPOS_HOPPROM_POLL_FAIL"),
                }
            }
            Err(miss) => {
                let why = Reason::of(&miss);
                if failing != Some(why) {
                    log!(
                        "hopprom: poll {}: {why} HOPOS_HOPPROM_POLL_FAIL",
                        src.base()
                    );
                }
                failing = Some(why);
            }
        }
        let spent = Duration::from_nanos(exec.now().saturating_sub(started));
        exec.after(interval.saturating_sub(spent)).await;
    }
}

/// De server-taak: de acceptor van `applibhttp`, met [`WORKERS`] werkers.
async fn server(
    listener: TcpListener,
    mux: &'static Mux<Route>,
    exec: &'static Exec,
    app: &'static App,
) {
    // Een verbinding die eindigt met een termijn of een reset is een
    // scraper die wegging; geen logregel waard (applibhttp logt niet).
    let handler: &'static _ = Box::leak(Box::new(async |ex: &mut Exchange<'_, TcpConn>| {
        http::handle(ex, mux, || {
            // De lening eindigt in dit blok, vóór de eerste schrijf.
            let latest = LATEST.get().borrow();
            http::body(latest.as_ref(), wall(app))
        })
        .await
    }));
    // Keert alleen terug als er geen werker te spawnen was.
    HTTP.run(exec, listener, handler).await;
    log!("hopprom: no server worker could be spawned, /metrics is down HOPOS_HOPPROM_SERVE_FAIL");
}
