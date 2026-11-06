//! `hop-gui-hopos`: het Hop-dashboard als app in een slot van HopOS.
//!
//! Hop plaatst deze ELF met een jobspec (`"driver":"hop"`, een artifact,
//! `"ports":{"http":80}`); de kern zet poort 80 van de uplink door naar het
//! slot (DNAT), en Hop geeft de poort mee in `ER_PORT_HTTP`. Eén
//! HTTP-server (leanhttp over `applib::tcp::TcpConn`) serveert de
//! ingebakken bestanden met [`hop_gui::serve`], dezelfde handler als de
//! host-binary.
//!
//! De vorm (handboek §2): `applibhttp` uit HopOS, een acceptor met een park
//! voor stille keep-alive-verbindingen en een vaste pool van [`WORKERS`]
//! werkers. Een werker bedient één verzoek; een verbinding die mag blijven,
//! wacht in het park van de acceptor, niet bij een werker. Geen taak per
//! verbinding.
//!
//! Marker: `HOPOS_HOPGUI_UP files=<n> bytes=<n> port=<n>` als de listener
//! staat. Per verzoek niets: een dashboard dat elke tien seconden ververst,
//! hoort de console niet te vullen.

#![cfg_attr(target_os = "none", no_std, no_main)]

extern crate alloc;

use alloc::boxed::Box;
use applib::appnet::{self, TcpListener};
use applib::rt::Exec;
use applib::tcp::TcpConn;
use applib::{App, EXEC, log};
use core::time::Duration;
use leanhttp::Exchange;

applib::main!(hop_gui_app);

/// Op de host bestaat dit image niet: daar is dit een lege binary, zodat
/// clippy de rest kan lezen.
#[cfg(not(target_os = "none"))]
fn main() {}

/// De poort zonder `ER_PORT_HTTP` (een image buiten Hop om).
const DEFAULT_PORT: u16 = 80;

/// De werkers: zoveel verzoeken tegelijk. Een browser opent er tot zes
/// naar één host bij de eerste lading (html, twee scripts, de stijlen, het
/// font); vier dekt dat, de rest wacht kort op de eerste die vrijkomt.
const WORKERS: usize = 4;

/// Zoveel stille keep-alive-verbindingen houdt het park; daarboven gaat een
/// verbinding na haar antwoord dicht. Een paar browsers met elk zes.
const PARKED: usize = 32;

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

#[expect(
    clippy::expect_used,
    clippy::panic,
    reason = "de start van de bin: zonder netstack, poort of werker is er niets te serveren, en een luide paniek met reden is het goede einde"
)]
async fn hop_gui_app(app: &'static App) {
    let exec: &'static Exec = EXEC.get();
    let port = port_of(app.env("ER_PORT_HTTP"));
    let net = appnet::up(app).expect("hop-gui: network stack");
    let listener = TcpListener::bind(port).expect("hop-gui: listen on ER_PORT_HTTP");
    let [a, b, c, d] = net.ip();
    log!(
        "hop-gui: serving the Hop dashboard on {a}.{b}.{c}.{d}:{port}, slot {}, {WORKERS} workers HOPOS_HOPGUI_UP files={} bytes={} port={port}",
        app.slot(),
        hop_gui::FILES.len(),
        hop_gui::total_bytes()
    );
    // Een verbinding die eindigt met een termijn of een reset is een
    // browser die wegging; dat is geen logregel waard (applibhttp logt niet).
    let handler: &'static _ = Box::leak(Box::new(async |ex: &mut Exchange<'_, TcpConn>| {
        hop_gui::serve(ex).await
    }));
    // Keert alleen terug als er geen werker te spawnen was.
    HTTP.run(exec, listener, handler).await;
    panic!("hop-gui: no worker could be spawned HOPOS_HOPGUI_SPAWN");
}

/// De poort uit `ER_PORT_HTTP`, of [`DEFAULT_PORT`] zonder of bij onzin
/// (luid: een jobspec die iets anders bedoelde, moet dat kunnen zien).
fn port_of(env: Option<&str>) -> u16 {
    match env.map(str::parse::<u16>) {
        None => DEFAULT_PORT,
        Some(Ok(p)) if p != 0 => p,
        Some(_) => {
            log!(
                "hop-gui: ER_PORT_HTTP={env:?} is not a port, using {DEFAULT_PORT} HOPOS_HOPGUI_PORT"
            );
            DEFAULT_PORT
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_port_comes_from_the_env_or_is_80() {
        assert_eq!(port_of(Some("8081")), 8081);
        assert_eq!(port_of(None), 80);
        assert_eq!(port_of(Some("0")), 80);
    }
}
