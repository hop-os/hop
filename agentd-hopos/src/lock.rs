//! De lock van de cluster op HopOS: welke opslag de lease en de clusterstaat krijgen, en de async contracten ervan.
//!
//! Een cluster heeft een S3-lock (`HOPOS_S3_*`, leans3 over leans3http en
//! `WebDial`), zoals in Go (`hopos.s3.*`, zie `pkg/agentboot` op
//! github.com/hop-os/hop, tag v1.0.7) en zoals de daemon
//! (`store::open_lease`): zonder type, of met `mem`, blijft de node de
//! standalone leader die hij was. De lease en de staat (`state/<cluster>`)
//! staan in dezelfde bucket, waar [`discovery::state_store_for`] de staat
//! van de daemon ook zet, zodat een host-agent en een HopOS-node van één
//! cluster dezelfde staat lezen.
//!
//! S3 alleen maakt geen cluster: `hopos.s3.*` is ook de object-store van de
//! apps (de store-ops), en een node die daarvoor een bucket krijgt, is niet
//! vanzelf een geclusterde node. De S3-lock vraagt `HOPOS_LOCK_TYPE=s3`,
//! zoals `cluster.lock.type` op de host.
//!
//! | Variabele | Betekenis | Standaard |
//! | --- | --- | --- |
//! | `HOPOS_LOCK_TYPE` | `s3` of `mem` (`hopos.lock.type`) | geen: standalone |
//! | `HOPOS_LOCK_KEY` | de sleutel van het lease-object (`hopos.lock.key`) | `leases/<cluster>` |
//! | `HOPOS_LEASE_TTL` | de lease in seconden (`hopos.lease_ttl`) | 30 |
//! | `HOPOS_ADVERTISE` | `ip` of `ip:poort` zoals de andere nodes deze node zien (`hopos.advertise`) | `HOPOS_NODE_IP` en `HOPOS_PORT` |
//!
//! `HOPOS_ADVERTISE` is er voor een node achter een NAT (QEMU slirp: de host
//! ziet de gast als `127.0.0.1` op een doorgezette poort): het endpoint van de
//! agent en het adres in de lease gebruiken dat adres, de listeners blijven op
//! `HOPOS_PORT` en poort + 1000. De leader-poort is ook hier poort + 1000.
//!
//! Het lease-object is dat van de host (`discovery::wire`): een host-agent
//! en een HopOS-node delen één lease in één bucket.
//!
//! De contracten ([`LeaseBackend`], [`StateBackend`]) zijn die van
//! `discovery::Backend` en `store::StateStore`, maar `async`: een aanroep
//! wacht op het net en geeft de core terug.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::future::Future;

use discovery::LeaseState;

use crate::env::{BootConfig, BootError, S3Config};
use crate::fetch::{Connect, Resolve};
use crate::s3::{S3Lease, S3State};

/// Het type van de lock: `s3` of `mem`.
pub const ENV_LOCK_TYPE: &str = "HOPOS_LOCK_TYPE";
/// De sleutel van het lease-object.
pub const ENV_LOCK_KEY: &str = "HOPOS_LOCK_KEY";
/// De lease in seconden.
pub const ENV_LEASE_TTL: &str = "HOPOS_LEASE_TTL";
/// Het adres zoals de andere nodes deze node zien.
pub const ENV_ADVERTISE: &str = "HOPOS_ADVERTISE";

/// `cfg` met het adres uit [`ENV_ADVERTISE`] als endpoint en lease-adres.
///
/// De kopie is voor de [`crate::Node`] (endpoint, eigen leader-adres) en de
/// lease; de binary bindt zijn poorten op de `cfg` van de env.
pub fn advertised(
    cfg: &BootConfig,
    get: impl Fn(&str) -> Option<String>,
) -> Result<BootConfig, BootError> {
    let Some(v) = get(ENV_ADVERTISE).filter(|v| !v.trim().is_empty()) else {
        return Ok(cfg.clone());
    };
    let bad = || BootError::Bad {
        var: ENV_ADVERTISE,
        value: v.clone(),
    };
    let (ip, port) = match v.trim().rsplit_once(':') {
        Some((ip, p)) => (ip, p.parse::<u16>().map_err(|_| bad())?),
        None => (v.trim(), cfg.port),
    };
    if ip.is_empty() || port == 0 || port > u16::MAX - 1000 {
        return Err(bad());
    }
    let mut out = cfg.clone();
    out.node_ip = String::from(ip);
    out.port = port;
    Ok(out)
}

/// De lease als niets hem zet: 30 s, de standaard van de daemon
/// (`config::TimeoutsConfig::leader_lease`).
pub const DEFAULT_LEASE_TTL_MS: u64 = 30_000;

/// De kortste lease die de bewoner aanneemt.
///
/// De elector antwoordt "wie leidt" uit de laatste lees zolang die jonger is
/// dan één lease, en de verkiezing vraagt het elke 10 s: met een lease
/// onder de tik is het antwoord bij elke vraag verlopen, leest de elector
/// opnieuw, en registreert een agent nooit (gemeten 30-09 op QEMU met 9 s:
/// elke tik "geen leader", vier tikken later een vergeefse overname). 15 s
/// laat een tik en een trage lees ruimte.
pub const MIN_LEASE_TTL_MS: u64 = 15_000;

/// De clusterconfig van de bewoner: de bucket, de sleutels en de lease.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusterConfig {
    /// De bucket van de lease en de gecommitte clusterstaat.
    pub s3: S3Config,
    /// De sleutel van het lease-object.
    pub lease_key: String,
    /// De sleutel van de clusterstaat (`state/<cluster>`).
    pub state_key: String,
    /// De lease in milliseconden.
    pub ttl_ms: u64,
}

impl ClusterConfig {
    /// Leest de lock uit de env; `Ok(None)` is standalone (geen lock-config).
    ///
    /// `s3` is de S3-sectie die [`crate::BootConfig`] al las (endpoint en
    /// bucket allebei gezet).
    pub fn from_env(
        get: impl Fn(&str) -> Option<String>,
        cluster: &str,
        s3: Option<&S3Config>,
    ) -> Result<Option<Self>, BootError> {
        let kind = get(ENV_LOCK_TYPE).unwrap_or_default();
        // Het type kiest, zoals `store::open_lease` op de host.
        let s3 = match kind.trim() {
            "" | "mem" => return Ok(None),
            "s3" => match s3 {
                Some(s) => s.clone(),
                None => {
                    return Err(BootError::Bad {
                        var: ENV_LOCK_TYPE,
                        value: String::from("s3 without HOPOS_S3_ENDPOINT and HOPOS_S3_BUCKET"),
                    });
                }
            },
            _ => {
                return Err(BootError::Bad {
                    var: ENV_LOCK_TYPE,
                    value: kind,
                });
            }
        };
        let ttl_ms = match get(ENV_LEASE_TTL) {
            None => DEFAULT_LEASE_TTL_MS,
            Some(v) => match v.trim().parse::<u64>() {
                Ok(s) if s.saturating_mul(1000) >= MIN_LEASE_TTL_MS => s.saturating_mul(1000),
                _ => {
                    return Err(BootError::Bad {
                        var: ENV_LEASE_TTL,
                        value: v,
                    });
                }
            },
        };
        let lease_key = get(ENV_LOCK_KEY)
            .filter(|k| !k.trim().is_empty())
            .unwrap_or_else(|| format!("leases/{cluster}"));
        Ok(Some(Self {
            s3,
            lease_key,
            state_key: format!("state/{cluster}"),
            ttl_ms,
        }))
    }

    /// Het tijdsbudget per backend-aanroep, zoals op de host.
    pub fn call_timeout_ms(&self) -> u64 {
        discovery::backend_timeout_for(self.ttl_ms)
    }

    /// Een korte omschrijving voor de opstartregel, zonder geheimen.
    pub fn label(&self) -> String {
        format!(
            "s3 ({}/{})",
            crate::client::redact(&self.s3.endpoint),
            self.s3.bucket
        )
    }
}

/// De lease-opslag, async: het contract van `discovery::Backend`.
///
/// Elke schrijf is voorwaardelijk op de vorige handle, en een lege handle
/// betekent "alleen als er nog niets is".
pub trait LeaseBackend {
    /// Leest de lease en zijn handle; `NoLease` als er geen is.
    fn read(&mut self) -> impl Future<Output = discovery::Result<(LeaseState, String)>>;
    /// Schrijft de lease als de huidige handle `prev` is en geeft de nieuwe.
    fn write(
        &mut self,
        prev: &str,
        state: &LeaseState,
    ) -> impl Future<Output = discovery::Result<String>>;
    /// Verwijdert de lease als de huidige handle `handle` is.
    fn delete(&mut self, handle: &str) -> impl Future<Output = discovery::Result>;
    /// Waarom de laatste aanroep `Unreachable` gaf, voor de logregel.
    fn last_error(&self) -> Option<&str>;
}

/// De gecommitte clusterstaat, async: het contract van `store::StateStore`.
///
/// Geen CAS: de schrijver is per constructie de huidige leaseholder.
pub trait StateBackend {
    /// Overschrijft de snapshot.
    fn save(&mut self, snapshot: &[u8]) -> impl Future<Output = Result<(), String>>;
    /// Leest de snapshot; `None` is "er is nog niets", een schone start.
    fn load(&mut self) -> impl Future<Output = Result<Option<Vec<u8>>, String>>;
    /// Waar de staat woont, voor de opstartregel; zonder geheimen.
    fn describe(&self) -> String;
}

/// Opent de lease-backend van `cfg` over `client` (dial, wortels, willekeur).
pub fn open_lease<C: Connect, R: Resolve>(
    cfg: &ClusterConfig,
    client: crate::client::Client<C, R>,
    wall_secs: fn() -> u64,
) -> S3Lease<C, R> {
    let timeout = core::time::Duration::from_millis(cfg.call_timeout_ms());
    S3Lease::new(
        client,
        &cfg.s3,
        &cfg.lease_key,
        timeout,
        cfg.ttl_ms,
        wall_secs,
    )
}

/// Opent de staat-backend van `cfg` over `client`, in de bucket van de lease.
pub fn open_state<C: Connect, R: Resolve>(
    cfg: &ClusterConfig,
    client: crate::client::Client<C, R>,
    wall_secs: fn() -> u64,
) -> S3State<C, R> {
    let timeout = core::time::Duration::from_millis(cfg.call_timeout_ms());
    S3State::new(client, &cfg.s3, &cfg.state_key, timeout, wall_secs)
}
