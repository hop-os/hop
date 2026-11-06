//! Het park van een acceptor: keep-alive-verbindingen tussen twee verzoeken, zonder werker.
//!
//! leanhttp bedient één verzoek per aanroep en geeft een verbinding die mag
//! blijven terug als [`Kept`]. Een werker houdt haar dan niet vast tot het
//! volgende verzoek (of de stilte) komt, maar geeft haar aan de acceptor
//! van zijn poort, en die zet haar hier neer. Het park wacht op
//! [`Readable::readable`], dat niets verbruikt: geen taak en geen timer per
//! verbinding (handboek §2, HopOS docs/apps.md "Pools, geen taak per
//! verbinding"). Komt er iets binnen (bytes, of de FIN van een client die
//! weggaat), dan gaat de verbinding naar een vrije werker; is ze te lang
//! stil, dan gaat ze dicht. De acceptor houdt één timer voor de vroegste
//! stilte ([`Park::next_until`]).
//!
//! Waarom: met keep-alive in de werker zelf legden zoveel stille
//! verbindingen als er werkers zijn de poort stil (gemeten 06-11-2026 op de
//! M4: 16 stille verbindingen en één verzoek, 4,9 s tegen 8 ms met een
//! park). Op de agent-poort van Hop kijkt de canary van HopOS mee: antwoordt
//! die niet, dan reset de node.
//!
//! Dezelfde vorm als het park van `applibhttp` in HopOS. Hop gebruikt die
//! crate niet: elke werker heeft hier zijn eigen vak in de [`crate::Hub`],
//! zijn eigen client voor de doorgiftes en zijn eigen stroom, en de
//! eigenaar wacht na een herstart op de werker die het verzoek droeg
//! ([`crate::Handoff::freed`]); de handler van `applibhttp` is één gedeelde
//! functie zonder werkernummer.

use alloc::vec::Vec;
use core::future::Future;
use core::pin::pin;
use core::task::Context;
use core::time::Duration;

use leanhttp::Kept;

/// Een verbinding waarop het park kan wachten zonder iets te verbruiken.
pub trait Readable {
    /// Klaar zodra er iets te lezen is: bytes, of het einde (een gesloten
    /// verbinding is meteen klaar; de volgende lees zegt wat er is). Een
    /// future die wegvalt voordat hij klaar is, kost niets.
    fn readable(&mut self) -> impl Future<Output = ()>;
}

impl Readable for applib::tcp::TcpConn {
    async fn readable(&mut self) {
        // Een fout (het handvat is weg) is ook "klaar": de werker leest dan
        // het einde en sluit.
        let _ = applib::tcp::TcpConn::readable(self).await;
    }
}

/// Een stille verbinding in het park.
struct Spot<C> {
    kept: Kept<C>,
    /// Tot wanneer ze stil mag zijn, in nanoseconden op de klok van de
    /// executor.
    until: u64,
}

/// De stille verbindingen van één acceptor.
pub struct Park<C> {
    spots: Vec<Spot<C>>,
    cap: usize,
    idle: u64,
}

impl<C: leanhttp::Conn + Readable> Park<C> {
    /// Een leeg park voor hoogstens `cap` verbindingen die elk hoogstens
    /// `idle` stil mogen zijn.
    pub fn new(cap: usize, idle: Duration) -> Self {
        Self {
            spots: Vec::new(),
            cap,
            idle: u64::try_from(idle.as_nanos()).unwrap_or(u64::MAX),
        }
    }

    /// Hoeveel verbindingen er nu staan.
    pub fn len(&self) -> usize {
        self.spots.len()
    }

    /// Of het park leeg is.
    pub fn is_empty(&self) -> bool {
        self.spots.is_empty()
    }

    /// Zet `kept` neer op `now`. Vol (of geen geheugen voor de plek) geeft
    /// haar terug: de acceptor sluit haar, en de client opent een nieuwe.
    ///
    /// # Errors
    ///
    /// De verbinding zelf, als ze er niet bij past.
    pub fn park(&mut self, kept: Kept<C>, now: u64) -> Result<(), Kept<C>> {
        if self.spots.len() >= self.cap || self.spots.try_reserve(1).is_err() {
            return Err(kept);
        }
        self.spots.push(Spot {
            kept,
            until: now.saturating_add(self.idle),
        });
        Ok(())
    }

    /// Eén ronde door het park op `now`: wat leesbaar werd naar `ready`
    /// (voor een werker), wat te lang stil was naar `gone` (om te sluiten).
    /// Op elke verbinding die blijft staan, wacht nu de waker van `cx`.
    /// Zegt of er iets uit het park ging.
    ///
    /// Verloren gaat niets: past een verbinding niet in `ready` of `gone`
    /// (geen geheugen), dan blijft ze staan tot de volgende ronde.
    pub fn sweep(
        &mut self,
        cx: &mut Context<'_>,
        now: u64,
        ready: &mut Vec<Kept<C>>,
        gone: &mut Vec<Kept<C>>,
    ) -> bool {
        let mut moved = false;
        let mut i = 0;
        while i < self.spots.len() {
            let out = match self.spots.get_mut(i) {
                Some(s) if s.until <= now => Some(false),
                Some(s) => pin!(s.kept.conn_mut().readable())
                    .poll(cx)
                    .is_ready()
                    .then_some(true),
                None => None,
            };
            let room = match out {
                Some(true) => ready.try_reserve(1).is_ok(),
                Some(false) => gone.try_reserve(1).is_ok(),
                None => false,
            };
            if !room {
                i += 1;
                continue;
            }
            let spot = self.spots.swap_remove(i);
            moved = true;
            if out == Some(true) {
                ready.push(spot.kept);
            } else {
                gone.push(spot.kept);
            }
        }
        moved
    }

    /// Het vroegste einde van een stilte: daar staat de ene timer van de
    /// acceptor op. `None` met een leeg park.
    pub fn next_until(&self) -> Option<u64> {
        self.spots.iter().map(|s| s.until).min()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::rc::Rc;
    use core::cell::Cell;
    use core::future::poll_fn;
    use core::task::{Poll, Waker};
    use leanhttp::{AsyncRead, AsyncWrite, Close, Exchange, IoError, Outcome};

    /// Een verbinding in het geheugen met een schakelaar voor "leesbaar".
    struct Wire {
        input: Vec<u8>,
        at: usize,
        ready: Rc<Cell<bool>>,
    }

    impl AsyncRead for Wire {
        fn poll_read(
            &mut self,
            _: &mut Context<'_>,
            buf: &mut [u8],
        ) -> Poll<Result<usize, IoError>> {
            let rest = &self.input[self.at..];
            let n = rest.len().min(buf.len());
            buf[..n].copy_from_slice(&rest[..n]);
            self.at += n;
            Poll::Ready(Ok(n))
        }
    }

    impl AsyncWrite for Wire {
        fn poll_write(&mut self, _: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, IoError>> {
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(&mut self, _: &mut Context<'_>) -> Poll<Result<(), IoError>> {
            Poll::Ready(Ok(()))
        }
    }

    impl Close for Wire {
        fn poll_close(&mut self, _: &mut Context<'_>) -> Poll<Result<(), IoError>> {
            Poll::Ready(Ok(()))
        }
    }

    impl Readable for Wire {
        async fn readable(&mut self) {
            let ready = self.ready.clone();
            poll_fn(|_| {
                if ready.get() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;
        }
    }

    /// Een verbinding na één keep-alive-verzoek, met haar schakelaar.
    fn kept() -> (Kept<Wire>, Rc<Cell<bool>>) {
        let ready = Rc::new(Cell::new(false));
        let wire = Wire {
            input: b"GET / HTTP/1.1\r\nHost: n\r\n\r\n".to_vec(),
            at: 0,
            ready: ready.clone(),
        };
        let mut fut = core::pin::pin!(leanhttp::serve(
            wire,
            async |ex: &mut Exchange<'_, Wire>| ex.write(b"ok").await.map(|_| ())
        ));
        let mut cx = Context::from_waker(Waker::noop());
        let Poll::Ready(Ok(Outcome::Kept(k))) = fut.as_mut().poll(&mut cx) else {
            panic!("a keep-alive connection");
        };
        (k, ready)
    }

    fn sweep(park: &mut Park<Wire>, now: u64) -> (bool, Vec<Kept<Wire>>, Vec<Kept<Wire>>) {
        let mut cx = Context::from_waker(Waker::noop());
        let (mut ready, mut gone) = (Vec::new(), Vec::new());
        let moved = park.sweep(&mut cx, now, &mut ready, &mut gone);
        (moved, ready, gone)
    }

    #[test]
    fn a_quiet_connection_waits_and_a_readable_one_goes_to_a_worker() {
        let mut park = Park::new(4, Duration::from_secs(2));
        let (a, a_ready) = kept();
        let (b, _) = kept();
        assert!(park.park(a, 0).is_ok());
        assert!(park.park(b, 0).is_ok());
        let (moved, ready, gone) = sweep(&mut park, 1);
        assert!(!moved && ready.is_empty() && gone.is_empty());
        assert_eq!(park.len(), 2);

        a_ready.set(true);
        let (moved, ready, gone) = sweep(&mut park, 1);
        assert!(moved);
        assert_eq!((ready.len(), gone.len(), park.len()), (1, 0, 1));
    }

    #[test]
    fn too_long_quiet_goes_and_the_alarm_is_the_earliest() {
        let s = 1_000_000_000;
        let mut park = Park::new(4, Duration::from_secs(2));
        assert_eq!(park.next_until(), None);
        assert!(park.park(kept().0, 0).is_ok());
        assert!(park.park(kept().0, s).is_ok());
        assert_eq!(park.next_until(), Some(2 * s));
        let (moved, ready, gone) = sweep(&mut park, 2 * s);
        assert!(moved);
        assert_eq!((ready.len(), gone.len(), park.len()), (0, 1, 1));
        assert_eq!(park.next_until(), Some(3 * s));
    }

    #[test]
    fn a_full_park_hands_the_connection_back() {
        let mut park = Park::new(1, Duration::from_secs(2));
        assert!(park.park(kept().0, 0).is_ok());
        assert!(park.park(kept().0, 0).is_err(), "full: back to close");
        assert_eq!(park.len(), 1);
        assert!(!park.is_empty());
    }
}
