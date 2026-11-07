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
//! Dezelfde vorm als het park van `applibhttp` in HopOS, maar Hop houdt
//! zijn eigen (N1, 07-11): de eigenaar wacht na een herstart op de werker
//! die het antwoord droeg ([`crate::Handoff::freed`]), en een werker sluit
//! zijn verbinding na het antwoord als geen andere vrij is
//! ([`crate::Handoff::none_free`]). De deuren van `applibhttp` kennen geen
//! van beide.

use alloc::vec::Vec;
use core::future::{Future, poll_fn};
use core::pin::pin;
use core::task::{Context, Poll};
use core::time::Duration;

use leanhttp::Kept;
use sync::mpsc::Mailbox;

use crate::Handoff;

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

    /// Zet wat de werkers teruggaven (`back`) neer op `now`; wat er niet
    /// bij past, gaat naar `gone`. Op `back` wacht nu de waker van `cx`.
    /// Zegt of er iets kwam.
    pub fn take_back<const N: usize>(
        &mut self,
        cx: &mut Context<'_>,
        back: &Mailbox<Kept<C>, N>,
        now: u64,
        gone: &mut Vec<Kept<C>>,
    ) -> bool {
        let mut came = false;
        while let Poll::Ready(kept) = pin!(back.recv()).poll(cx) {
            came = true;
            if let Err(kept) = self.park(kept, now) {
                gone.push(kept);
            }
        }
        came
    }

    /// Geeft `job` aan een vrije werker van `pool`. Zijn ze alle bezig, dan
    /// wacht dit op de bel van [`Handoff::free`], en zet intussen wat de
    /// werkers teruggeven hier neer ([`Park::take_back`]): anders loopt
    /// `back` vol en sluit een werker een goede verbinding (zoals
    /// `applibhttp` in HopOS sinds rc4).
    pub async fn place<T, const N: usize>(
        &mut self,
        pool: &Handoff<T>,
        back: &Mailbox<Kept<C>, N>,
        job: T,
        now: impl Fn() -> u64,
        gone: &mut Vec<Kept<C>>,
    ) {
        let mut placing = pin!(pool.place(job));
        poll_fn(|cx| {
            self.take_back(cx, back, now(), gone);
            placing.as_mut().poll(cx)
        })
        .await;
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
    use core::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use core::task::Waker;
    use leanhttp::{AsyncRead, AsyncWrite, Close, Exchange, IoError, Outcome};
    use std::sync::Arc;
    use std::task::Wake;

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

    /// Wekt en telt.
    struct Count(AtomicUsize);

    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, SeqCst);
        }
    }

    #[test]
    fn a_full_pool_waits_on_its_bell_and_parks_what_comes_back() {
        // Eén werker, bezig, en plek voor één verbinding in `back`.
        let pool: Handoff<u8> = Handoff::new(1);
        let back: Mailbox<Kept<Wire>, 1> = Mailbox::new();
        let mut park = Park::new(4, Duration::from_secs(2));
        let mut gone = Vec::new();
        assert_eq!(pool.give(1), Ok(0));
        let count = Arc::new(Count(AtomicUsize::new(0)));
        let waker = Waker::from(count.clone());
        let mut cx = Context::from_waker(&waker);
        let woken = || count.0.load(SeqCst);
        {
            let mut placing = pin!(park.place(&pool, &back, 2, || 0, &mut gone));
            assert!(placing.as_mut().poll(&mut cx).is_pending());
            // Twee keep-alives terug terwijl de acceptor wacht: de rij van
            // één loopt niet vol, dus geen werker sluit een goede verbinding.
            for _ in 0..2 {
                let before = woken();
                assert!(back.try_send(kept().0).is_ok(), "back is emptied");
                assert!(woken() > before, "back rings the acceptor");
                assert!(placing.as_mut().poll(&mut cx).is_pending());
            }
            // De werker komt vrij: de bel, geen klok.
            let before = woken();
            pool.free(0);
            assert!(woken() > before, "free rings the acceptor");
            assert!(placing.as_mut().poll(&mut cx).is_ready());
        }
        assert_eq!((park.len(), gone.len()), (2, 0));
        assert!(pool.none_free(), "the job is with the worker");
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
