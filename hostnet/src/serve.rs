//! Eén verbinding bedienen op een thread uit een vaste pool: [`serve_conn`].

use leanhttp::{Conn, Exchange, Outcome};

/// Bedient `conn` tot hij sluit: elk verzoek met [`leanhttp::serve`], en
/// zolang keep-alive mag het volgende op dezelfde thread
/// ([`leanhttp::Kept::serve`]).
///
/// Waarom een lus en geen park (zoals `applibhttp` op HopOS): op de host is
/// een verbinding een thread uit een vaste pool die blokkerend leest; die
/// thread hoort al bij deze ene verbinding. Tot leanhttp 3.2.0 zat deze lus
/// in `leanhttp::serve` zelf, met 60 s stilte tussen twee verzoeken; nu is
/// dat de koptermijn van het volgende verzoek (`REQUEST_TIMEOUT`, 15 s).
/// Elke server op de host kapt zijn leestermijnen af op 5 s (`READ_CAP`),
/// dus de stilte blijft wat ze was: 5 s.
///
/// Een overgenomen verbinding (`hijack`) gaat hier dicht: geen server op de
/// host neemt er een over.
///
/// # Errors
///
/// De fout van leanhttp (een termijn, een reset, een fout van de handler);
/// de verbinding is dan dicht.
pub async fn serve_conn<C, H>(conn: C, mut handler: H) -> leanhttp::Result
where
    C: Conn,
    H: AsyncFnMut(&mut Exchange<'_, C>) -> leanhttp::Result,
{
    let mut out = leanhttp::serve(conn, &mut handler).await?;
    loop {
        out = match out {
            Outcome::Kept(k) => k.serve(&mut handler).await?,
            Outcome::Closed => return Ok(()),
            Outcome::Hijacked(h) => {
                let mut conn = h.conn;
                let _ = leanhttp::close(&mut conn).await;
                return Ok(());
            }
        };
    }
}
