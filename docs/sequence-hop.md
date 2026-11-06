# Hop op HopOS: de volgorde en de afwegingen

Wat Hop als bewoner van HopOS doet, in de volgorde waarin het gebeurt, met
per stap waarom het zo is en of de volgorde verplicht is. Het doel is het
ontwerp na te lopen: is elke keuze logisch, en waar kan het simpeler. Dit is
deel 3 van drie: eerst HopOS (`hop-os/docs/sequence-hopos.md`, deel 1), dan
LEAN (deel 2), dan HOP (dit).

Stand: main op 2364cc7 (3.0.18, 24-10-2026), plus één ongecommitte
reparatie in de werkboom (F1, de F23 uit deel 1). Gelezen op 25-10-2026, uit
de code en niet uit de andere docs; waar die afwijken, staat het in §12.
Regelnummers verschuiven, dus de verwijzingen noemen pad en functie.

De kern-kant staat in deel 1 en wordt hier niet herhaald. Een verwijzing als
"deel 1 §5.4" wijst daarheen:

- §5: hoe de kern Hop plaatst, bewaakt, herstart en laat vallen;
- §7.2: een system-API-call, de toelating en het plafond per slot;
- §10: de kern-flip;
- §13: de regels R1-R4 en het voorstel voor de kern.

**Buiten beschouwing:** de Linux/docker-host (`agentd`, `hostnet`,
`runner/src/host`), de apps in `apps/`, `cli`, `OLD` en `archive`. De
gedeelde logica (`agent`, `leader`, `runner`, `store`, `types`, `api`,
`hop-http`) komt alleen voor waar ze op dit pad ligt. De TCP-stack, TLS en
HTTP (leannet, leantls, leanhttp) zijn deel 2.

**Hoe te lezen.** Elke sequence is een genummerde lijst in de echte
volgorde. Bij een stap staat:

- **Waarom**: de reden uit de code of de commit (met datum of hash). Staat
  er geen, dan zegt de stap dat: *geen reden gedocumenteerd*.
- **Verplicht**: de volgorde moet zo, met wat er misgaat als je hem omdraait.
- Een verwijzing als **[F3]** of **[S7]** wijst naar de lijst in §11
  (F = mogelijke fout, S = kandidaat om te schaven).

Zekerheid in §11: **H** = beide kanten in de code gelezen, **M** =
waarschijnlijk, **V** = vermoeden. "Zelf getoetst" betekent dat de bewering
na het lezen nog een keer apart tegen de code is gehouden.

## Inhoud

1. Het hele verloop op één pagina
2. Van eerste instructie tot `HOP_UP`
3. De eigenaar draait: de lus en de tik
4. De verbindingen met de kern
5. De staat bij de kern
6. Herstart onder de apps (`POST /restart`)
7. De kern-flip vanuit Hop
8. Stoppen en vallen
9. Een job van API-call tot draaiende app
10. Het clusterverloop
11. Te wegen: fouten en kandidaten
12. Docs en commentaar die achterlopen
13. Het voorstel: wachten, bellen, hard falen, LEAN

---

## 1. Het hele verloop op één pagina

```
kern   supervise: wait_uplink (≤10 s) ─ spec (env, token, cores) ─ put slot 1   [deel 1 §5.2]
applib rt::run: heap, klok, watch (hartslag + kill-vlag), [smp], spawn main
Hop    resident:
         HOP_BOOT ─ appnet::up ─ BootConfig ─ ClusterConfig
         spawn kloktaak (SNTP, dan SET_CLOCK; elk uur)
         spawn store-taak (NEXT_STORE, een lange wacht van 4 s, eindeloos)
         KernSys (de hoofdverbinding) ─ init_specs (env, of stat + lees /hop/init-jobs.json)
         downloader ─ spawn downloadtaak ─ tweede downloader (flip, herstart) ─ advertised
         Node::new  |  start_cluster (spawn lease, staat, link, dispatch) + Node::new_clustered
         restore (STATE_GET, adopteren) ─ veger ─ cluster_boot | init-jobs zaaien
         bind :P en :P+1000 ─ spawn acceptor + 4 werkers per poort ─ HOP_UP
         hoofdlus: bus ─ eigen regels ─ inbox ─ tik (1 s) ─ wake (bel, mail, brok, bus/tik)
einde    stop-bel: staat, terug          | herstart aangenomen: 202 uit, staat, terug
         fout bij de start: exit 3 (kern zweeg), exit 0 (return), of pending() (config)
```

Na `HOP_UP` gebeurt alles als taak op de executor van de app-core van Hop
(of op de OS-core, in de groep `system`). Er zijn ongeveer twintig taken: de
drie van applib (`watch`, `main`, de pomp van het net), de klok, de store, de
download, in een cluster vier taken voor het net, en per poort een acceptor
met vier werkers.

De vier dingen die overal terugkomen, en die in de stappen hieronder steeds
de reden zijn:

1. **Eén eigenaar per staat.** De `Node` (agent, leader, runner, de API's)
   leeft in één taak, de eigenaar. Wie iets wil, stuurt een bericht: de
   werkers via de `Hub`, de taken van de cluster via de `Inbox`, de
   downloadtaak via `ORDERS` en `PIECES` (handboek §1).
2. **De eigenaar wacht niet op het net.** Wat de lease-opslag, een andere node
   of een artifact-server raakt, gaat naar een taak die die verbinding
   bezit. Wat de kern raakt, wacht de eigenaar wel af (`.await` op de
   hoofdverbinding). Eén uitzondering: de download van een flipbundel of een
   nieuw Hop-image [F4].
3. **Bij de start hard, daarna verdragen.** Sinds 1c01b0d (24-10) stopt Hop
   met exit 3 als de kern bij de start niet antwoordt (restore, klok,
   init). Na de start verdraagt hij een kern die zwijgt: de laatste stand,
   een regel, de volgende tik opnieuw. Die tweedeling is het hoofdpunt van
   §13.
4. **Een ritme, geen gebeurtenis.** De tik van 1 s drijft bijna alles: de
   monitor, de logpomp, de verkiezing, de staat bij de kern. Waar een
   antwoord of een bel kan, is het een kandidaat in §11.

---

## 2. Van eerste instructie tot `HOP_UP`

### 2.1 Vóór `resident`: applib (`applib/src/rt.rs` `run`)

1. `App::new` uit de RAM-declaratie. Zonder staart parkeert de core: de kern
   ziet een core die nooit READY werd.
2. De heap tussen het image en de stack, de timebase, de event-stream,
   `announce`, de MMU-regel, `smp::init` en (met meer cores) `bring_up`.
3. Spawn `watch` (de hartslag op de control-page en de kill-vlag), dan spawn
   `main` = `resident(app)` gevolgd door `app.shutdown(0)`.
   - **Het gevolg:** een `return` uit `resident` is een exit met code 0. De
     kern behandelt elke exit die hij niet vroeg als een val
     (`hopos/src/slots.rs` `crashed`: `HOPOS_HOP_EXIT`, dan
     `HOPOS_HOP_DOWN`), ook code 0 (zelf getoetst) [S1].
   - Een paniek is exit 2, een fout van de kern `EXIT_KERNEL` = 3.

### 2.2 `resident`, stap voor stap (`agentd-hopos/src/main.rs`)

| # | Stap | Waarom hier | Verplicht? |
| --- | --- | --- | --- |
| 1 | `HOP_BOOT` | | |
| 2 | `appnet::up`; een fout is `HOP_NET_FAIL` en `return` (exit 0) | Zonder net geen API en geen kern | Vóór alles wat het net of de kern raakt |
| 3 | `BootConfig::from_env`; een fout is `HOPOS_API_NO_AUTH` en `pending()` | "Fail closed, zoals de Go-kern": een lege sleutel zet de HMAC-toets uit (ed9dd8f, 29-09) | Vóór elke API |
| 4 | De regels van `insecure` | | |
| 5 | `ClusterConfig::from_env`; een fout is `HOP_LOCK_BAD` en `pending()` | "Een node die stil standalone draait naast zijn cluster, is een tweede leader" (f67b7fe, 30-09) | Vóór de node |
| 6 | `HOP_MEMORY_DEFAULT`, `HOP_NO_CLOCK` | Een kern die `HOPOS_MEMORY` of de wandklok niet geeft; de huidige kern geeft ze altijd [S5] | |
| 7 | Spawn de **kloktaak**; een fout is `HOP_SNTP_FAIL` en door | SNTP is net (DNS en UDP), dus een eigen taak; de downloader vertrouwt `https` pas na SNTP | De volgorde tegenover de store-taak: *geen reden gedocumenteerd* |
| 8 | Spawn de **store-taak**; een fout is `HOP_STORE_KERNEL` en door | De store-calls van de apps (f67b7fe, 30-09) | |
| 9 | `KernSys::new` (de hoofdverbinding), dan **`init_specs`** over die verbinding (werkboom) | De init-jobs gaan geclusterd mee in `ClusterParts`, standalone pas na de restore; over de eigen verbinding, want een losse was de vierde [F1] | Vóór `start_cluster` |
| 10 | De downloader (`HttpImages`) met `entropy`: `HOP_TLS_ENTROPY_HW` of `_WEAK` | | Vóór de downloadtaak |
| 11 | `PIECES.split`, spawn de **downloadtaak**; een fout is `return` | Tot 3.0.7 hield een download de API 10 tot 30 s vast (3d9886a, 03-10) | |
| 12 | Een tweede `HttpImages` voor de flip en de herstart | "Zeldzaam, en die wacht op de kern zelf" [F4] [S6] | |
| 13 | `lock::advertised` (`HOPOS_ADVERTISE`); een fout is `HOP_LOCK_BAD` en `pending()` | *Geen reden gedocumenteerd* voor deze plek, ná drie spawns [F9] | Vóór de node (endpoint en lease-adres) |
| 14 | `Node::new` (registreert de eigen agent bij de eigen leader: `HOP_LEADER`), of `start_cluster` (spawn lease, staat, link, dispatch) en `Node::new_clustered` (`HOP_CLUSTER`) | Standalone leidt de node altijd; geclusterd beslist de verkiezing | Vóór de restore |
| 15 | De regels uit `HOP_LOG` naar de ring van `hop` | De ring bestaat pas met de node (d916924, 23-10) | |
| 16 | **`node.restore`**: `Kept::Unreachable` is `exit 3` | Wat nog draait, wordt van deze Hop, vóór de veger en vóór de eerste registratie bij een leader (3dca8f2, 22-10); "nooit leeg beginnen omdat de kern zweeg" (1c01b0d) | **Ja**: omgekeerd stopt de veger elke app [F2] [F7] |
| 17 | **`sweep_strays`**: `HOP_STRAY_STOPPED` | Twee benches in slot 3 en 4 die niemand kende, hielden elke plaatsing en flip tegen (Pi 4, 01-10, 196b1d3) | **Ja**: ná de restore [F6] |
| 18 | Geclusterd `cluster_boot` (de boot-claim, alleen met de klok); standalone `seed_init_jobs`, alleen bij een schone staat | "Nooit zaaien op een opslagfout" (Go) | Ná restore en veger |
| 19 | De `Hub`, `bind` van beide poorten (een fout is `HOP_LISTEN_FAIL` en "kost alleen die poort"), spawn acceptor en vier werkers per poort; een fout is `return` | "Zodra `HOP_UP` op het log staat, neemt de stack een SYN aan, ook als deze taak nog geen ronde had" | Binden vóór de spawn [F13] [S18] |
| 20 | `HOP_UP` | | |
| 21 | De hoofdlus (§3) | | |

Wat opvalt in deze volgorde:

- **Vier manieren van eindigen bij de start** [S1]:
  - `return` (exit 0): `HOP_NET_FAIL` en vier keer `HOP_SPAWN_FAIL`;
  - `app.shutdown(EXIT_KERNEL)` (exit 3): `HOP_KERNEL_GONE step=restore|clock|init`;
  - `pending()`, levend zonder API: `HOPOS_API_NO_AUTH` en twee keer `HOP_LOCK_BAD`;
  - luid en door: de spawn van klok of store, een poort die niet bindt, en
    de init-jobs (`HOP_INIT_FAIL`).
- **De configtoetsen staan verspreid.** Stap 3 en 5 komen vóór elke spawn,
  stap 13 erna. Met een slechte `HOPOS_ADVERTISE` zegt Hop "REFUSING to
  start", terwijl de kloktaak de klok zet en de store-taak de apps bedient
  (zelf getoetst) [F9] [S2].
- **De klok loopt naast de start.** Het zaaien (stap 18) en de eerste
  plaatsingen wachten er niet op. Een artifact over `https` faalt dan met
  "no wall clock" en herstart met back-off [F8].
- **`init_specs` staat sinds de werkboom vóór de restore.** Ook een start die
  geen schone staat vindt, doet nu de stat (en de lees) van
  `/hop/init-jobs.json`, en een kern die daar zwijgt, stopt Hop met
  `step=init` in plaats van `step=restore`. Op een board zonder schijf geeft
  de stat een weigering en dus elke start een `HOP_INIT_FAIL`-regel (M: de
  status van die weigering niet nagelopen).
- **De poorten binden pas na restore, veger en zaaien.** De canary van de
  kern dialt 10.100.0.2:8080 en geeft daar 15 s voor [F13].

### 2.3 De taken naast de eigenaar

| Taak | Bezit | Wacht op | Verbinding met de kern | Bij een fout |
| --- | --- | --- | --- | --- |
| `watch` (applib) | de hartslag, de kill-vlag | zijn ritme | geen (control-page) | |
| kloktaak | `UdpLink`, per sync een eigen `KernSys` | `NTP_EVERY` (3600 s) of `NTP_RETRY` (300 s) | alleen tijdens `SET_CLOCK` (werkboom) | SNTP-fout luid (drie keer, dan één op twaalf); een latere `SET_CLOCK` zonder antwoord luid (`Later`); een weigering, of geen antwoord vóór de eerste sync: exit 3 (`Fatal`) |
| store-taak | `objstore::Service` en de bucket | `NEXT_STORE` (lange wacht, `POLL_MS` 4 s) | blijvend | 1 s wachten en opnieuw, eindeloos; luid drie keer, dan één per zestig [F11] |
| downloadtaak | `HttpImages` | `ORDERS` | geen | `Piece::End(Err)` naar de eigenaar |
| lease, staat, link, dispatch (cluster) | elk een eigen `Client` | hun rij | geen | een antwoord als `Mail`; de fout van de opslag één keer per wissel |
| acceptor (per poort) | de `TcpListener` | `accept` | geen | een regel, 100 ms, opnieuw |
| werker (vier per poort) | een verbinding, een eigen `Client` voor doorgiftes | `Handoff::take` | geen | de verbinding dicht |
| eigenaar (`resident`) | de `Node`, de hoofdverbinding | `wake` | blijvend | §3 |

---

## 3. De eigenaar draait: de lus en de tik

### 3.1 Eén ronde van de hoofdlus (`resident`)

1. **De bus leeg** (`hub.next`), in volgorde:
   - `Question::Http`: `node.handle_routed` (wacht op de kern waar nodig),
     dan `hub.answer`. Nam de kern net een herstart aan, dan eerst
     `LEAVING`, dan het antwoord, dan `leave` en terug (§6).
   - `Question::Poll` (een open stroom): `node.poll`.
   - `Question::StreamDone`: de teller af.
   - **Waarom eerst de bus, level-triggered:** wat binnenkwam terwijl de
     eigenaar op de kern wachtte, ligt er nog.
2. **De regels van de andere taken** uit `HOP_LOG` naar de ring van `hop`.
3. **De inbox** van de cluster: `node.on_mail`.
4. **De tik**, als hij aan de beurt is (§3.2).
5. **`wake`**: de stop-bel, dan een `Mail`, dan een brok van de download,
   dan de bus of de tik.
   - **Waarom de bel eerst:** "wat na hem nog binnenkomt, kent de volgende
     Hop niet".
   - **Waarom een brok vóór de bus:** zo krijgen ze om de beurt een ronde, en
     houdt een drukke API een download niet tegen (03-10).
6. Een brok: `node.on_piece` (een call naar de kern). De bel:
   `node.on_stop` en terug.

### 3.2 De tik (`Node::tick`, `agentd-hopos/src/node.rs`)

1. **`measure_system`:** om de `monitor_interval` (standaard 5 s)
   `SLOT_STATUS` van slot 0 en van het eigen slot, voor de telemetrie.
2. **Leidt deze node:** de heartbeat van de eigen agent bij de eigen leader.
   - **Waarom:** anders verklaart de eigen leader hem na 30 s dood.
3. **Elke `LEADER_TICK` (10 s), als hij leidt:** de takenlijsten van de
   agents op andere nodes opnieuw vragen, dan `leader.tick` (dode agents,
   settle, om de derde tik de vangnet-reconcile).
4. **Een koude flip die niet sprong:** na `COLD_FLIP_WAIT` `cold_flip_back`
   (§7.2).
5. **`agent.tick`**: de monitor (een `Poll` per draaiende taak, om de 5 s) en
   de herstarts die aan de beurt zijn, dan `run_actions`.
6. **`drain`**: de acties die daaruit volgen, hoogstens 16 rondes
   (`ACTION_ROUNDS`) [S13].
7. **`pump_logs`**: per gearmde kooi `NEXT_LOG` tot er niets meer is.
8. **`pump_kern_logs`**: hoogstens 256 regels van slot 0; na een weigering
   pas na 60 s opnieuw [S5].
9. **`cluster_tick`**: `join` (zolang de klok er niet is), `elector.tick`
   (de renew op een derde van de lease), elke 10 s `election.tick`, en de
   snapshot van de leader naar de staat-taak.
10. `collect_events`, dan **`keep_state`**: de staat bij de kern als hij
    veranderde of als `STATE_REFRESH` (10 s) verstreek [S4].

De volgorde binnen de tik: *geen reden gedocumenteerd*, behalve voor de
laatste stap ("na elke tik waarin hij veranderde: de tik is de debounce").

### 3.3 Wat de eigenaar de kern vraagt

Alles over de ene hoofdverbinding, na elkaar, met de eigenaar als wachter.
Met N draaiende apps:

| Wat | Hoe vaak | Calls |
| --- | --- | --- |
| `NEXT_LOG` per app, tot leeg | elke tik | N + het aantal regels |
| `NEXT_LOG` slot 0 | elke tik | 1 + het aantal regels (≤256) |
| `SLOT_STATUS` per taak: de poll, en daarna nog een keer voor het gebruik | om de 5 s | 2N |
| `SLOT_STATUS` slot 0 en het eigen slot | om de 5 s | 2 |
| `STATE_PUT` | bij een wijziging, en elke 10 s | ≥0,1 per s |

Dat is ruim N + 1 calls per seconde zonder dat er iets gebeurt. Elke call
is een rondreis over het slot-LAN. De poll en het gebruik vragen dezelfde
`SLOT_STATUS` twee keer: `Runner::status` en daarna `record_usage`, dat
`HopRunner::usage` vraagt (zelf getoetst) [S7].

### 3.4 Te wegen

- **De eigenaar wacht wel op het net bij de flip en de herstart**: de
  download van de bundel of het image loopt in `handle_routed` [F4].
- **Pollen waar een antwoord kan:** de logpomp, de dubbele status, en de
  stromen die elke 0,5 s vragen wat er bij kwam (`hop_http::STREAM_POLL`)
  [S7].
- **`ACTION_ROUNDS`** begrenst een keten die niet convergeert ("de volgende
  tik pakt de rest op"). Een keten die niet stopt, is een fout in de agent
  [S13].

---

## 4. De verbindingen met de kern

### 4.1 Het plafond

De kern laat een app twee system-verbindingen per levensduur toe
(`MAX_SYSTEM_CONNS`): "applib houdt er één open; de tweede is voor een
herverbinding waarvan HOP de FIN van de oude nog niet zag". Hop krijgt er
drie (`MAX_HOP_CONNS`): "één meer dan een app, voor de store-taak". Gemeten
op 30-09: met twee kreeg de kloktaak geen verbinding meer (deel 1 §7.2,
`kern/src/system.rs` `admit`).

| Verbinding | Houder | Hoe lang |
| --- | --- | --- |
| hoofd | de eigenaar: init, restore, adopteren, vegen, plaatsen, stromen, status, logs, staat, flip, herstart | blijvend (elke tik een call, dus nooit stil) |
| store | de store-taak | blijvend (de lange wacht op `NEXT_STORE`) |
| klok | de kloktaak | sinds de werkboom alleen rond `SET_CLOCK`; verbonden pas als SNTP een meting gaf (de client verbindt lui) |

### 4.2 F1: de vierde bij de start (gerepareerd in de werkboom)

Tot de werkboom opende Hop er bij de start vier:

1. de hoofdverbinding;
2. de store-taak;
3. de kloktaak, die haar verbinding het hele uur tot de volgende sync
   openhield;
4. een losse verbinding in `init_specs`, voor één stat van
   `/hop/init-jobs.json`.

De vierde werd geweigerd. Sinds 1c01b0d was dat fataal
(`HOP_KERNEL_GONE step=init`, exit 3). In deel 1 maakte dat de store-ring
rood (F23).

De reparatie (ongecommit):

- `init_specs` krijgt `&mut SystemClient` en loopt over `sys.conn_mut()`,
  vooraan in de start (stap 9).
- De kloktaak doet `drop(sys)` vóór de wacht.

Nagelopen of Hop elders meer dan drie kan houden (zelf getoetst: er zijn
precies drie plekken die `net.system_client()` aanroepen, alle drie in
`main.rs`):

- **Herstart:** de nieuwe Hop telt vers. De teller hoort bij een levensduur
  en de generatie staat vóór het startschot (fb81b13, deel 1 §5.4).
- **Flip, herstart-image, download, restore, adopteren, vegen:** allemaal over
  de hoofdverbinding. De downloadtaak heeft geen verbinding met de kern; de
  eigenaar stroomt de brokken zelf.
- **Na een warme flip:** de oude verbindingen zijn weg met de oude
  node-stack, en de adoptie geeft een nieuwe generatie, dus een nieuwe telling.

De piek is dus precies drie: hoofd en store vanaf de eerste `.await`, en de
klok een rondreis van SNTP later.

### 4.3 Geen speling [F12]

Een app heeft één verbinding en één plaats voor de herverbinding. Hop heeft
er twee blijvend en één voor de klok, en dus geen plaats voor een
herverbinding:

1. applib herhaalt een call na een transportfout één keer over een verse
   verbinding (`applib/src/sys.rs` `call_into`; deel 1 §13.9 vraag 9);
2. de oude verbinding is dan al gedropt, maar de kern geeft haar plaats pas
   terug als zijn werker het sluiten ziet;
3. na een `Timeout` (een call van meer dan 10 s) is die werker nog bezig met
   het oude frame;
4. komt de klok op dat moment voor `SET_CLOCK`, dan is zij de vierde en
   geweigerd. Is het haar eerste `SET_CLOCK`, dan is dat `Fatal`.

De kans is klein, maar de regel van de kern ("de tweede is voor een
herverbinding") geldt voor Hop niet meer. Er is geen toets die de
verbindingen telt: de nep-kern (`hopos-runner/src/fake.rs`) telt niet [S17].

---

## 5. De staat bij de kern

### 5.1 Schrijven

Het blob is `agent::Handoff` versie 2: JSON met de jobs, de taken met hun
kooi in `pid`, en standalone de snapshot van de leader als tekst. Hoogstens
`HOP_STATE_MAX` (1 MiB). `STATE_PUT` vervangt het hele blob
(`hopos-runner/src/state.rs`).

- **Waarom in de kern en niet op hopfs:** het blob sterft met de boot. Tot
  7726782 (02-10) stond het in `/hop/agent-state.json`, en een taak van een
  vorige boot werd een spook op het slot van een nieuwe bewoner (de M4: twee
  minuten 503 "port 80 is taken").

Hop schrijft het op vier momenten:

1. `keep_state` na elke tik met een wijziging (de tik is de debounce);
2. `keep_state` elke `STATE_REFRESH` (10 s), ook zonder wijziging. **Waarom:**
   na een warme flip heeft de nieuwe kern niets, en "de system-client
   verbindt stil opnieuw", dus Hop ziet de wissel niet (3dca8f2, 22-10);
3. `Node::restart`, vlak vóór `HOP_RESTART`. Mislukt dat, dan geen herstart;
4. `leave` na de 202, en `on_stop` bij de stop-bel.

**Wie leest het?** Alleen een nieuwe Hop in `restore`. Een nieuwe Hop met een
blob komt er alleen na `HOP_RESTART`: de kern herstart een gevallen Hop niet
(deel 1 §5.5), en een warme flip start geen nieuwe Hop. `HOP_RESTART` komt
alleen uit `Node::restart`, en die schrijft het blob vlak ervoor en weigert
als dat mislukt; `leave` schrijft het daarna nog een keer. Wat stap 1 en 2
schrijven, wordt dus altijd overschreven vóór iemand het leest (zelf
getoetst) [S4]. De regel in `STATE_REFRESH` ("een herstart binnen tien
seconden na een flip is de enige die dan nog een lege kern treft") klopt
daarom niet: die herstart schrijft het blob zelf eerst (§12).

### 5.2 Lezen: `Node::restore`

1. `STATE_GET` met een buffer van `HOP_STATE_MAX`, eenmalig.
2. De uitkomst als `Kept`:

   | Antwoord | `Kept` | Wat de start daarna doet |
   | --- | --- | --- |
   | niets (`NotFound` of maat 0) | `Clean` | veger, init-jobs |
   | een foutstatus (een kern zonder de ops) | `Unsupported` | veger, init-jobs [S5] |
   | transport, termijn, protocol | `Unreachable` | exit 3 (1c01b0d) |
   | `TooBig`, `OutOfMemory`, of een blob dat `Handoff::parse` weigert | `Unreadable` | **veger, geen init-jobs** [F2] |
   | een blob | `Adopted(n)` | veger |

3. **`adopt`:**
   1. per taak in het blob met een kooi ≠ 0 en ≠ het eigen slot:
      `SLOT_STATUS`. Alleen `Running` en nog van niemand wordt overgenomen
      (`HopRunner::adopt_running`);
   2. `agent.adopt` met de jobs en de overgenomen taken;
   3. standalone: `load_committed_state` van de leader en
      `register_with_placed`. De leader telt de overgenomen taken als
      geplaatst en plaatst ze niet nog eens;
   4. `HOP_ADOPTED n=`, en `HOP_ADOPT_SKIPPED n=` voor wat niet meer draait;
   5. `drain`. De Start-acties daaruit claimen nog geen slot: de runner zet
      ze in de rij, en het slot komt pas bij de eerste brok (§9). De veger
      gaat dus nog vóór elke nieuwe kooi.
   - **Verplicht:** adopteren vóór de veger. Wat de vorige Hop neerlegde en
     nog draait, is van deze; de rest is van niemand.

### 5.3 Te wegen

- **`Unreadable` begint leeg, en dan stopt de veger elke app** (zelf
  getoetst) [F2]. De doc van `Unreachable` noemt precies dat als reden om
  hard te stoppen ("leeg beginnen zou de veger elke app laten stoppen"). Bij
  `Unreadable` gebeurt het toch. Standalone verdwijnen ook de jobs (de
  leader zat in hetzelfde blob), en de init-jobs komen niet terug (de staat
  is niet schoon). De aannemelijke trigger is een herstart naar een Hop met
  een andere `HANDOFF_VERSION`, of met een veld dat de nieuwe
  `Job::from_value` of `Task::from_value` weigert.
- **Adopteren leunt op een `SLOT_STATUS` die nooit faalt** [F7].
  `KernSys::slot_status` geeft zonder antwoord de laatste stand, of een leeg
  slot als er nooit een stand was. Een verse Hop heeft nooit een stand. Een
  call die bij de overname niet antwoordt, laat de taak dus niet overnemen.
  De veger ziet hetzelfde lege slot en laat hem staan. De leader plaatst hem
  opnieuw, naast de oude.
- **`restore_entries` laat stil vallen** wat boven `MAX_JOBS` of `MAX_TASKS`
  uitkomt (`agent/src/node.rs`). De runner heeft de kooi dan al
  overgenomen, dus de veger stopt hem niet en geen taak kent hem (V: de
  grenzen zijn ruim).

---

## 6. Herstart onder de apps (`POST /restart`)

De kern-kant is deel 1 §5.4. Hier de kant van Hop (`agentd-hopos/src/restart.rs`,
`node.rs` `restart`, `main.rs` `leave`).

1. **`POST /restart` op de agent-poort**, achter de HMAC van `/flip`
   (`api/src/node.rs` `restart`):
   - zonder body hetzelfde image;
   - met `{"url","sha256"}` een nieuw image. De URL moet absoluut zijn en de
     som 64 hex-tekens.
   - De 202 staat klaar, met `Effect::Restart`.
2. **`Node::restart`**, in de eigenaar: `HOP_RESTART`. Met een URL eerst
   `restart::fetch` = `flip::fetch_raw` met `HOP_IMAGE_JOB`: `START_SLOT`
   rauw, dan de download, brok voor brok met `STREAM_IMAGE` [F4].
3. **`save_state`.** Mislukt dat, dan wordt het rauwe slot gestopt en is het
   antwoord 502.
   - **Waarom:** "zonder staat geen herstart: de nieuwe Hop zou elke app als
     bewoner van niemand stoppen".
4. **`restart::ask`**: `HOP_RESTART` met het slot (0 is hetzelfde image), de
   som en een termijn van 0 (de standaard van de kern, 3 s), met een
   call-termijn van 20 s. Een weigering stopt het slot, en het antwoord is
   502 (`HOP_RESTART_FAIL`).
5. **Aangenomen:** `leaving`, `HOP_RESTART_ACCEPTED`. De hoofdlus zet
   `LEAVING` en geeft dan pas het antwoord.
   - **Verplicht:** de vlag vóór het antwoord. De werker leest hem zodra hij
     het antwoord heeft, en sluit na de 202 (`Connection: close`).
6. **`leave`** → `restart::depart`: wat het eerst komt, in deze rangorde:
   1. de stop-bel;
   2. de werker van de 202 is vrij (`Handoff::freed`);
   3. `REPLY_GRACE` (1 s) is om.
   - **Waarom:** tot 22-10 stopte de kern Hop een vaste 500 ms na zijn OK, en
     het antwoord kwam nooit aan (curl HTTP 000).
   - `REPLY_GRACE` is hoogstens de helft van de termijn van de kern (een
     `const`-assert).
7. **`node.leave`**: `HOP_RESTART_LEAVING why=replied|grace|bell`, de staat
   een laatste keer, `HOP_STOPPING`.
8. **Terug uit `resident`**: applib doet `net.shutdown` (200 ms) en exit 0.
   De kern wacht hierop (`wait_leave`), stopt de kooi, geeft de stromen
   terug en plaatst de nieuwe Hop (deel 1 §5.4).
9. **De nieuwe Hop** loopt §2 vanaf stap 1. `restore` geeft `Adopted(n)`.
   "Als we binnen 30 s herstarten en onze staat terugzetten, is er niets
   gebeurd": de lease leeft 30 s en dezelfde node pakt hem meteen terug
   (`Discovery::plan_claim`).

Te wegen:

- **Geclusterd doet de nieuwe Hop pas mee na zijn eigen SNTP** [F3].
  `CLOCK_SYNCED` is een static van het nieuwe proces en begint op `false`.
  `join` wacht erop, ook al zette de vorige Hop de klok van de kern al. Lukt
  de eerste SNTP niet, dan komt de volgende pas na `NTP_RETRY` (300 s). In
  die tijd:
  - geen renew, dus de lease is na 30 s weg;
  - geen heartbeat, dus de leader verklaart de agent na 30 s dood en plaatst
    de taken elders;
  - bij de terugkeer stopt `trim_returning_agent_surplus` de taken hier.

  De belofte van de moduledoc (binnen 30 s is er niets gebeurd) geldt dus
  alleen als SNTP meteen lukt.
- **De nieuwe Hop moet binnen 15 s binden** [F13]. De canary van de kern
  (`hopos/src/watchdog.rs`) dialt 8080 en houdt een geslaagde dial 15 s vers.
  De nieuwe Hop bindt pas na `init_specs`, restore, adopteren, vegen (elke
  stray tot 3 s) en zaaien.
- **De download van een nieuw image loopt in de eigenaar** [F4].

---

## 7. De kern-flip vanuit Hop

De kern-kant is deel 1 §10. Hier de kant van Hop (`agentd-hopos/src/flip.rs`,
`node.rs` `flip`).

### 7.1 Warm

1. **`POST /flip`** (`api/src/node.rs`):
   - de URL moet absoluut zijn en de som 64 hex-tekens;
   - `cold` moet een bool zijn. Iets anders is een 400, "geen stille warme
     flip".
2. **`Node::flip`**: `HOP_FLIP`, dan `flip::fetch`: een rauw slot
   (`FLIP_BUNDLE_JOB`) en de download, in de eigenaar [F4].
3. **`flip::ask`**: `PrivOp::Flip` met het slot en de som, met een
   call-termijn van 20 s.
   - Een weigering: het slot wordt gestopt, 502, `HOP_FLIP_FAIL`.
   - Aangenomen: `HOP_FLIP_ACCEPTED` en de 202.
4. **Hop draait door.** De kern springt een halve seconde later (deel 1
   §10.2). Hop op de OS-core staat stil tot kern B hem terugzet; op een
   app-core draait hij door.
- **Verplicht:** de 202 pas na het ja van de kern. Daarna is er niets meer
  om naar te kijken (de moduledoc).

### 7.2 Koud

1. **`cold_refusal`**: het board-contract uit `HOPOS_COLD_FLIP`.
   - `no` (geen PSCI, de M4) is een 502 zonder download.
   - `fresh` (de Pi 5) is een 502 als er sinds de start iets op een app-core
     draaide.
   - **Waarom:** "Wat Hop al weet, vraagt hij niet". De kern weigert pas ná
     de stops die Hop hieronder doet.
2. **`flip::fetch`.**
   - **Verplicht:** de download vóór de stops. "Een URL die niet werkt, laat
     alles draaien."
3. **`agent.hold_for_flip`**: een Stop per taak, maar het record blijft
   `Stopping` met zijn reservering, zodat de telling van de leader klopt als
   de kern toch niet springt (2180acc, 03-10). Dan `drain`: elke stop is een
   `STOP_SLOT` van hoogstens 3 s, in de eigenaar. `HOP_FLIP_COLD_STOP`.
4. **`flip::ask(cold)`.**
   - Een weigering: `cold_flip_back`.
   - Aangenomen: `cold_flip_until` = nu + `COLD_FLIP_WAIT` (30 s).
5. **De tik:** leeft deze Hop na 30 s nog, dan "sprong de kern niet":
   `cold_flip_back` (`HOP_FLIP_COLD_BACK`), en de vastgehouden taken
   herstarten. **Waarom:** de Pi 5 nam op 03-10 een koude flip aan en sprong
   niet.
   - Staat Hop op een app-core, dan stopt de kern hem zelf vóór de sprong
     (`stop_residents`, §8.1). De detector werkt dus alleen voor een Hop op
     de OS-core.

Deel 1 §13.7 stelt voor dat er na de 202 geen weg terug is: óf de sprong, óf
`fatal`. Dan valt deze tak aan Hop-kant weg [S10].

### 7.3 Na de warme sprong: het gat

Hop ziet de wissel niet. Zijn verbindingen met de kern zijn weg met de oude
node-stack. Kern B haalt eerst een DHCP-lease en zet dan pas de stack en de
system-listener op (deel 1 §3.3, `hopos/src/net.rs` `Node::run`). Vóór de
lease gooit de DHCP-lus elk frame van een slot weg (deel 1 F7). Hop op de
OS-core krijgt zijn beurten al na `setup`, dus vóór die lease. Op een app-core
draait hij gewoon door (M: het moment van de eerste beurt niet nagemeten).

Elke call in dat gat kost de eigenaar een dial van 3 s (`SYS_DIAL_TIMEOUT`),
en door de herhaling van applib nog een. Wat Hop dan doet:

| Call | In het gat |
| --- | --- |
| `SLOT_STATUS` (poll, gebruik, meting) | de laatste stand; geen fout [S9] |
| `NEXT_LOG` (apps) | `None`, stil |
| `NEXT_LOG` slot 0 | `Ok(None)`, stil (alleen een foutstatus telt als weigering) |
| `STATE_PUT` | `HOP_KERNEL_STATE_FAIL`, drie keer luid, de volgende tik opnieuw |
| `NEXT_STORE` (store-taak) | `HOP_STORE_KERNEL`, elke seconde opnieuw |
| `SET_CLOCK` | `Later` als de klok in deze Hop al eens stond; anders **`Fatal`** |

Hop komt dus door het gat dankzij de terugvallen die bij de start sinds
1c01b0d verboden zijn [F10]. `docs/flip.md` in hop-os zegt dat Hop zijn
staat opnieuw "hoort te schrijven zodra zijn system-client de nieuwe kern
bereikt". Hop doet het op het ritme van 10 s, en volgens §5.1 heeft die
schrijf geen lezer [S4].

---

## 8. Stoppen en vallen

### 8.1 De stop-bel

`wake` geeft `Woke::Stop`: `on_stop` schrijft de staat (`HOP_STOPPING`), en
Hop keert terug. De kern luidt de bel van Hop op twee plekken:

- na een aangenomen herstart, als vangnet (`BELL_STOP`, deel 1 §5.4);
- bij een koude flip, als Hop op een app-core staat: `stop_residents` stopt
  elke bewoner buiten core 0 (hop-os `hopos/src/flip.rs`, deel 1 §10.3). De
  staat die `on_stop` dan schrijft, sterft met de sprong.

Zit de eigenaar in een lange call (een flipdownload), dan hoort hij de bel
pas daarna [F4].

### 8.2 Hoe Hop eindigt

| Situatie | Hoe | Code | De kern | De watchdog |
| --- | --- | --- | --- | --- |
| Herstart aangenomen, of de bel | terug uit `resident` | 0 | verwacht, de nieuwe Hop | |
| `HOP_NET_FAIL`, `HOP_SPAWN_FAIL` (×4) | `return` | 0 | `HOPOS_HOP_EXIT`, `HOPOS_HOP_DOWN` | fase 2: reset na 15 s; fase 1: blind (deel 1 F5) |
| `HOP_KERNEL_GONE step=restore\|clock\|init` | `shutdown(EXIT_KERNEL)` | 3 | idem | idem |
| een paniek | de paniekhaak van applib | 2 | `HOPOS_HOP_EXIT` | idem |
| `HOPOS_API_NO_AUTH`, `HOP_LOCK_BAD` (×2) | `pending()` | geen | ziet niets: de hartslag loopt door | de canary op 8080 faalt: fase 2 reset na 15 s, fase 1 blind |
| spawn van klok of store, `HOP_LISTEN_FAIL`, `HOP_INIT_FAIL` | een regel en door | geen | ziet niets | ziet niets, behalve zonder agent-poort (de canary) |

- **`pending()` botst met hard falen.** Een configfout is een node die niets
  doet, maar de hartslag slaat. In fase 1 (de eerste boot) aait de watchdog
  dan blind en voorgoed. In fase 2 (na een herstart) volgt een reset, en de
  koude boot met dezelfde config komt in fase 1 en blijft daar. Het eindigt
  dus hetzelfde als een exit, maar via een andere weg [S1].
- **De code 0 of 3 maakt voor de kern geen verschil.** Alleen de regel
  verschilt. Het commentaar bij `EXIT_KERNEL` zegt "geen 0, dus de kern laat
  hem liggen", maar de kern laat elke ongevraagde exit liggen (§12).
- **Een falende Hop is een falende node** (deel 1 §5.5, Derek 23-10). Aan de
  Hop-kant geldt dat tot nu toe alleen bij de start, en alleen voor de
  kern-calls van 1c01b0d.

---

## 9. Een job van API-call tot draaiende app

1. **`POST /v1/jobs`** op de leader-poort (P + 1000). Op de agent-poort
   wordt het `Effect::Proxy`: standalone in-proces naar de eigen leader,
   geclusterd een doorgifte die de werker zelf uitvoert (`forward::serve`).
2. **De werker:** leanhttp leest het verzoek (`READ_CAP` 2 s stilte), dan
   `hub.ask_routed` naar de eigenaar. Mag de verbinding na het antwoord
   blijven (keep-alive), dan gaat ze terug naar het park van de acceptor
   (`Park`, sinds 06-11-2026; hoogstens `PARK_IDLE` 2 s stil), niet naar
   de werker: een stille client houdt geen werker vast.
3. **De eigenaar:** `leader_reply`. Leidt hij niet, dan een 503. Anders
   `LeaderApi::handle` met `LeaderCluster` over `Local`: de leader slaat de
   job op, reconcilet en vraagt per plaatsing `Transport::run`:
   - **de eigen agent:** `agent.run`, met de toelating tegen cores, geheugen
     en `pool_largest`, dan een taak `Queued` en een `Action::Start`;
   - **een agent op een andere node:** `Relay::run` antwoordt "aangenomen"
     uit de boeken en zet een `POST /run` in de rij van de dispatch-taak
     (§10.3).
4. **Het antwoord** naar de werker, dan `drain`: `Node::start` →
   `HopRunner::start`.
   - De spec: geen container, precies één artifact, rauw (geen `extract`).
     1024 shares is één core; met een sharegroup één core in een pool.
   - `Started::AwaitImage`, en een `Loading` in de rij `loads`.
   - **Waarom nog geen slot:** "het slot kiest de kern pas bij `image_begin`:
     hij kent de bewoners die een kern-flip overleefden".
5. **`flush`:** `take_order` geeft de voorste download (één tegelijk, "zodat
   de starts hun image in volgorde krijgen") in `ORDERS`, en zet `WANTED`.
6. **De downloadtaak:** `HttpImages::fetch`: opzoeken, TCP, TLS (met de
   vertrouwde klok, anders `NoClock` [F8]), redirects. Dan
   `Piece::Begin(size)` (zonder `Content-Length` geen start), de brokken
   (64 KiB), en het einde, door `PIECES` (twee plaatsen).
7. **De eigenaar, `Begin`:** `image_begin` → `START_SLOT` (de claim, deel 1
   §6.1). De taak is nu `Streaming`.
8. **De eigenaar, per brok:** `image_chunk` → `STREAM_IMAGE` in happen van
   `MAX_CHUNK` (deel 1 §6.2).
   - **Verplicht:** `Placed` alleen op de laatste byte. Meer bytes dan de
     lengte, of een kern die anders telt, is een stop en een fout, "nooit een
     halve app".
   - Bij `Placed`: `Armed`, en een `SLOT_STATUS` voor de kooiregel in de log
     van de taak.
9. **`End(Ok)`:** `finish_load` → `report_start` → `agent.on_started`:
   `Running`, `HOP_JOB_PLACED slot=N`, en een `Notify` (Started) naar de
   leader en `/v1/events`.
10. **Daarna:** om de 5 s een `Poll` (`SLOT_STATUS`) en het gebruik (nog een
    `SLOT_STATUS`), en elke tik de logpomp.

De foutpaden:

- **`NoCapacity`:** `release_unplaceable`. De taak gaat terug naar de leader
  (Unplaceable, `mark_unplaced`). Herstarten zou elke keer het image halen en
  weer falen (gemeten 26-07).
- **`Failed`:** `schedule_restart`, met een back-off van 1, 2, 4, 8, 16 en
  dan 30 s. De standaard is onbeperkt (`DEFAULT_MAX_RESTARTS` = -1). Dat is
  het herstartbeleid van een app, geen truukje van Hop.
- **Een stop:** `Action::Stop` → `STOP_SLOT` met 3 s
  (`HOP_STOP_TIMEOUT_MS`). Bevestigt de kern niet, dan blijft de kooi van
  Hop (`Quarantined`).

Te wegen:

- **Eén download tegelijk**, terwijl de runner er vier toelaat
  (`MAX_CONCURRENT_DOWNLOADS`) en de kern zegt "Hop begrenst zelf op vier"
  (`kern/src/system.rs` `MAX_STREAMS`). Op HopOS zijn de grens van vier en
  `Error::Busy` dood (zelf getoetst: de node vraagt `image_begin` alleen
  voor de voorste download) [S11].
- **Health probes** zijn op HopOS niet bedraad. Er komt één regel
  (`HOP_PROBE_SKIPPED`), en een job met een `health_check` meldt `Start` in
  plaats van `Started` [S19].

---

## 10. Het clusterverloop

Zonder lock is de node een standalone leader. Met `HOPOS_LOCK_URL`, of
`HOPOS_LOCK_TYPE=s3`, doet hij wat de daemon doet (`node/cluster.rs`,
`lease.rs`, `link.rs`, `relay.rs`, `agent/src/election.rs`, `elector.rs`).

### 10.1 De start geclusterd

1. `start_cluster` spawnt vier taken: lease, staat, link en dispatch. Elke
   taak heeft een eigen `Client` met `CLUSTER_IDLE` (20 s).
   - **Waarom eigen taken:** de eigenaar wacht nooit op het net. De link-taak
     is apart, want een register laat de leader reconcilen, en die kan deze
     node om een `/run` vragen: "wachtte de eigenaar zelf op het antwoord,
     dan wachtte hij op zichzelf".
2. `Node::new_clustered`: de leader-helft dicht, geen leader-adres.
3. Restore (zonder het deel van de leader) en de veger.
4. **`cluster_boot` → `join`:** alleen met een gezette klok. Zonder klok één
   regel `HOP_CLUSTER_NO_CLOCK`, en elke tik opnieuw kijken.
   - **Waarom:** een lease is een tijd op de wandklok van de schrijver. De
     kern zet de klok vast op 29-09-2026 tot SNTP lukt
     (`HOPOS_CLOCK_FIXED`).
5. **De boot-claim:** `try_become_leader` → `LeaseOp::Claim`. Is hij raak,
   dan `become_leader_now`, zonder de drempel van vier tikken.
6. **Leider worden in twee stappen:**
   1. `start_leading` → `StateOp::Load` (`HOP_LEADER_LOADING`); tot het
      antwoord geeft de leader-API een 503;
   2. `finish_leading`: een nieuwe `Leader` met de geladen staat, `live`,
      `register_own`, en op een schone staat de init-jobs.

### 10.2 Een volger

1. Elke `ELECTION_TICK` (10 s): `get_leader` uit de laatste lees van de
   elector. Is die lees ouder dan een lease, dan eerst een `LeaseOp::Read`.
2. `Register` of `Heartbeat` naar de link-taak: een ondertekende `POST` met
   `LINK_TIMEOUT` (5 s). Het antwoord komt als `Mail::Link` terug in
   `election.on_reply`.
3. **De faaltellers:**
   - na 4 mislukte tikken (`TAKEOVER_AFTER`) probeert hij de lease te
     claimen;
   - na 7 (`ISOLATED_AFTER`) stopt hij zijn taken, maar alleen als de opslag
     een levende leader meldt (wij zijn de geïsoleerde) of zelf zwijgt. Zegt
     de opslag "geen leader", dan blijven de taken (traqqr, 08-09: na 70 s
     doodden beide agents elke taak).

### 10.3 De leider

1. **De lease:** `elector.tick` stuurt op een derde van de TTL (10 s) een
   `Renew`; het antwoord verlengt `expires_at`. "Nog steeds van mij" is een
   vergelijking met de klok.
   - **Waarom zo:** op Bunny duurde een PUT 4 tot 20 s (08-09). Een
     synchrone renew hield de heartbeat van de leader zelf voorbij de
     dood-drempel.
2. **De heartbeats van de agents** komen binnen over de API. De eigen agent
   slaat elke tik in-proces.
3. **`LEADER_TICK` (10 s):** dode agents (na 30 s), en de vangnet-reconcile
   om de derde tik.
4. **De snapshot:** na een mutatie `PERSIST_DEBOUNCE` (1 s), dan
   `StateOp::Save` naar de staat-taak. Die stuurt alleen de laatste: oudere
   in de rij gaan niet meer over de draad.
5. **De relay naar agents op andere nodes:**
   - `run` antwoordt "aangenomen", tenzij een verse weigering geldt
     (`REFUSAL_TTL` 30 s), en zet de `POST /run` in de rij;
   - een weigering of geen antwoord: `mark_unplaced`, en de reconcile kiest
     een andere agent;
   - stop, stop-task en delete gaan in de rij en zijn meteen "gedaan". "Een
     stop die niet aankomt, ziet de leader bij de volgende registratie";
   - de takenlijsten: elke leader-tik opnieuw (`GET /tasks`).
6. **De dispatch-taak** voert de aanroepen na elkaar uit: `run` en `tasks`
   met 5 s, stoppen en verwijderen met 60 s [S16].

### 10.4 Aftreden en failover

1. **Verdrongen** (de renew zegt `displaced`): `StopLeader` eerst (de
   leader-helft leeg, de dispatch-rij leeg), dan de lease los. Dat is de
   volgorde van `Election::step_down`.
2. **De leider valt weg:**
   1. zijn lease verloopt na 30 s;
   2. de volgers falen hun heartbeat, en na 4 tikken claimt er een;
   3. `plan_claim` met de verlopen lease;
   4. de nieuwe leider laadt de gecommitte staat;
   5. de agents registreren met wat ze draaien (`placed`);
   6. `trim_returning_agent_surplus` en de reconcile.
3. **Hop van de leider herstart:** dezelfde eigenaar pakt zijn eigen lease
   terug, maar pas na zijn eigen SNTP [F3].

### 10.5 Protocol of truukje

Wat het protocol nodig heeft en dus blijft:

- de lease verlengen;
- de faaltellers (4 en 7) en de regel "de opslag zegt geen leader";
- de weigering die 30 s geldt;
- de ghost-overname op S3 (`s3.rs`, Bunny 08-09).

Een agent die niet antwoordt, een opslag die traag is: dat is het LAN, en
daar hoort een termijn en een tweede ronde bij. Dat is geen retry om een fout
van de eigen node te maskeren.

Te wegen:

- **Een leider die zijn staat niet kon lezen, leidt leeg** [F5].
  `finish_leading` maakt bij `Err` (niet gelezen) of een kapotte snapshot
  toch een `live` leader met een lege store. Hij zaait niet ("nooit zaaien
  op een storing"), maar de eerste jobmutatie (een `POST`, een delete) maakt
  hem vies. De snapshot schrijft dan `state/<cluster>` met alleen die job. De
  snapshot is "de enige waarheid", en verwijderen is afwezigheid. Op de host
  is het hetzelfde (`agentd/src/node.rs` `become_leader`).
- **De dispatch-taak is serieel** [S16]. Een stop naar een dode agent houdt
  elke `run` daarna tot 60 s op.

---

## 11. Te wegen: fouten en kandidaten

### 11.1 Mogelijke fouten (F)

F1 is de reparatie die al in de werkboom staat. De rest staat in de volgorde
waarin ik ze zou bekijken.

| # | Wat | Waar | Zeker |
| --- | --- | --- | --- |
| F1 | **Vier system-verbindingen bij de start tegen een plafond van drie** (F23 uit deel 1): hoofd, store-taak, de kloktaak die haar verbinding het hele uur openhield, en een losse in `init_specs`. Sinds 1c01b0d was de geweigerde vierde fataal (`HOP_KERNEL_GONE step=init`). **Gerepareerd in de werkboom, niet gecommit:** `init_specs` over `sys.conn_mut()`, `drop(sys)` vóór de wacht. Elders geen vierde (§4.2). Bijwerking: de stat gebeurt nu bij elke start | `main.rs` (`init_specs`, `clock_task`, `resident`) | H, zelf getoetst |
| F2 | **`Kept::Unreadable` begint leeg, en de veger stopt elke app**; standalone gaan ook de jobs weg (de leader zat in hetzelfde blob) en komen de init-jobs niet terug. De aannemelijke trigger is een herstart naar een Hop met een andere `HANDOFF_VERSION` of een veld dat de nieuwe decoder weigert; ook `TooBig` en `OutOfMemory` bij het lezen | `node.rs` (`restore`), `main.rs` (`resident`), `agent/src/handoff.rs` (`parse`) | H, zelf getoetst |
| F3 | **Een herstarte geclusterde Hop doet pas mee na zijn eigen SNTP**: `CLOCK_SYNCED` begint per proces op `false`, en `join` eist hem, ook al staat de klok van de kern al. Mislukt de eerste sync, dan na 300 s pas opnieuw: geen renew, geen heartbeat, lease en agent na 30 s weg, de taken elders | `main.rs` (`CLOCK_SYNCED`, `clock_ok`), `node/cluster.rs` (`join`) | H (mechanisme), M (gevolg) |
| F4 | **De eigenaar wacht op de hele download van een flipbundel of een nieuw Hop-image**, en bij een koude flip op elke stop (3 s per stuk). In die tijd: geen bus (de API staat stil), geen tik, geen stop-bel, en geclusterd geen renew (de elector tikt in de eigenaar) en geen heartbeat | `node.rs` (`flip`, `restart`), `flip.rs` (`fetch_raw`), `main.rs` (de tweede `HttpImages`) | H (mechanisme, zelf getoetst), M (gevolg) |
| F5 | **Een leider die zijn staat niet kon lezen, leidt met een lege store**; de eerste jobmutatie overschrijft `state/<cluster>` met alleen die job | `node/cluster.rs` (`finish_leading`), `leader/src/persist.rs`, op de host `agentd/src/node.rs` (`become_leader`) | H, zelf getoetst |
| F6 | **De veger ziet alleen de slots 2 tot en met `HOPOS_CORES`** (`KernSys::num_cores` = de app-cores), terwijl de kern tot `max_slots` uitdeelt: `max(app-cores + 1, 32)`, op rk3566 11 en op de LicheeRV 16. Een bewoner van niemand in een hoger slot (een app in een sharegroup) blijft tot de koude boot. **Opgelost (02-11):** er is geen maximum aantal slots meer (hop-os b5bd0a2, het slot-LAN een /16); de veger vraagt de bezette slots aan de kern (`SLOTS`, in stukken zoals `getdents`) en slaat het eigen slot over | `runner/src/hopos.rs` (`sweep_strays`), `hopos-runner/src/lib.rs` (`occupied`) | H, zelf getoetst |
| F7 | **`slot_status` slikt elke fout**: de laatste stand, of een leeg slot. Een verse Hop heeft geen laatste stand, dus een `SLOT_STATUS` die bij de overname niet antwoordt, laat een draaiende taak niet overnemen én niet vegen; de leader plaatst hem opnieuw naast de oude. Dat gaat tegen de regel van 1c01b0d (bij de start hard) | `hopos-runner/src/lib.rs` (`slot_status`), `runner/src/hopos.rs` (`adopt_running`) | H (code), V (kans) |
| F8 | **`https` vóór de eerste SNTP faalt** (leanhttps `NoClock`) en herstart met back-off, terwijl `HOP_SNTP_FAIL` en het commentaar zeggen dat https "wacht". De kloktaak loopt naast het zaaien, dus een init-job van GitHub faalt bij een koude boot vrijwel altijd eerst (met een Crash-event) | `main.rs` (`clock_task`), `fetch.rs`, `agent/src/node.rs` (`schedule_restart`) | H (mechanisme), M (hoe vaak) |
| F9 | **Een halflevende Hop na een late configweigering**: `advertised` komt ná de spawn van klok-, store- en downloadtaak, en met `pending()` blijven `SET_CLOCK` en de store-dienst voor de apps lopen onder "REFUSING to start agent/leader" | `main.rs` (`resident`) | H, zelf getoetst |
| F10 | **De warme flip leunt aan Hop-kant op stille terugvallen**: kern B heeft pas een system-API na zijn DHCP-lease, en Hop draait eerder of door. In het gat: de laatste stand, lege logs, staat en store die het later opnieuw proberen, tot 2 × 3 s dial per call in de eigenaar. Een eerste `SET_CLOCK` in het gat is `Fatal` | `hopos-runner/src/lib.rs`, `main.rs` (`store_task`, `clock_task`), `sntp.rs` (`apply`); hop-os `hopos/src/net.rs` (`Node::run`) | M |
| F11 | **De store-taak is bij de start niet hard**: een `NEXT_STORE` die niet antwoordt, is een lus van 1 s met regels, terwijl restore, klok en init sinds 1c01b0d hard stoppen | `main.rs` (`store_task`) | H |
| F12 | **Geen speling in het verbindingenbudget**: de herverbinding van applib kan de vierde plaats vragen terwijl de kern de oude nog telt (na een `Timeout` is zijn werker nog bezig). Valt dat samen met de eerste `SET_CLOCK`, dan is het `Fatal` | `applib/src/sys.rs` (`call_into`), hop-os `kern/src/system.rs` (`admit`) | V |
| F13 | **De poorten binden pas na restore, veger en zaaien**, en de canary geeft 15 s. Een herstart waarvan de nieuwe Hop langer doet tot `bind` (strays van 3 s per stop, trage calls), reset de node. Die termijn staat nergens aan Hop-kant | `main.rs` (`resident`); hop-os `hopos/src/watchdog.rs` (`STALE_NS`) | M |

### 11.2 Kandidaten om te schaven (S)

Waar het ontwerp twee dingen doet waar één kan, pollt waar een antwoord kan,
of code draagt zonder gebruiker. Elk punt heeft één regel richting.

| # | Wat | Richting |
| --- | --- | --- |
| S1 | Vier manieren van eindigen bij de start: `return` (exit 0), `shutdown(3)`, `pending()`, en een regel en door | Eén `fail(stap, reden) -> !` met een exitcode ≠ 0 |
| S2 | De configtoetsen verspreid; `advertised` na drie spawns | Alle config vooraan, vóór de eerste spawn |
| S3 | De klok als parallelle taak met een eigen verbinding (F3, F8, F12) | SNTP als stap in de start (met termijn), `SET_CLOCK` via de eigenaar, daarna elk uur een meting als bericht |
| S4 | De staat bij de kern op elke tik en elke 10 s (`Keep`, `STATE_REFRESH`, `keep_state`): niemand leest die versie | Alleen in `restart`, `leave` en `on_stop` |
| S5 | Compat voor kernen van vóór v3.0.19, terwijl Hop sinds 1c01b0d op v3.0.19 leunt: `Kept::Unsupported`, `Keep.refused`, `HOP_KERN_LOG_NONE` en `KERN_LOG_RETRY_MS`, `HOP_NO_CLOCK`, `DEFAULT_MEMORY`, de standaard van `ColdFlip`, `cores.max(1)`, `Origin::None`, "een kern zonder rij", "een kern van vóór deze afspraak" | Een ondergrens, dan weg (vraag 6) |
| S6 | Twee downloaders: de downloadtaak en die van de node voor bundel en image | Ook bundel en image via de downloadtaak, met een rauwe sink |
| S7 | Pollen waar een antwoord kan: `NEXT_LOG` per app per tik (plus tot 256 kernregels), twee `SLOT_STATUS` per taak per 5 s, stromen elke 0,5 s, de acceptor elke 5 ms als alle werkers bezet zijn (`BUSY_POLL`), `deliver` elke 10 ms bij een volle inbox, `/tasks` bij elke agent per leader-tik | `NEXT_LOG` als lange wacht over alle slots (zoals `NEXT_STORE`); één `SLOT_STATUS` per poll; een bel van de eigenaar naar zijn stromen; `Handoff::free` wekt de acceptor |
| S8 | Herhaallussen op de runtime-paden: de store-taak (1 s), `save_state` (elke tik), `SET_CLOCK` `Later`, een fout van `accept` (100 ms) | Na vraag 1: een kern die niet antwoordt, is ook na de start fataal |
| S9 | Stille terugvallen in `KernSys`: `slot_status`, `next_log_line`, `next_kern_line` | Een fout naar boven; de aanroeper beslist, en die beslist hard |
| S10 | De koude flip: `COLD_FLIP_WAIT` als detector, `held`, `resume_after_flip`, `cold_flip_back` | Weg met deel 1 §13.7 (na de 202 geen weg terug) |
| S11 | `MAX_CONCURRENT_DOWNLOADS` en `Error::Busy` zijn op HopOS dood; de kern zegt "Hop begrenst zelf op vier" | Weg, en de regel in de kern bijstellen |
| S12 | Standalone na een herstart twee registraties: `Node::new` registreert leeg, de restore daarna met wat hij draait | Eén, na de restore |
| S13 | `ACTION_ROUNDS` (16) en `LINK_ROUNDS` (4) als grens tegen een keten die niet convergeert | Bij de grens luid (een fout in de agent), of fataal |
| S14 | `HOP_LISTEN_FAIL` "kost alleen die poort": zonder leader-poort geen leader-API, zonder agent-poort faalt de canary | Fataal |
| S15 | Init-jobs die niet te lezen of te zaaien zijn: `HOP_INIT_FAIL` en door | Vraag 7 |
| S16 | De dispatch-taak serieel met 60 s voor een stop | Stoppen en runs gescheiden, of de stop korter |
| S17 | Geen toets op het verbindingenbudget: de nep-kern telt niet | Een toets die de verbindingen van de start telt, met de grens van de kern |
| S18 | De poorten binden na de restore (F13) | Binden vooraan (de stack neemt de handshake aan, de canary slaagt), acceptors en werkers later |
| S19 | Health probes niet bedraad op HopOS | Bedraden, of een job met een `health_check` in de toelating weigeren |

### 11.3 Wat in deze ronde logisch bleek

Om de lijst eerlijk te houden: de meeste volgordes zijn verplicht en goed
beredeneerd, met de les en de datum in de code.

- **De start:** eerst de restore, dan de veger, dan de cluster of de
  init-jobs, en nooit zaaien op een staat die niet schoon is. Elke stap heeft
  zijn meting (01-10, 02-10, 22-10).
- **De herstart:** eerst de download, dan de staat, dan de vraag. Bij een
  weigering wordt het rauwe slot opgeruimd. De vlag gaat vóór het antwoord,
  en Hop wacht op het eigen antwoord in plaats van een vaste tijd.
- **De koude flip:** eerst de download, dan pas de stops. Wat Hop al weet
  (het board-contract), vraagt hij niet.
- **Eén eigenaar met een bus**, en de downloads van artifacts ernaast
  (03-10). De taken van de cluster bezitten hun eigen verbinding, en de
  link-taak staat apart om een wachten op zichzelf te voorkomen.
- **De listeners binden vóór de spawn**, zodat `HOP_UP` waar is.
- **De lease als timer** (Bunny, 08-09), de faaltellers, en de regel "de
  opslag zegt geen leader, dus de taken blijven" (traqqr, 08-09).
- **De staat in kerngeheugen**, die met de boot sterft (7726782).
- **SNTP streng** (mode, LI, stratum, het originate-veld, na 2026), en
  https alleen met een vertrouwde klok.
- **`init_specs` over de hoofdverbinding** (de werkboom): een stap in de
  reeks in plaats van een eigen verbinding.

---

## 12. Docs en commentaar die achterlopen

De code wint. Dit is wat bij het lezen afweek. Niets hiervan is rechtgezet.

**`README.md`**

| Plek | Wat er staat | Hoe het is |
| --- | --- | --- |
| Streams | "4 on a host, 2 on HopOS" | HopOS laat er 3 toe (`node.rs` `MAX_STREAMS`, 149a63f) |
| Restarting Hop | "The kernel rings Hop's stop bell, Hop puts its state down once more and exits" | Sinds 22-10 vertrekt Hop zelf na zijn 202 (`restart::depart`); de bel is het vangnet |
| Restarting Hop | "again every 10 s (a warm kernel flip leaves the new kernel empty)" | Klopt als beschrijving, maar die schrijf heeft geen lezer (§5.1) |

**`../CLAUDE.md`**, de sectie "HopOS Driver (driver=`hop`)", is van de
Go-generatie:

- `internal/runner/hopos.go` en `SlotManager`;
- de two-phase load met de apploader, en TamaGo-images;
- `LogBroadcaster` en `metal/slots`;
- "11 slots + core 0 voor de agent".

Nu is het `runner/src/hopos.rs` (`HopRunner` over `SystemApi`). Hop haalt
het image zelf en stroomt het met `STREAM_IMAGE`; er is geen apploader.

**`../PLAN.md`** is het plan van na de traqqr-migratie (08-09, Go v1.0.x).
Principe 1 ("geen mutexen, geen atomics in productiecode") klopt niet voor de
bewoner: `CLOCK_SYNCED`, `WANTED` en `LEAVING` zijn atomics ("een vlag, geen
protocol"). Principe 2 ("hop moet daar doorheen blijven draaien en zichzelf
herstellen") gaat over de externe store. Dat past bij §10.5, maar het leest
als het tegendeel van hard falen. Een regel erbij dat het alleen voor het
LAN buiten de node geldt, maakt het eenduidig.

**`../HOWTO.md`** zegt niets over de HopOS-bewoner.

**Commentaar in de code**

| Plek | Wat er staat | Hoe het is |
| --- | --- | --- |
| `main.rs`, moduledoc | "Daarna drie soorten taken" | Er staan er vier, en de store-taak en de vier taken van de cluster ontbreken |
| `main.rs`, moduledoc | De start: staat, veger, cluster, "en alleen bij een schone start de init-jobs uit `HOPOS_INIT_JOBS` of `/hop/init-jobs.json`" | Het lezen staat sinds de werkboom vóór de restore; alleen het zaaien hangt van een schone start af |
| `main.rs` `clock_task` en `HOP_SNTP_FAIL` | "`https` wacht op een vertrouwde klok", "https downloads wait for it" | `https` faalt (`NoClock`) en de taak herstart (F8) |
| `main.rs` `BUSY_POLL` | "een vierde gelijktijdige verbinding" | Met `WORKERS` = 4 is het de vijfde |
| `node.rs` `EXIT_KERNEL` | "geen 0, dus de kern laat hem liggen" | De kern laat elke ongevraagde exit liggen, ook 0 (`hopos/src/slots.rs` `crashed`) |
| `node.rs` `STATE_REFRESH` | "een herstart binnen tien seconden na een flip is de enige die dan nog een lege kern treft" | `Node::restart` schrijft de staat zelf vlak vóór de vraag (§5.1) |
| `node.rs` `Kept::Unreadable` | "dan geen init-jobs" | En de veger stopt elke app (F2) |
| `flip.rs`, moduledoc | "die bouwt de system-client zelf opnieuw op" | Pas als kern B zijn lease heeft (§7.3) |
| `node/cluster.rs` en `local.rs`, moduledoc | "Zonder lock-config (`HOPOS_LOCK_URL`, `HOPOS_S3_*`)", "In een cluster (`HOPOS_LOCK_URL` of `HOPOS_S3_*`)" | S3 alleen maakt geen cluster; daarvoor is `HOPOS_LOCK_TYPE=s3` nodig (`lock.rs`) |
| `runner/src/hopos.rs` `sweep_strays` | "Alleen de slots boven het eigen (1)" | En niet hoger dan `num_cores` (F6) |
| `runner/src/hopos.rs` `MAX_CONCURRENT_DOWNLOADS` | "Hoeveel images de node tegelijk binnenhaalt" | Op HopOS één (S11) |

**In hop-os, over Hop**

| Plek | Wat er staat | Hoe het is |
| --- | --- | --- |
| `kern/src/system.rs` `MAX_STREAMS` | "Hop begrenst zelf op vier (`runner::MAX_CONCURRENT_DOWNLOADS`)" | Hop stroomt één image tegelijk, plus hoogstens een bundel of een image van Hop |
| `kern/src/system.rs` `MAX_SYSTEM_CONNS` | "de tweede is voor een herverbinding" | Voor Hop gaat de extra plaats naar de store-taak, en is er geen speling (§4.3) |
| `docs/flip.md` | Hop "hoort zijn staat opnieuw te schrijven zodra zijn system-client de nieuwe kern bereikt" | Hop doet het op een ritme van 10 s |

---

## 13. Het voorstel: wachten, bellen, hard falen, LEAN

Dit is het voorstel voor de Hop-kant. Het staat nog niet vast. Het rust op
§11, op deel 1 §13 en op Dereks regels:

- "liever luid falen dan 800 truukjes iets in leven te krijgen. Goed wachten,
  goed gewacht, hard falen";
- "sequence wanneer mogelijk, bellen doen we eigenlijk alleen met er staat
  iets op het LAN";
- "de backbone moet eenvoudig en LEAN blijven, anders wordt een FLIP alleen
  maar lastiger";
- "niet 100 retries, we wachten op een juiste manier, en anders falen we hard
  overal; alleen code waar nodig en met een reden".

Per punt staat wat er nu is, wat ik zou doen, en wat er daardoor weg kan.
Wat ik niet zelf kan beslissen, staat in §13.9.

### 13.1 De vier regels, voor Hop

**R1. Bellen.** Hop wacht op het LAN en op antwoorden van de kern, en op
niets anders:

- een verbinding of een verzoek;
- de lease-opslag, een andere node, een artifact-server, de NTP-server;
- het antwoord op een call naar de kern;
- de stop-bel.

Binnen Hop is alles een volgorde, of een bericht met een antwoord.

**R2. Wachten.** Elke wacht is één antwoord plus één termijn. Waar nu een
ritme vraagt "is er al iets" (de logs, de stromen, een vrije werker), komt een
lange wacht of een bel van de eigenaar.

**R3. Falen.** Hop valt, dan valt de node. Eén weg: `fail(stap, reden) -> !`
met een regel `HOP_FATAL step=<stap> <reden>`, het net-afscheid en een exit
≠ 0. De kern laat hem liggen, de watchdog reset (deel 1 §13.2 en §13.4).

| Is een antwoord binnen het protocol (blijft) | Is fataal |
| --- | --- |
| Een agent die niet antwoordt, een weigering van een agent | Een kern die niet antwoordt, of weigert wat hij nooit weigert |
| Een lease-opslag die traag is of zwijgt (de faaltellers, de lease als timer) | Een staat bij de kern die niet te lezen is (vraag 3) |
| Een artifact-server of DNS die faalt (de taak faalt, het herstartbeleid van de app) | Een config die niet klopt (vraag 2) |
| NTP die niet antwoordt (de volgende meting) | Een taak die niet spawnt, een poort die niet bindt |
| Een app die crasht (het herstartbeleid) | Een keten van acties die niet convergeert |

**R4. LEAN.** Code zonder gebruiker of zonder reden gaat weg. Een tweede weg
naar hetzelfde gaat weg.

### 13.2 De start als één reeks

| Nu | Voorstel |
| --- | --- |
| Configtoetsen in stap 3, 5 en 13; vier manieren van eindigen (S1, S2, F9) | **Alle config eerst**, vóór de eerste spawn: `BootConfig`, `ClusterConfig`, `advertised`. Een fout is `fail(config)` (of zichtbaar parkeren, vraag 2) |
| De poorten binden na de restore (F13) | **Binden direct na `appnet::up`.** De stack neemt de handshake aan, dus de canary slaagt tijdens de restore. Acceptors en werkers komen pas na de restore |
| De klok als parallelle taak; https faalt tot ze er is (F8); een herstarte Hop wacht op zijn eigen SNTP (F3) | **De klok als stap**, ná de veger en vóór het zaaien of de cluster: één sync met termijn (DNS plus `ATTEMPTS` × 3 s), `SET_CLOCK` over de hoofdverbinding. Na een herstart zegt het blob dat de klok staat (vraag 5) |
| `init_specs` over de hoofdverbinding (de werkboom) | **Blijft**, als stap vóór de restore |
| Restore: `Unreadable` begint leeg (F2); een `SLOT_STATUS` zonder antwoord is een leeg slot (F7) | **Elke kern-call van de start is hard**: `Unreachable`, `Unreadable` (vraag 3) en een `SLOT_STATUS` zonder antwoord bij het adopteren en vegen |
| De veger tot `HOPOS_CORES` (F6) | **De veger over de bezette slots.** Er is geen maximum (02-11): de kern somt ze op (`SLOTS`, hop-os b5bd0a2); een `HOPOS_SLOTS` bestaat niet |
| Spawn van klok en store: een regel en door | **Elke spawn is `fail(spawn)`** |
| Standalone na een herstart twee registraties (S12) | **Eén**, na de restore |

De reeks wordt dan:

1. `HOP_BOOT`;
2. alle config;
3. `appnet::up`;
4. `bind` van beide poorten;
5. `init_specs`;
6. restore en adopteren;
7. de veger;
8. de klok (of het blob zegt dat ze staat);
9. de node, de cluster-taken, de boot-claim of de init-jobs;
10. spawn van de store-, download-, acceptor- en werkertaken en de
    uurmeting;
11. `HOP_UP`.

Wat weg kan:

- `pending()` op drie plekken;
- `HOP_CLUSTER_NO_CLOCK` en de join-poll per tik;
- de regels "cannot spawn … and continue";
- de tweede registratie.

Voor de flip: een Hop die maar één weg naar "draait" heeft, heeft ook maar
één toestand om over te dragen.

### 13.3 De verbindingen

| Nu | Voorstel |
| --- | --- |
| Drie: hoofd, store, klok rond `SET_CLOCK`; geen speling (F12) | **Twee vast:** de hoofdverbinding (alles van de eigenaar, ook `SET_CLOCK`: de uurmeting komt als bericht naar de eigenaar) en de store-taak (de lange wacht). De derde plaats is weer de herverbinding, zoals bij een app |
| Geen toets op het budget (S17) | **Een toets** met een nep-kern die de verbindingen telt en bij de grens van de kern weigert |

`MAX_HOP_CONNS` kan dan blijven, met het commentaar "de store-taak plus de
herverbinding".

### 13.4 De staat bij de kern

| Nu | Voorstel |
| --- | --- |
| Elke tik met een wijziging, en elke 10 s (S4) | **Alleen in `restart` (vóór de vraag), `leave` en `on_stop`.** Weg: `Keep`, `STATE_REFRESH`, `keep_state`, `state_failed` en het ritme |
| `Unsupported` en `Keep.refused` voor een oude kern (S5) | **Weg** met de ondergrens (vraag 6) |
| `Unreadable` begint leeg (F2) | **`fail(restore)`** (vraag 3) |

Voor de flip: niets. Het blob reist niet mee, en na deze stap schrijft Hop
het pas als iemand het gaat lezen.

### 13.5 Herstart en flip

| Nu | Voorstel |
| --- | --- |
| Bundel en image in de eigenaar (F4, S6) | **Via de downloadtaak**: een `Order` met een rauwe sink. De eigenaar stroomt de brokken zoals bij een artifact en blijft tikken. Het antwoord op `POST /flip` of `/restart` wacht in de werker, niet in de eigenaar. Weg: de tweede `HttpImages` |
| Na de warme sprong de terugvallen (F10) | **Afhankelijk van vraag 1.** Garandeert kern B de system-API vóór Hop weer praat, dan is elke call zonder antwoord ook na de start fataal (S8, S9). Weg: de laatste stand in `slot_status`, de stille `None` van de logs, de lus van de store-taak, de regel `Later` van de klok |
| De koude flip met `COLD_FLIP_WAIT` en de weg terug (S10) | **Weg met deel 1 §13.7.** Na de 202 springt de kern of valt hij. Hop stopt zijn taken (`hold_for_flip` blijft, want de leader moet ze elders plaatsen) en wacht niet op een detector |
| De canary van 15 s als onzichtbare termijn van de herstart (F13) | Opgelost door het vroege binden (§13.2) |

### 13.6 Het cluster

| Nu | Voorstel |
| --- | --- |
| Een leider zonder geladen staat leidt leeg (F5) | **Niet leiden:** `StopLeader` en de lease los, en de volgende verkiezing opnieuw (vraag 4). Een opslag die zwijgt, is het LAN: dat is een tweede ronde, geen fout van deze node |
| Een herstarte leider wacht op zijn eigen SNTP (F3) | Opgelost door het blob (vraag 5) |
| De dispatch-taak serieel met 60 s (S16) | Stoppen in een eigen rij, of de termijn van de daemon herzien |
| De lease, de faaltellers, de ghost-overname | **Blijft:** protocol (§10.5) |

### 13.7 Pollen en bellen

| Nu | Voorstel |
| --- | --- |
| `NEXT_LOG` per app per tik, plus de kern (S7) | **Eén lange wacht** `NEXT_LOG` over alle slots, zoals `NEXT_STORE` (kern-werk, en dat kost een verbinding: vraag 8) |
| Twee `SLOT_STATUS` per poll | **Eén**: de status draagt het gebruik al |
| Stromen elke 0,5 s | **Een bel van de eigenaar** naar een stroom als er een regel of event bij kwam (in-proces, geen interrupt: een waker in de `Hub`) |
| `BUSY_POLL` (5 ms) | **`Handoff::free` wekt de acceptor**; de `idle`-waker is er al, voor één wachter |
| `deliver` met `INBOX_RETRY` (10 ms) | Een waker op de inbox, of een inbox die groot genoeg is voor alle uitstaande opdrachten |

### 13.8 LEAN: weg zonder vervanging

Per stuk eerst een grep op gebruikers, dan pas weghalen.

- **Compat voor kernen onder de ondergrens (vraag 6):**
  - `Kept::Unsupported`, `Keep.refused`, `HOP_KERNEL_STATE_NONE` en
    `HOP_KERNEL_STATE_KEPT`;
  - `KernLogNote`, `KERN_LOG_RETRY_MS`, `kern_refused`;
  - `HOP_NO_CLOCK` en de monotone terugval in `wall_ms`;
  - `DEFAULT_MEMORY` en `HOP_MEMORY_DEFAULT`;
  - de standaard `Yes` van `ColdFlip` voor een ontbrekende sleutel;
  - `cores.max(1)` zonder system-core;
  - `Origin::None` ("an older kernel");
  - "een kern zonder rij" in de store-taak;
  - "een kern van vóór deze afspraak" in `restart.rs`.
- **Dood op HopOS:** `MAX_CONCURRENT_DOWNLOADS`, `downloads` en `Error::Busy`
  in `HopRunner`. De node doet er één tegelijk.
- **Zonder lezer:** de periodieke staat (S4).
- **Een tweede weg naar hetzelfde:**
  - de tweede downloader (S6);
  - de tweede registratie (S12);
  - `pending()` naast `return` naast `shutdown` (S1).
- **Alleen voor de toetsen:** `flip::flip` (fetch plus ask in één) gebruikt
  alleen de toets in `flip.rs`; de node roept `fetch` en `ask` apart aan.

### 13.9 Vragen die ik niet zelf kan beslissen

1. **Het gat na een warme flip.** Kern B heeft pas een system-API na zijn
   lease, en Hop draait al. Twee opties:
   - **a.** Kern-werk: de system-listener op 10.100.0.1 staat los van de
     lease (vóór Hop een beurt krijgt), of Hop krijgt pas beurten als de
     listener er is. Dan is aan Hop-kant elke call zonder antwoord fataal,
     ook na de start.
   - **b.** Zo laten: Hop verdraagt het gat met de terugvallen van §7.3.

   Voorstel: **a**. Het sluit aan bij deel 1 §13.3 ("de boot als één
   reeks") en haalt de helft van S8 en S9 weg.
2. **Een configfout.** Twee opties:
   - **a.** `fail(config)`: exit, en een resetlus als de config op het
     medium fout is (of blind in fase 1, deel 1 F5);
   - **b.** zichtbaar parkeren, zoals nu met `pending()`, maar dan zonder de
     taken die al liepen (F9).

   Voorstel: **a**, met de termijn van de ene guard uit deel 1 §13.2. Een
   node die niets doet maar slaat, is een vermomde fout.
3. **`Kept::Unreadable`.** Hard falen (de koude boot begint schoon, met de
   init-jobs), of leeg beginnen zoals nu (de veger stopt alles, standalone
   zonder jobs)? Voorstel: hard. Bij een nieuw image met een andere
   `HANDOFF_VERSION` hoort de oude Hop de herstart te weigeren; dat kan
   alleen als het image zijn versie kenbaar maakt (bijvoorbeeld in de
   sha-toets van `restart`).
4. **Een leider die zijn staat niet kan lezen.** Niet leiden (de lease los,
   een volgende ronde), of fataal? Voorstel: niet leiden. Het is het LAN (de
   opslag), en een andere node kan het wel. Dit raakt ook de host
   (`agentd`).
5. **De klok na een herstart.** Twee opties:
   - **a.** Het blob draagt "de klok staat" mee; de nieuwe Hop vertrouwt de
     klok van de kern, die zijn voorganger zette;
   - **b.** SNTP als stap, ook bij een herstart.

   Voorstel: **a**, want b laat een herstart afhangen van een server buiten
   het LAN. Daaronder ligt de vraag wat een geclusterde node zonder NTP doet:
   nu wacht hij luid en voorgoed (`HOP_CLUSTER_NO_CLOCK`). Fataal na een
   termijn, of wachten?
6. **De ondergrens van de kern voor Hop.** Sinds 1c01b0d leunt Hop op
   v3.0.19 (de toelating vanaf de eerste instructie), en hij bouwt tegen
   v3.0.20. Wordt v3.0.19 de grens, dan kan alles uit §13.8 "compat" weg.
   Een M4 draait nog Hop 3.0.9 op kern 3.0.20; het omgekeerde (een nieuwe
   Hop op een oude kern) kan alleen via `POST /restart` naar een nieuw
   image.
7. **Init-jobs die niet te lezen of te zaaien zijn.** Nu een regel en door:
   een node zonder zijn basis. Hard (een resetlus bij een fout bestand), of
   luid door? Voorstel: hard als het bestand er is maar niet klopt; geen
   bestand blijft "geen init-jobs".
8. **`NEXT_LOG` als lange wacht.** Een lange wacht houdt een verbinding vast.
   Over de hoofdverbinding kan dat niet (de eigenaar wacht dan), dus het kost
   een eigen verbinding: weer drie vast. Twee opties:
   - **a.** De logs in de store-taak, die toch al lang wacht (één wacht op
     "store of log");
   - **b.** Het pollen per tik houden, met de reden in de code.

   Voorstel: **a**, als de kern één lange wacht voor beide kan geven.
9. **Exitcodes.** Eén code voor elke fatale val, of per soort (3 kern, 4
   config, 5 intern)? Voor de kern maakt het geen verschil (§8.2); voor de
   diagnose wel. Voorstel: per soort, vastgelegd in `abi`.
10. **De ene retry van applib** (deel 1 §13.9 vraag 9). Blijft hij, dan hoort
    de plaats ervoor in het budget (§13.3). Valt hij, dan is elke
    transportfout in Hop fataal (met vraag 1a).

### 13.10 Volgorde van uitvoeren (voor de agents)

Elke fase eindigt met `tools/gate.sh` in hop, en de HopOS-ringen die Hop
raken: `hop`, `hop-restart`, `hop-fail`, `flip`, `store` en de cluster-ring
(`tools/qemu-test-cluster.sh`). Daarna `target/` meten en opruimen. Per fase
krijgt elke agent zijn eigen bestanden.

1. **Fundament** (één agent, alles hangt eraan):
   - `fail(stap, reden) -> !` en de exitcodes (vraag 9);
   - alle config vooraan;
   - de toets op het verbindingenbudget (S17);
   - F1 committen.
2. **Parallel:**
   - **a. De start-reeks** (`main.rs`): vroeg binden, de klok als stap en
     `SET_CLOCK` via de eigenaar, elke spawn hard, één registratie.
   - **b. De staat en de restore** (`node.rs`, `hopos-runner/src/state.rs`,
     `runner/src/hopos.rs`): S4, `Unreadable` hard, adopteren en vegen hard
     over de bezette slots die de kern opsomt (`SLOTS`; er is geen
     maximum en geen `HOPOS_SLOTS`, 02-11).
   - **c. De downloads** (`download.rs`, `flip.rs`, `restart.rs`, `node.rs`
     `flip` en `restart`): bundel en image via de downloadtaak (F4, S6), en
     de koude weg terug weg als deel 1 §13.7 er is.
   - **d. Het cluster** (`node/cluster.rs`, `relay.rs`): F5, S16.
3. **Na de kern-beslissingen** (vraag 1, 6 en 8): de terugvallen en de
   herhaallussen op de runtime-paden weg (S8, S9), de compat weg (S5), en
   het pollen (S7).
4. **De docs** (§12).

Na fase 2 van deel 1 (de boot als één reeks in de kern) valt vraag 1 vanzelf
naar a.
