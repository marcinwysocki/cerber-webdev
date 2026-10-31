# Cerber Telemetry Gateway

Publiczne, read-only API telemetrii robota Cerber (DEEP Robotics Lite3 Venture).
Gateway stoi w sieci VPN, czyta rosbridge i MQTT, a na zewnątrz wystawia
wyłącznie odczyt: REST + WebSocket. Sterowanie nie jest wystawione.

```
robot (ROS 2) --rosbridge ws:9090 (VPN)--> gateway (serwer w VPN) --Caddy 443--> świat
GPS           --MQTT cerber/gps/data-^
```

## Kontrakt

Kontrakt jest generowany z kodu (`contracts.py`), nie pisany ręcznie:

| Plik | Co to jest |
| --- | --- |
| `contract/openapi.json` | OpenAPI 3.1 — wszystkie endpointy REST, schematy, `securitySchemes`, plus `x-websockets` opisujące strumień WS |
| `contract/schemas/*.json` | JSON Schema (draft 2020-12) każdego modelu publicznego, do walidacji po stronie klienta/CI |
| `contract/examples/*.json` | przykładowe payloady, zwalidowane modelami i schematami |

Regeneracja po zmianie modeli: `python export_contract.py`.

## Endpointy

| Metoda | Ścieżka | Zwraca |
| --- | --- | --- |
| GET | `/health` | `Health` — stan połączeń z rosbridge i MQTT (bez klucza) |
| GET | `/api/v1/robots` | `RobotList` |
| GET | `/api/v1/robots/{robot_id}` | `RobotSummary` (`online` i `last_seen` jak w `status`, patrz niżej) |
| GET | `/api/v1/robots/{robot_id}/telemetry/latest` | `TelemetrySnapshot` — ostatni znany stan, sekcje `null` dopóki nie przyjdą; pole `status` z chwili odpowiedzi |
| GET | `/api/v1/robots/{robot_id}/gps/latest` | `GPSFix` albo 404 |
| GET | `/api/v1/robots/{robot_id}/streams` | `VideoStreamList` — adresy WHEP; `vpn_only` z `STREAMS_VPN_ONLY` |
| WS | `/api/v1/robots/{robot_id}/telemetry/stream?api_key=…` | ramki `TelemetryEventEnvelope`: pierwsza `snapshot` (z `status`), potem `imu` / `joints` / `odometry` / `ultrasound` / `ptz` / `gps` / `sensors_alive` (każdy typ najwyżej raz na `WS_THROTTLE_S`, patrz niżej) oraz `status` (przy zmianie i co `WS_HEARTBEAT_S`); ramki od klienta są ignorowane (max 4096 B) |

Autoryzacja: nagłówek `X-API-Key` (REST) lub query `api_key` (WS). Zły klucz → 401; na WS połączenie jest
przyjmowane i od razu zamykane kodem 4401 (nieznany `robot_id` → 4404), więc klient widzi kod.

Throttling WS: zdarzenia każdego typu (`imu`, `joints`, `odometry`, `ultrasound`, `ptz`, `sensors_alive`, `gps`)
wychodzą najwyżej raz na `WS_THROTTLE_S` (domyślnie 1 s). Pierwsze zdarzenie danego typu idzie od razu i otwiera
okno; kolejne w oknie tylko podmieniają „ostatnią wartość”, która wychodzi na koniec okna (i otwiera następne).
Dzięki temu ostatni stan zawsze dociera do klienta najpóźniej po `WS_THROTTLE_S`, np. `ptz` MOVING → WAITING
kończy się ramką WAITING. `snapshot` (pierwsza ramka WS i `/telemetry/latest`) jest aktualizowany każdą
wiadomością, a `status` nie jest throttlowany. `WS_THROTTLE_S=0` wyłącza throttling.

Status robota: `{"type":"status","robot_id":…,"ts":<epoch>,"data":RobotStatus}`, gdzie `RobotStatus` =
`{online, last_seen, rosbridge_connected, mqtt_connected}`.

- `online` = połączenie z rosbridge jest otwarte **i** ostatnia wiadomość ROS przyszła w ciągu `STALE_AFTER_S`
  (domyślnie 5 s); `last_seen` = czas ostatniej wiadomości ROS. GPS z MQTT nie podtrzymuje `online` (jego
  świeżość mówi `gps_at`). `online` i `last_seen` w `RobotSummary` (REST) mają tę samą definicję.
- `status` wychodzi do wszystkich klientów od razu, gdy zmieni się `online`, `rosbridge_connected` albo
  `mqtt_connected` (np. robot przestał nadawać → `online: false` najpóźniej ok. 1 s po upływie `STALE_AFTER_S`),
  oraz jako heartbeat co `WS_HEARTBEAT_S` (domyślnie 5 s) do każdego klienta, niezależnie od jego aktywności.
  Klient nie musi odpowiadać. Brak ramek dłużej niż kilka `WS_HEARTBEAT_S` = zerwane połączenie z gatewayem.
- `snapshot` (pierwsza ramka WS) i `/telemetry/latest` mają pole `status` wyliczone w chwili wysłania.

Limity WS (kody zamknięcia są też w `x-websockets.closeCodes`):

| Kod | Kiedy |
| --- | --- |
| 1013 | osiągnięty limit `MAX_WS_CLIENTS` połączeń na robota — połączenie jest przyjmowane i od razu zamykane; spróbuj później |
| 1011 | brak odpowiedzi na ping WS: serwer wysyła ping co 20 s i czeka 20 s na pong. Przy `WS_THROTTLE_S=1` tak zwykle jest rozłączany klient, który przestał czytać (po ~40 s) |
| 1008 / 1006 | klient nie odbiera ramek przy wyższym tempie (np. `WS_THROTTLE_S=0`): każdy klient ma własną kolejkę 100 ramek, przepełnienie = rozłączenie; wolny klient nie spowalnia innych ani zbierania danych z ROS/MQTT. Ramka close z kodem 1008 dochodzi tylko wtedy, gdy bufor TCP zdąży się opróżnić w ciągu 1 s — zwykle gateway po prostu zrywa połączenie bez ramki close, a klient widzi 1006 |
| 1009 | ramka od klienta większa niż 4096 B (`--ws-max-size`, liczone po dekompresji permessage-deflate) |

Po zakończeniu obsługi połączenia WS gateway zrywa gniazdo najpóźniej po 12 s, także gdy klient nie odbiera
zaległych danych. Zerwanie idzie przez RST (`SO_LINGER` 0), więc jądro od razu zwalnia niewysłany bufor
i nie zostaje osierocone gniazdo w `FIN_WAIT_1`; klient widzi reset połączenia (1006).

## Uruchomienie

```bash
pip install -r requirements.txt
export API_KEY=... ROSBRIDGE_HOST=10.13.13.2 MQTT_HOST=10.13.13.1
uvicorn main:app --host 0.0.0.0 --port 8000 --ws-max-size 4096   # to samo co CMD w Dockerfile
# albo: python main.py (limit ramki ustawia sam; adres z `HOST`/`PORT`, domyślnie 0.0.0.0:8000)
# docs: http://localhost:8000/docs
```

Gateway nie czeka na rosbridge: gdy robot/VPN nie odpowiada, API startuje od razu, `/health` mówi
`degraded` (`rosbridge_connected: false`), GPS z MQTT działa, a połączenie z rosbridge jest ponawiane w tle.
Ponawianie ma dwie fazy:

- szybka — przez pierwsze `ROSBRIDGE_FAST_RETRY_S` (300 s) od utraty połączenia (albo od startu, jeśli połączenia
  jeszcze nie było) przerwa zaczyna się od ~1 s i rośnie najwyżej do ~10 s; gdy host w ogóle nie odpowiada (np.
  peer VPN wyłączony), sama próba czeka do 5 s na TCP, więc próby wypadają co ~6–15 s;
- wolna — potem jedna próba co `ROSBRIDGE_SLOW_RETRY_S` (60 s; plus do 5 s samej próby przy milczącym hoście),
  bez końca. Wejście w tę fazę loguje jedną linię
  (`[rosbridge] still unreachable after …`); kolejne próby nie są logowane.

Udane połączenie zeruje politykę: następna utrata połączenia znów zaczyna od fazy szybkiej. Każde udane
połączenie loguje jedną linię (`[rosbridge] connected to …` za pierwszym razem, potem `[rosbridge] reconnected
to …`), utrata — `[rosbridge] connection closed …`. Po każdym połączeniu topiki są subskrybowane dokładnie raz
(nieudane próby niczego nie kolejkują), bez restartu procesu. Wiadomość odrzucona (NaN/Inf, zły format) to jedna
linia `[rosbridge] <topik>: drop …` / `[mqtt] <topik>: drop …`, najwyżej raz na 10 s na topik (z liczbą
pominiętych od poprzedniej linii), więc czujnik zawieszony na NaN nie zalewa logów. Linie `[rosbridge]`/`[mqtt]`
idą na stdout od razu (bez buforowania), także bez `PYTHONUNBUFFERED`. `rosbridge_connected` jest `true` dopiero po handshake'u
WebSocket z rosbridge (samo połączenie TCP to za mało).

Zmienne: `ROBOT_ID` (`cerber-lite3-01`), `ROBOT_MODEL`, `API_KEY` (wymagany — bez niego aplikacja nie wystartuje), `ROSBRIDGE_HOST/PORT`, `MQTT_HOST/PORT`, `MQTT_GPS_TOPIC`,
`MEDIAMTX_BASE` (pusty — whep_url względne), `STREAMS_VPN_ONLY` (1), `STALE_AFTER_S` (5 — po tylu sekundach bez wiadomości ROS robot jest
offline), `WS_THROTTLE_S` (1.0 — najwyżej jedna ramka WS na typ zdarzenia na tyle sekund; `0` wyłącza),
`WS_HEARTBEAT_S` (5 — co tyle sekund `status` do każdego klienta WS; `0` = tylko przy zmianach), `ROSBRIDGE_FAST_RETRY_S` (300) / `ROSBRIDGE_SLOW_RETRY_S` (60 — fazy
ponawiania połączenia z rosbridge),
`ROSBRIDGE_ENABLED` / `MQTT_ENABLED` (`0` wyłącza źródło, np. do testów),
`MAX_WS_CLIENTS` (20 — limit połączeń WS na robota), `HOST` / `PORT` (tylko dla `python main.py`).

Testy (tylko 127.0.0.1, ok. 1 min; katalog `tests/` nie trafia do obrazu):

```bash
pip install -r requirements.txt pytest httpx websockets
pytest -q tests
```

## Wystawienie publicznie

Serwis `gateway` jest w `docker-compose.yml` (sieć `cerber_internal`) bez `ports:` — ruch z zewnątrz idzie
tylko przez reverse proxy. W `.env` stacku ustaw `GATEWAY_API_KEY` (bez niego kontener nie wystartuje) oraz `ROSBRIDGE_HOST`
(adres robota w VPN; domyślna wartość w `docker-compose.yml` jest tylko przykładem).

Konfiguracja dla Caddy 2 na **tej samej** publicznej nazwie hosta co frontend i backend (tu `cerber.example.org`),
**bez** list dostępu (zabezpieczeniem jest klucz API). Gateway i backend dzielą przestrzeń `/api/v1/*`, więc
Caddy rozdziela je po ścieżce: `/health`, `/api/v1/robots`, `/api/v1/robots/*`, `/docs` i `/openapi.json` idą do
gatewaya, pozostałe `/api/*` do backendu, reszta to SPA. Bloki `handle` są rozłączne; Caddy sortuje po długości tylko pojedyncze matchery `path`, a matcher
gatewaya ma kilka ścieżek, więc zostaje w kolejności z pliku — blok gatewaya musi stać przed `handle /api/*`
(kolejność tras można sprawdzić przez `caddy adapt`). Ścieżki nie są obcinane — gateway serwuje
dokładnie te adresy, które ma w kontrakcie. Upstream `cerber-staging-gateway:8000` to kontener
`${PROJECT_NAME}-gateway` z `docker-compose.yml`; w tym samym projekcie compose wystarczy nazwa serwisu
`gateway:8000`. Caddy musi być w sieci `cerber_internal`. Nazwę upstreamu Caddy rozwiązuje przy każdym żądaniu,
więc startuje i przeładowuje się także wtedy, gdy kontener gatewaya nie działa (odpowiada wtedy 502). Certyfikat
TLS dla hosta bierze sam z ACME. WebSocket działa w `reverse_proxy` bez dodatkowej konfiguracji (upgrade
przechodzi sam); `stream_close_delay` tylko opóźnia zerwanie otwartych WS po `caddy reload`.

WebSocket niesie klucz w query (`?api_key=…`). Bez dyrektywy `log` w bloku hosta Caddy nie pisze access logu
w ogóle. Jeśli go włączasz, dodaj filtr `query` na polu `request>uri`, który usuwa (`delete`) albo maskuje
(`replace`) `api_key` — jak niżej. Błędy obsługi żądania (np. 502, gdy gateway nie działa) Caddy pisze do
swojego domyślnego loggera (stderr, czyli `docker logs`) razem z `request>uri`, dlatego ten sam filtr jest też
w globalnym bloku `log` na początku Caddyfile.

Klienci REST wysyłają klucz **wyłącznie** w nagłówku `X-API-Key`, nigdy w query. Caddy loguje w access logu
wszystkie nagłówki żądania (`log_credentials` chroni tylko `Cookie` i `Authorization`), więc filtr usuwa też
`request>headers>X-Api-Key`. Gdyby któryś klient dopisał klucz do URL, filtr `query` obejmuje też ścieżki REST
(jeden `log` na cały host).

Sam gateway wycina ze swoich logów wartość `api_key` (także z zakodowaną nazwą lub separatorem, np.
`%61pi_key`, `api_key%3D`, `api_key%3A`, `"api_key": "…"`, `%22api_key%22%3A%22…`), nagłówek `X-API-Key`
(logi handshake'u na `--log-level debug`), wartości z błędów parsowania nagłówków, a także każde wystąpienie
samego klucza (klucz od 8 znaków) — dosłownie albo z dowolnymi znakami zakodowanymi jako `%XX` w dowolnej
wielkości liter (np. `%2b` z .NET), w ścieżce, query, `raw_path` na `--log-level trace` i w tracebackach.
Ramki WS na `--log-level debug` (od klienta i do klienta, także odesłany PING/CLOSE) są logowane tylko jako
typ i rozmiar, bez treści; na `trace` powód zamknięcia od klienta tylko jako długość.

```caddyfile
{
	# opcje globalne: domyślny logger Caddy'ego (stderr -> docker logs); tu trafiają m.in. błędy 502 z request>uri
	log {
		format filter {
			request>uri query {
				delete api_key
			}
			request>headers>X-Api-Key delete
		}
	}
}

cerber.example.org {
	# TLS: certyfikat dla tej nazwy Caddy pobiera sam z ACME

	# gateway telemetrii: konkretne ścieżki, przed ogólnym /api/*
	@gateway path /health /api/v1/robots /api/v1/robots/* /docs /openapi.json
	handle @gateway {
		# kontener ${PROJECT_NAME}-gateway w sieci cerber_internal; w tym samym projekcie compose wystarczy gateway:8000
		reverse_proxy cerber-staging-gateway:8000 {
			stream_close_delay 5m   # po `caddy reload` nie zrywaj otwartych WS od razu
		}
	}

	# pozostałe /api/* to backend (pełna ścieżka, bez obcinania)
	handle /api/* {
		reverse_proxy cerber-staging-backend:3000
	}

	# reszta: SPA frontendu
	handle {
		root * /srv/frontend-dist
		try_files {path} /index.html
		file_server
	}

	# bez tego bloku Caddy nie pisze access logu wcale; jeśli włączasz, usuń klucz z query (WS) i z nagłówka (REST)
	log {
		output file /var/log/caddy/cerber.log
		format filter {
			request>uri query {
				replace api_key REDACTED
			}
			request>headers>X-Api-Key delete
		}
	}
}
```

Strumienie wideo (WebRTC) są poza tym API: `/streams` zwraca tylko adresy WHEP. Domyślnie są to ścieżki
względne (`/video/<kamera>/whep`) na tym samym hoście co API — reverse proxy kieruje `/video/*` do MediaMTX;
`MEDIAMTX_BASE` (np. `https://host/video`) zamienia je na adresy bezwzględne. Flaga
`vpn_only` w odpowiedzi odzwierciedla `STREAMS_VPN_ONLY`, nie stan sieci — dostęp do samych strumieni
ogranicza konfiguracja proxy/firewalla.

## Znane luki (z analizy stocku Lite3)

- `battery_percent` jest zawsze `null`: stockowy node `jetson2motion` nie publikuje baterii. Gdy ROS doda
  publisher (`sensor_msgs/BatteryState`), wystarczy dopisać subskrypcję w `ROS_SUBSCRIPTIONS` i jedno zdarzenie.
- `ts` i `*_at` to czas odbioru przez gateway (epoch), bo `header.stamp` z robota jest monotoniczny.
- Jeden robot (`ROBOT_ID`); drugi = drugi wpis w `robots` i osobny rosbridge/MQTT.
- Jednostki w opisach pól (rad/s, m/s², rad dla stawów) są konwencją `sensor_msgs` — stockowy node przepisuje
  wartości z UDP bez konwersji (kąty RPY konwertuje ze stopni, prędkości już nie), więc warto je raz
  potwierdzić na robocie. `ultrasound` to element `[1]` tablicy `ultrasound[2]` z `RobotState`; który to
  fizycznie czujnik i w jakiej jednostce — źródła nie mówią.
