# MediaMTX — wideo

Jeden serwer MediaMTX (`mediamtx` w `docker-compose.yml`, konfiguracja w `mediamtx.yml`) przyjmuje strumienie od
nadawców i rozdaje je przeglądarkom przez WebRTC (WHEP). Nic nie nagrywa i nie transkoduje.

```
robot (kamery)  --SRT :8890 (tylko VPN, bez hasła)-->  MediaMTX  --WHEP /video/<ścieżka>/whep--> Caddy 443 --> przeglądarka
dron DJI        --RTMP :1935 (internet, login+hasło)-->          --RTSP :8554 (opcjonalnie, TCP)--> ffplay / VLC
```

## Ścieżki

| Ścieżka | Nadawca | Wejście | Publikowanie |
|---|---|---|---|
| `video/normal_cam` | robot | SRT | bez hasła; port SRT jest dostępny tylko z VPN (firewall serwera) |
| `video/IR_cam` | robot | SRT | jak wyżej |
| `video/dji01` | dron DJI Mavic 3M (operator w terenie) | RTMP | login i hasło z `.env` |

Odczyt każdej ścieżki jest publiczny (decyzja z 2026-10-31, jak telemetria): przeglądarka nie potrafi trzymać
sekretu, a adresy i tak są w bundlu frontendu. Prefiks `video/` jest wspólny, bo reverse proxy kieruje `/video/*`
do MediaMTX; dzięki temu nowa ścieżka nie wymaga zmian w Caddy.

## Adresy

Dla hosta `cerber.example.org` (przykład) i domyślnych portów:

| Co | Adres |
|---|---|
| nadawanie z drona (RTMP) | `rtmp://cerber.example.org:1935/video/dji01?user=<DJI_PUBLISH_USER>&pass=<DJI_PUBLISH_PASS>` |
| to samo jako URL + klucz strumienia (aplikacje DJI/OBS z dwoma polami) | URL `rtmp://cerber.example.org:1935/video`, klucz `dji01?user=…&pass=…` |
| odtwarzanie w przeglądarce (wbudowany odtwarzacz MediaMTX) | `https://cerber.example.org/video/dji01/` |
| WHEP (własny odtwarzacz, np. `WebRTCCamera.jsx`) | `POST https://cerber.example.org/video/dji01/whep` |
| RTSP do diagnostyki (gdy `RTSP_ENABLE=yes`) | `rtsp://cerber.example.org:18554/video/dji01` (tylko TCP) |

Login i hasło idą w query RTMP, bo aplikacja DJI ma jedno pole na adres. MediaMTX nie loguje query, więc hasło
nie trafia do `docker logs`. Nie przesyłaj tego adresu kanałem, który zostaje w historii (np. publiczny czat).

## Zmienne w `.env`

| Zmienna | Znaczenie | Domyślnie |
|---|---|---|
| `SRT_PORT`, `WEBRTC_PORT`, `STREAM_PORT`, `WEBRTC_ADDITIONAL_HOSTS` | jak dotąd (SRT, sygnalizacja WHEP, port ICE 1:1, publiczny adres dla ICE) | wymagane |
| `DJI_PUBLISH_USER`, `DJI_PUBLISH_PASS` | login i hasło nadawcy na `video/dji01`; trafiają do wpisu nr 2 `authInternalUsers` przez `MTX_AUTHINTERNALUSERS_2_*` | **wymagane** — compose odmawia startu bez nich |
| `RTMP_PORT` | port RTMP na hoście | `1935` |
| `RTSP_ENABLE` | `yes` włącza nasłuch RTSP (odczyt, tylko TCP) | `no` |
| `RTSP_PORT` | port RTSP na hoście; mapowany zawsze, ale przy `RTSP_ENABLE=no` nic w kontenerze nie słucha | `18554` |

Hasło: coś losowego, np. `openssl rand -base64 24`. Wartości w `mediamtx.yml` (`dji` / `set-in-env`) to zaślepki,
które nadpisuje compose; sprawdzono, że bez nadpisania strumień z tym hasłem jest odrzucany.

## Firewall serwera

Do tej pory z internetu otwarte były tylko porty Caddy i port ICE (`STREAM_PORT`, TCP+UDP). Dron nadaje z LTE,
więc **`RTMP_PORT`/TCP musi być otwarty publicznie** — stąd hasło. `RTSP_PORT`/TCP otwieraj tylko, gdy RTSP ma być
używane spoza serwera. SRT zostaje zamknięte dla świata (robot nadaje przez VPN). MediaMTX nie ma ochrony przed
zgadywaniem haseł; nieudane próby widać w logu jako `failed to authenticate`.

## Testy bez drona

Z dowolnej maszyny z Dockerem (lub lokalnym `ffmpeg`, wtedy bez `docker run … linuxserver/ffmpeg:latest`):

```bash
# nadawanie: kolorowe pasy + ton, 720p30, GOP 2 s (taki sam jak zalecany dla drona)
docker run --rm linuxserver/ffmpeg:latest -re -f lavfi -i testsrc2=size=1280x720:rate=30 -f lavfi -i sine=frequency=440 \
  -c:v libx264 -preset ultrafast -tune zerolatency -g 60 -pix_fmt yuv420p -b:v 2000k -c:a aac \
  -f flv "rtmp://cerber.example.org:1935/video/dji01?user=$DJI_PUBLISH_USER&pass=$DJI_PUBLISH_PASS"
```

```bash
# odczyt RTSP (gdy włączone)
docker run --rm linuxserver/ffmpeg:latest -rtsp_transport tcp -i rtsp://cerber.example.org:18554/video/dji01 -frames:v 30 -f null -
```

Potem `https://cerber.example.org/video/dji01/` w przeglądarce. OBS: Ustawienia → Stream → Własny serwer, adres i
klucz z tabeli wyżej.

Zweryfikowane lokalnie (MediaMTX v1.21.3): RTMP z poprawnym hasłem publikuje; złe hasło, zaślepka z `mediamtx.yml`
i brak loginu dostają `authentication failed`; SRT bez hasła nadal publikuje na `video/normal_cam`, ale nie na
`video/dji01`; przy `RTSP_ENABLE=no` port 8554 odmawia połączeń, przy `yes` ffmpeg czyta H264+AAC po TCP;
przeglądarka odtwarza `video/dji01` przez WHEP.

## Ograniczenia

- **Bez dźwięku w przeglądarce.** DJI wysyła audio AAC, którego WebRTC nie przenosi; MediaMTX pomija tę ścieżkę
  (`skipping track … MPEG-4 Audio`) i wideo gra normalnie. RTSP ma audio. Gdyby dźwięk w przeglądarce był potrzebny,
  trzeba włączyć HLS (`hls: yes`, port 8888, osobna trasa w Caddy) kosztem kilku sekund opóźnienia.
- **RTMPS nie jest skonfigurowane.** MediaMTX umie (`rtmpEncryption`, port 1936), ale potrzebuje własnego certyfikatu
  i klucza w kontenerze. Jeśli operator drona wymaga RTMPS, trzeba to dołożyć.
- **Parametry strumienia z drona:** H.264, odstęp klatek kluczowych do 2 s (dłuższy opóźnia start obrazu w WebRTC),
  bez B-klatek. Bitrate według łącza LTE operatora.
