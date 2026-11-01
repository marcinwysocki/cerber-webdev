#!/bin/sh
# Zrodlo strumienia demo dla sciezki $MTX_PATH (np. video/dji01/demo), uruchamiane przez MediaMTX (runOnInit).
# Gdy istnieje /media/<nazwa>.mp4 (np. dji01.mp4 dla video/dji01/demo), leci w petli; inaczej obraz testowy.
# DEMO_STREAMS=no wylacza demo bez zmiany konfiguracji (proces tylko spi, zeby runOnInitRestart go nie wznawial).
set -u
if [ "${DEMO_STREAMS:-yes}" != "yes" ]; then
  exec sleep infinity
fi
name=$(basename "$(dirname "$MTX_PATH")")   # video/dji01/demo -> dji01
file="/media/$name.mp4"
dst="srt://127.0.0.1:8890?streamid=publish:$MTX_PATH&pkt_size=1316"
common="-c:v libx264 -preset ultrafast -tune zerolatency -g 50 -pix_fmt yuv420p -b:v 1200k -an -f mpegts"
if [ -f "$file" ]; then
  # nagranie w petli; -re = tempo rzeczywiste
  exec ffmpeg -nostdin -loglevel warning -re -stream_loop -1 -i "$file" $common "$dst"
else
  # obraz testowy, inny wzor dla kazdej sciezki, zeby bylo widac ktore demo to ktore
  case "$name" in
    dji01) src="testsrc2=size=1280x720:rate=25" ;;
    *)     src="smptehdbars=size=1280x720:rate=25" ;;
  esac
  exec ffmpeg -nostdin -loglevel warning -re -f lavfi -i "$src" $common "$dst"
fi
