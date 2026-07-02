#!/bin/sh
set -u

DST="srt://172.30.0.10:7001?mode=caller&pkt_size=1316"
FONT="/usr/share/fonts/truetype/dejavu/DejaVuSansMono-Bold.ttf"

while true; do
  echo "[producer] streaming to $DST"
  ffmpeg -hide_banner -loglevel warning \
    -re -f lavfi -i "testsrc2=size=1280x720:rate=30" \
    -f lavfi -i "sine=frequency=1000:sample_rate=48000" \
    -vf "drawtext=fontfile=${FONT}:text='TC %{pts\:hms}  N %{n}':fontcolor=white:fontsize=40:x=30:y=30:box=1:boxcolor=black@0.6" \
    -c:v libx264 -preset veryfast -tune zerolatency -b:v 2000k -maxrate 2000k -bufsize 1000k -g 30 -pix_fmt yuv420p \
    -c:a aac -b:a 128k -ar 48000 \
    -f mpegts "$DST"
  echo "[producer] ffmpeg exited ($?); reconnecting in 3s"
  sleep 3
done
