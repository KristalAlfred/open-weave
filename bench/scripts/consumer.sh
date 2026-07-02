#!/bin/sh
set -u

SRC="srt://172.31.0.10:7003?mode=caller"

while true; do
  echo "[consumer] pulling from $SRC"
  ffmpeg -hide_banner -loglevel info -stats -fflags nobuffer \
    -i "$SRC" \
    -an -vf "freezedetect=n=0.003:d=0.5,blackdetect=d=0.5:pic_th=0.98" \
    -f null - 2>&1
  echo "[consumer] ffmpeg exited ($?); reconnecting in 3s"
  sleep 3
done
