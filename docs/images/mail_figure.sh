#!/usr/bin/env bash
# README의 메일 그림(mail-move.png, mail-post-close.png)을 다시 만든다. 저장소 루트에서 실행한다.
# 필요: 브리핑 venv, 헤드리스 Chrome(CHROME 환경변수로 실행 파일 지정), ImageMagick(magick)
set -euo pipefail
CHROME=${CHROME:-google-chrome}
for cmd in "$CHROME" magick; do
  command -v "$cmd" >/dev/null || { echo "필요한 명령이 없음: $cmd" >&2; exit 1; }
done
OUT=docs/images
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

services/briefing/.venv/bin/python "$OUT/mail_example.py" "$WORK"
for name in move post-close; do
  "$CHROME" --headless=new --disable-gpu --hide-scrollbars --force-device-scale-factor=2 --window-size=720,4000 \
    --screenshot="$WORK/$name.png" "file://$WORK/mail-$name.html" >/dev/null
  # 아래 빈 곳을 잘라 내고 둘레에 여백을 둔다.
  magick "$WORK/$name.png" -trim -bordercolor white -border 24 -strip "$OUT/mail-$name.png"
done
