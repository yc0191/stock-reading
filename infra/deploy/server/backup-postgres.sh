#!/bin/bash
# 데이터 서버에서 Postgres 를 pg_dump 로 내려받아 S3 에 올린다(매주 일요일 새벽, stock-backup.timer).
# 내려받기가 끝까지 성공한 파일만 올리려고 먼저 서버 안 임시 파일에 쓴다. 중간에 끊긴 덤프가 올라가지 않는다.
# 설정: BACKUP_S3_BUCKET(필수), BACKUP_S3_PREFIX, POSTGRES_DB, POSTGRES_ADMIN_USER, AWS_REGION. 시험용: BACKUP_NOW(ISO 8601).
set -euo pipefail

bucket=${BACKUP_S3_BUCKET:?BACKUP_S3_BUCKET 이 필요합니다}
prefix=${BACKUP_S3_PREFIX:-postgres}
db=${POSTGRES_DB:-stock}
admin=${POSTGRES_ADMIN_USER:-postgres}
container=${POSTGRES_CONTAINER:-stock-postgres}

if [ -n "${BACKUP_NOW:-}" ]; then
  stamp=$(TZ=Asia/Seoul date -d "$BACKUP_NOW" +%Y%m%d-%H%M)
else
  stamp=$(TZ=Asia/Seoul date +%Y%m%d-%H%M)
fi

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
file="$work/${db}-${stamp}.dump"

echo "$(date -u +%FT%TZ) backup: pg_dump 시작 ($db)"
# 컨테이너 안의 소켓 연결로 돌려 비밀번호를 명령줄에 쓰지 않는다.
docker exec "$container" pg_dump -U "$admin" -d "$db" --format=custom >"$file"
[ -s "$file" ] || { echo "backup: 덤프 파일이 비어 있음" >&2; exit 1; }
size=$(wc -c <"$file" | tr -d ' ')

aws s3 cp "$file" "s3://${bucket}/${prefix}/$(basename "$file")" ${AWS_REGION:+--region "$AWS_REGION"} --only-show-errors
echo "$(date -u +%FT%TZ) backup: 업로드 끝 ($(basename "$file"), ${size} bytes)"
