#!/bin/bash
# 서버를 처음 만든 뒤 한 번 돌려 스왑 파일(기본 1GB)을 만든다. 메모리가 모자랄 때 프로세스가 강제 종료되는 대신 느려지게 하는 완충이다.
#   sudo setup-swap.sh [크기 MB, 기본 1024]
# 이미 스왑이 켜져 있으면 아무것도 하지 않는다(여러 번 돌려도 같다). 서비스 서버·데이터 서버 모두에서 쓴다.
set -euo pipefail

size_mb=${1:-1024}
swap_file=${SWAP_FILE:-/swapfile}

[[ "$size_mb" =~ ^[0-9]+$ ]] && [ "$size_mb" -gt 0 ] || { echo "사용법: $0 [크기 MB]" >&2; exit 64; }
[ "$(id -u)" -eq 0 ] || { echo "root 로 실행해야 합니다" >&2; exit 1; }

if swapon --show --noheadings | grep -q .; then
  echo "이미 스왑이 켜져 있어 아무것도 하지 않음"
  exit 0
fi

if [ ! -f "$swap_file" ]; then
  # 임시 이름으로 끝까지 만든 뒤 이름을 바꾼다. 중간에 끊겨도 반쯤 찬 파일이 스왑 이름으로 남지 않는다.
  rm -f "$swap_file.new"
  dd if=/dev/zero of="$swap_file.new" bs=1M count="$size_mb" status=none
  chmod 600 "$swap_file.new"
  mkswap "$swap_file.new" >/dev/null
  mv "$swap_file.new" "$swap_file"
fi
swapon "$swap_file"
# 재부팅 뒤에도 켜지게 한다.
grep -q "^$swap_file " /etc/fstab || echo "$swap_file none swap sw 0 0" >>/etc/fstab
# 스왑은 비상용이므로 메모리에 여유가 있을 때는 거의 쓰지 않게 한다.
echo 'vm.swappiness=10' >/etc/sysctl.d/99-stock-reading-swap.conf
sysctl -q -p /etc/sysctl.d/99-stock-reading-swap.conf
echo "스왑 ${size_mb}MB 켬: $swap_file"
