#!/bin/bash
# Run as root on the dedicated disposable Debian test host after building bin/.
set -euo pipefail
if [[ $EUID -ne 0 ]]; then echo 'Run with sudo on the authorized test host.' >&2; exit 1; fi
source_dir=$(cd "$(dirname "$0")/.." && pwd)
state_dir=/var/lib/bsos-test
install -d -m 0755 /usr/local/lib/bsos-test "$state_dir" "$state_dir/snapshots"
install -m 0755 "$source_dir/bin/bsos" "$source_dir/bin/bsosd" /usr/local/lib/bsos-test/
modprobe zram
# An ordinary backing file complements zram, so larger objects exercise the
# non-zram tier without using any part of the host system block device.
if [[ ! -e "$state_dir/disk.img" ]]; then
    truncate -s 384M "$state_dir/disk.img"
    /usr/local/lib/bsos-test/bsos blk init --yes --id 0x42534F530001 "$state_dir/disk.img"
fi
if [[ ! -e "$state_dir/pan.json" ]]; then
    /usr/local/lib/bsos-test/bsos zram create --id 0x42534F530002 320M
    python3 - <<'PY'
import glob,json,struct
from pathlib import Path
state=Path('/var/lib/bsos-test')
for name in glob.glob('/dev/zram[0-9]*'):
    try:
        with open(name,'rb',buffering=0) as f: header=f.read(4096)
        if len(header)>=16 and header[:4]==b'NBSS' and struct.unpack_from('<Q',header,8)[0]==0x42534F530002:
            zram=name
            break
    except OSError:
        continue
else:
    raise SystemExit('Created zram identity not found')
pool={'version':1,'devices':[
    {'device_path':str(state/'disk.img'),'disk_id':'0x42534F530001'},
    {'device_path':zram,'disk_id':'0x42534F530002','size_bytes':320*1024*1024,'status':'match'}]}
(state/'pan.json').write_text(json.dumps(pool,indent=2)+'\n')
PY
fi
cat > /etc/modules-load.d/bsos-test.conf <<'CONFIG'
zram
CONFIG
cat > "$state_dir/bsosd.toml" <<'TOML'
pan_path = "/var/lib/bsos-test/pan.json"
grpc_listen = "127.0.0.1:19090"
max_put = "256MB"
small_file_pow2 = 6
zram_snapshot_dir = "/var/lib/bsos-test/snapshots"
reservation_stall_timeout_ms = 30000
pool_gate_fanout_timeout_ms = 2000
write_dispatch_concurrency = 64
chd_target_p = 0.2
trim_interval_seconds = 900
trim_min_file_count = 100
trim_threshold_ratio = 0.2
trim_min_threshold_bytes = "1MB"
trim_max_threshold_bytes = "16MB"
TOML
cat > /etc/systemd/system/bsos-test.service <<'UNIT'
[Unit]
Description=BSOS integration test daemon
After=systemd-modules-load.service

[Service]
Type=simple
WorkingDirectory=/var/lib/bsos-test
ExecStart=/usr/local/lib/bsos-test/bsosd -config /var/lib/bsos-test/bsosd.toml
Restart=on-failure
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload
systemctl enable --now bsos-test.service
systemctl is-active bsos-test.service
