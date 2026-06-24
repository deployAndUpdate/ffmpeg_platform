#!/bin/sh
set -e

# Named volume монтируется от root — даём app права на запись выходных файлов ffmpeg.
if [ -d /data ]; then
    chown -R app:app /data 2>/dev/null || true
fi

mkdir -p /tmp/jobs
chown app:app /tmp/jobs 2>/dev/null || true

exec su-exec app /usr/local/bin/worker
