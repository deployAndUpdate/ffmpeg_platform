#!/usr/bin/env bash
# Submit N transcoding jobs via POST /jobs (multipart upload).
# Requires R2 on scheduler and curl; jq is optional (pretty job id output).

set -euo pipefail

SCHEDULER_URL="${SCHEDULER_URL:-http://localhost:8080}"
PRESET="${PRESET:-h264_crf23}"
OUTPUT_EXT="${OUTPUT_EXT:-mp4}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

VIDEO_FILE="$ROOT_DIR/testdata/sample.mp4"
if [[ ! -f "$VIDEO_FILE" ]]; then
	VIDEO_FILE="$ROOT_DIR/testdata/fixtures/sample.mp4"
fi

usage() {
	cat <<'EOF'
Usage: submit-jobs.sh

Interactive script: prompts for client API key and job count N,
then uploads testdata/sample.mp4 (or testdata/fixtures/sample.mp4) N times.

Environment:
  SCHEDULER_URL   scheduler base URL (default: http://localhost:8080)
  PRESET          transcode preset (default: h264_crf23)
  OUTPUT_EXT      output extension (default: mp4)

Requires scheduler R2 object storage (multipart POST /jobs).
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
	usage
	exit 0
fi

read -rsp "Client API key (SCHEDULER_CLIENT_API_KEY): " API_KEY
echo
read -rp "Количество запросов N: " N

if [[ -z "$API_KEY" ]]; then
	echo "Ошибка: API key не может быть пустым" >&2
	exit 1
fi

if ! [[ "$N" =~ ^[1-9][0-9]*$ ]]; then
	echo "Ошибка: N должно быть положительным целым числом" >&2
	exit 1
fi

if [[ ! -f "$VIDEO_FILE" ]]; then
	echo "Ошибка: файл не найден: $VIDEO_FILE" >&2
	exit 1
fi

echo "Scheduler:  $SCHEDULER_URL"
echo "Файл:       $VIDEO_FILE"
echo "Preset:     $PRESET"
echo "Output ext: $OUTPUT_EXT"
echo "Отправка $N job(s)..."
echo

ok=0
fail=0

for ((i = 1; i <= N; i++)); do
	echo "[$i/$N] POST /jobs ..."

	response="$(curl -sS -w "\n%{http_code}" -X POST "$SCHEDULER_URL/jobs" \
		-H "Authorization: Bearer $API_KEY" \
		-F "file=@${VIDEO_FILE}" \
		-F "preset=${PRESET}" \
		-F "output_ext=${OUTPUT_EXT}")"

	http_code="${response##*$'\n'}"
	body="${response%$'\n'*}"

	if [[ "$http_code" == "201" ]]; then
		ok=$((ok + 1))
		if command -v jq >/dev/null 2>&1; then
			job_id="$(echo "$body" | jq -r '.id // empty')"
			status="$(echo "$body" | jq -r '.status // empty')"
			echo "  OK  HTTP $http_code  id=$job_id  status=$status"
		else
			echo "  OK  HTTP $http_code"
			echo "  $body"
		fi
	else
		fail=$((fail + 1))
		echo "  FAIL HTTP $http_code" >&2
		echo "  $body" >&2

		if [[ "$http_code" == "503" && "$body" == *"object storage is not configured"* ]]; then
			echo "  Подсказка: на scheduler не настроен R2 (multipart upload недоступен)." >&2
		fi
		if [[ "$http_code" == "401" || "$http_code" == "403" ]]; then
			echo "  Подсказка: проверьте SCHEDULER_CLIENT_API_KEY." >&2
		fi
	fi
done

echo
echo "Готово: успешно=$ok, ошибок=$fail"
[[ "$fail" -eq 0 ]]
