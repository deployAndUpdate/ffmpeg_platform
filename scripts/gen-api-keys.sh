#!/usr/bin/env bash
# Generate cryptographically random scheduler API keys.
# Print to stdout — copy into .env (dev) or a secrets manager (prod). Do not commit prod keys.

set -euo pipefail

FORMAT="plain"
BYTES=24

usage() {
	cat <<'EOF'
Usage: gen-api-keys.sh [--format=plain|env|export] [--bytes=N]

Generates SCHEDULER_CLIENT_API_KEY, SCHEDULER_WORKER_API_KEY and
SCHEDULER_ADMIN_API_KEY using openssl rand.

Formats:
  plain   labeled output (default)
  env     KEY=value lines for .env
  export  export KEY="value" lines (for source .env)

Examples:
  ./scripts/gen-api-keys.sh
  ./scripts/gen-api-keys.sh --format=env >> .env
EOF
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--format=*)
		FORMAT="${1#*=}"
		;;
	--bytes=*)
		BYTES="${1#*=}"
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		echo "unknown argument: $1" >&2
		usage >&2
		exit 1
		;;
	esac
	shift
done

case "$FORMAT" in
plain | env | export) ;;
*)
	echo "invalid --format=$FORMAT (expected plain, env, or export)" >&2
	exit 1
	;;
esac

if ! command -v openssl >/dev/null 2>&1; then
	echo "openssl is required but not found in PATH" >&2
	exit 1
fi

if ! [[ "$BYTES" =~ ^[0-9]+$ ]] || [[ "$BYTES" -lt 16 ]]; then
	echo "--bytes must be an integer >= 16" >&2
	exit 1
fi

rand_hex() {
	openssl rand -hex "$BYTES"
}

client_key="sk_client_$(rand_hex)"
worker_key="sk_worker_$(rand_hex)"
admin_key="sk_admin_$(rand_hex)"

print_pair() {
	local name="$1"
	local value="$2"
	case "$FORMAT" in
	plain)
		printf '%s=%s\n' "$name" "$value"
		;;
	env)
		printf '%s=%s\n' "$name" "$value"
		;;
	export)
		printf 'export %s="%s"\n' "$name" "$value"
		;;
	esac
}

if [[ "$FORMAT" == "plain" ]]; then
	echo "# Scheduler API keys — store in secrets; do not commit production values." >&2
	echo "# Auth: set SCHEDULER_API_KEY_REQUIRED=true on scheduler and worker." >&2
	echo >&2
fi

print_pair "SCHEDULER_CLIENT_API_KEY" "$client_key"
print_pair "SCHEDULER_WORKER_API_KEY" "$worker_key"
print_pair "SCHEDULER_ADMIN_API_KEY" "$admin_key"

if [[ "$FORMAT" == "plain" ]]; then
	echo >&2
	echo "# Optional (scheduler only):" >&2
	echo "SCHEDULER_API_KEY_REQUIRED=true" >&2
fi
