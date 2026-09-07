#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
bin="$root/build/tettix"
f="$root/tests/parquet/example.parquet"
gold="$root/tests/parquet"

extract_table() {
  awk '/^bytes_read\t/{exit} p{print} $0=="id\tname\tage\tcity" || $0=="id\tage" || $0=="id" || $0=="name"{p=1; print}'
}

fail() { echo "FAIL: $*" >&2; exit 1; }

out="$("$bin" dump "$f")"
echo "$out" | extract_table | diff -u "$gold/example.table.txt" - || fail "dump table"
echo "$out" | grep -qx 'bytes_read	594' || fail "dump should read whole file, got: $(echo "$out" | grep bytes_read)"

out="$("$bin" scan "$f" --col id)"
echo "$out" | extract_table | diff -u "$gold/example.id.txt" - || fail "scan --col id table"
echo "$out" | grep -qx 'bytes_read	445' || fail "scan --col id IO, got: $(echo "$out" | grep bytes_read)"

out="$("$bin" scan "$f" --col id --col age)"
echo "$out" | extract_table | diff -u "$gold/example.id-age.txt" - || fail "scan id,age table"

out="$("$bin" scan "$f" --col id --gt 1)"
echo "$out" | extract_table | diff -u "$gold/example.id-gt1.txt" - || fail "scan --gt 1 table"

echo OK
