#!/bin/bash
###
# Проверка кеширования: первый вызов /helloDoc/users идёт в MongoDB (~1 с),
# повторные отдаются из Redis (< 100 мс). TTL кеша - 60 с.
###
set -euo pipefail
cd "$(dirname "$0")/.."

URL="${APP_URL:-http://localhost:8080}/helloDoc/users"

docker compose exec -T redis redis-cli FLUSHALL >/dev/null
for i in 1 2 3 4 5; do
  curl -s -o /dev/null -w "Вызов $i: %{time_total} c\n" "$URL"
done
echo "Ключи в Redis: $(docker compose exec -T redis redis-cli --scan --pattern 'api:cache*' | tr '\n' ' ')"
