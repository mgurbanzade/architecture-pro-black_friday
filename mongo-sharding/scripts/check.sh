#!/bin/bash
###
# Проверка: общее количество документов и количество документов на каждом шарде.
###
set -euo pipefail
cd "$(dirname "$0")/.."

count() {
  docker compose exec -T "$1" mongosh --port "$2" --quiet \
    --eval 'db.getSiblingDB("somedb").helloDoc.countDocuments()'
}

echo "Всего документов (mongos_router): $(count mongos_router 27020)"
echo "Документов на shard1:             $(count shard1 27018)"
echo "Документов на shard2:             $(count shard2 27019)"
