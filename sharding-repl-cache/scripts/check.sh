#!/bin/bash
###
# Проверка: общее количество документов, количество документов на каждом шарде
# и состав реплик каждого шарда.
###
set -euo pipefail
cd "$(dirname "$0")/.."

count() {
  docker compose exec -T "$1" mongosh --port "$2" --quiet \
    --eval 'db.getSiblingDB("somedb").helloDoc.countDocuments()'
}

replicas() {
  docker compose exec -T "$1" mongosh --port "$2" --quiet --eval '
    const s = rs.status();
    print("  " + s.set + ": " + s.members.length + " реплики");
    s.members.forEach(m => print("    " + m.name + "  " + m.stateStr + "  docs=" +
      new Mongo(m.name).getDB("somedb").helloDoc.countDocuments()));'
}

echo "Всего документов (mongos_router): $(count mongos_router 27020)"
echo "Документов на shard1:             $(count shard1-1 27018)"
echo "Документов на shard2:             $(count shard2-1 27019)"
echo
echo "Реплики:"
replicas shard1-1 27018
replicas shard2-1 27019
