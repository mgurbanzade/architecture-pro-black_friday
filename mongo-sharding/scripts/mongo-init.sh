#!/bin/bash
###
# Инициализация шардированного кластера MongoDB и наполнение данными.
# Запуск: ./scripts/mongo-init.sh (после docker compose up -d)
###
set -euo pipefail
cd "$(dirname "$0")/.."

# Выполнить mongosh-скрипт из stdin на сервисе: mongo_exec <service> <port> <<'EOF' ... EOF
mongo_exec() {
  docker compose exec -T "$1" mongosh --port "$2" --quiet --eval "$(cat)"
}

# Ждём, пока узел ответит на ping
wait_up() {
  echo "  ждём $1:$2..."
  until docker compose exec -T "$1" mongosh --port "$2" --quiet \
      --eval "db.adminCommand('ping').ok" >/dev/null 2>&1; do
    sleep 1
  done
}

# Ждём, пока узел станет PRIMARY своего replica set
wait_primary() {
  echo "  ждём PRIMARY на $1:$2..."
  until docker compose exec -T "$1" mongosh --port "$2" --quiet \
      --eval "db.hello().isWritablePrimary" 2>/dev/null | grep -q true; do
    sleep 1
  done
}

echo "1/4 Сервер конфигурации: rs.initiate"
wait_up configSrv 27017
mongo_exec configSrv 27017 <<'EOF'
try {
  rs.initiate({
    _id: "config_server",
    configsvr: true,
    members: [{ _id: 0, host: "configSrv:27017" }]
  });
  print("config_server: OK");
} catch (e) { print("config_server: " + e.codeName); }
EOF
wait_primary configSrv 27017

echo "2/4 Шарды: rs.initiate"
wait_up shard1 27018
mongo_exec shard1 27018 <<'EOF'
try {
  rs.initiate({ _id: "shard1", members: [{ _id: 0, host: "shard1:27018" }] });
  print("shard1: OK");
} catch (e) { print("shard1: " + e.codeName); }
EOF
wait_up shard2 27019
mongo_exec shard2 27019 <<'EOF'
try {
  rs.initiate({ _id: "shard2", members: [{ _id: 0, host: "shard2:27019" }] });
  print("shard2: OK");
} catch (e) { print("shard2: " + e.codeName); }
EOF
wait_primary shard1 27018
wait_primary shard2 27019

echo "3/4 Роутер: addShard + shardCollection"
wait_up mongos_router 27020
mongo_exec mongos_router 27020 <<'EOF'
sh.addShard("shard1/shard1:27018");
sh.addShard("shard2/shard2:27019");
sh.enableSharding("somedb");
sh.shardCollection("somedb.helloDoc", { name: "hashed" });
print("Шарды: " + db.adminCommand({ listShards: 1 }).shards.map(s => s._id).join(", "));
EOF

echo "4/4 Наполнение данными"
mongo_exec mongos_router 27020 <<'EOF'
const coll = db.getSiblingDB("somedb").helloDoc;
if (coll.countDocuments() === 0) {
  const docs = [];
  for (let i = 0; i < 1000; i++) docs.push({ age: i, name: "ly" + i });
  coll.insertMany(docs);
}
print("Вставлено документов: " + coll.countDocuments());
EOF

echo
./scripts/check.sh
