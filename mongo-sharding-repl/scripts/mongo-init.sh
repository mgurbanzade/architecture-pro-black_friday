#!/bin/bash
###
# Инициализация шардированного кластера MongoDB с репликацией (по 3 реплики на шард)
# и наполнение данными.
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

# Ждём, пока в replica set появится PRIMARY
wait_primary() {
  echo "  ждём PRIMARY в replica set узла $1:$2..."
  until docker compose exec -T "$1" mongosh --port "$2" --quiet \
      --eval "db.hello().primary ? 'yes' : 'no'" 2>/dev/null | grep -q yes; do
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

echo "2/4 Replica set для каждого шарда (3 реплики)"
for n in shard1-1 shard1-2 shard1-3; do wait_up "$n" 27018; done
mongo_exec shard1-1 27018 <<'EOF'
try {
  rs.initiate({
    _id: "shard1",
    members: [
      { _id: 0, host: "shard1-1:27018" },
      { _id: 1, host: "shard1-2:27018" },
      { _id: 2, host: "shard1-3:27018" }
    ]
  });
  print("shard1: OK");
} catch (e) { print("shard1: " + e.codeName); }
EOF

for n in shard2-1 shard2-2 shard2-3; do wait_up "$n" 27019; done
mongo_exec shard2-1 27019 <<'EOF'
try {
  rs.initiate({
    _id: "shard2",
    members: [
      { _id: 0, host: "shard2-1:27019" },
      { _id: 1, host: "shard2-2:27019" },
      { _id: 2, host: "shard2-3:27019" }
    ]
  });
  print("shard2: OK");
} catch (e) { print("shard2: " + e.codeName); }
EOF
wait_primary shard1-1 27018
wait_primary shard2-1 27019

echo "3/4 Роутер: addShard + shardCollection"
wait_up mongos_router 27020
mongo_exec mongos_router 27020 <<'EOF'
sh.addShard("shard1/shard1-1:27018,shard1-2:27018,shard1-3:27018");
sh.addShard("shard2/shard2-1:27019,shard2-2:27019,shard2-3:27019");
sh.enableSharding("somedb");
sh.shardCollection("somedb.helloDoc", { name: "hashed" });
db.adminCommand({ listShards: 1 }).shards.forEach(s => print(s._id + ": " + s.host));
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
