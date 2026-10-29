# mongo-sharding - задание 2. Шардирование MongoDB

## Состав стенда

| Сервис | Роль | Порт |
|---|---|---|
| `configSrv` | сервер конфигурации, replica set `config_server` | 27017 |
| `shard1` | шард 1, replica set `shard1` | 27018 |
| `shard2` | шард 2, replica set `shard2` | 27019 |
| `mongos_router` | роутер mongos, единая точка входа в кластер | 27020 |
| `pymongo_api` | приложение `kazhem/pymongo_api:1.0.0` | 8080 |

- База: `somedb`
- Коллекция: `helloDoc`
- Шард-ключ: `{ name: "hashed" }`, хешированное шардирование для равномерного распределения.

## Быстрый запуск

```shell
docker compose up -d
./scripts/mongo-init.sh
```

Скрипт инициализирует кластер, вставляет 1000 документов и выводит, сколько документов на каждом шарде. Повторный запуск безопасен.

Проверка в любой момент:

```shell
./scripts/check.sh
```

## Шаги инициализации вручную

То же, что делает `scripts/mongo-init.sh`.

**1. Сервер конфигурации**

```shell
docker compose exec -T configSrv mongosh --port 27017 --quiet <<EOF
rs.initiate({
  _id: "config_server",
  configsvr: true,
  members: [{ _id: 0, host: "configSrv:27017" }]
})
EOF
```

**2. Шарды**

```shell
docker compose exec -T shard1 mongosh --port 27018 --quiet <<EOF
rs.initiate({ _id: "shard1", members: [{ _id: 0, host: "shard1:27018" }] })
EOF

docker compose exec -T shard2 mongosh --port 27019 --quiet <<EOF
rs.initiate({ _id: "shard2", members: [{ _id: 0, host: "shard2:27019" }] })
EOF
```

**3. Роутер: регистрация шардов и шардирование коллекции**

```shell
docker compose exec -T mongos_router mongosh --port 27020 --quiet <<EOF
sh.addShard("shard1/shard1:27018")
sh.addShard("shard2/shard2:27019")
sh.enableSharding("somedb")
sh.shardCollection("somedb.helloDoc", { name: "hashed" })
EOF
```

**4. Наполнение данными**

```shell
docker compose exec -T mongos_router mongosh --port 27020 --quiet <<EOF
use somedb
for (var i = 0; i < 1000; i++) db.helloDoc.insertOne({ age: i, name: "ly" + i })
EOF
```

## Проверка

**Общее количество документов**

```shell
docker compose exec -T mongos_router mongosh --port 27020 --quiet <<EOF
use somedb
db.helloDoc.countDocuments()
EOF
```

**Количество документов на каждом шарде**

```shell
docker compose exec -T shard1 mongosh --port 27018 --quiet <<EOF
use somedb
db.helloDoc.countDocuments()
EOF

docker compose exec -T shard2 mongosh --port 27019 --quiet <<EOF
use somedb
db.helloDoc.countDocuments()
EOF
```

Ожидаемо: всего 1000, на shard1 ~492, на shard2 ~508.

**Приложение**

http://localhost:8080 - JSON с `mongo_topology_type: "Sharded"`, `collections.helloDoc.documents_count: 1000` и списком шардов в `shards`.

## Остановка

```shell
docker compose down -v
```
