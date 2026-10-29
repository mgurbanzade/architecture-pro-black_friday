# mongo-sharding-repl - задание 3. Шардирование + репликация MongoDB

## Состав стенда

| Сервис | Роль | Replica set | Порт |
|---|---|---|---|
| `configSrv` | сервер конфигурации | `config_server` | 27017 |
| `shard1-1`, `shard1-2`, `shard1-3` | 3 реплики шарда 1 | `shard1` | 27018 |
| `shard2-1`, `shard2-2`, `shard2-3` | 3 реплики шарда 2 | `shard2` | 27019 |
| `mongos_router` | роутер mongos | - | 27020 |
| `pymongo_api` | приложение `kazhem/pymongo_api:1.0.0` | - | 8080 |

- Каждый шард - replica set из трёх узлов: 1 PRIMARY и 2 SECONDARY. При падении PRIMARY оставшиеся узлы выбирают новый.
- База: `somedb`
- Коллекция: `helloDoc`
- Шард-ключ: `{ name: "hashed" }`

## Быстрый запуск

```shell
docker compose up -d
./scripts/mongo-init.sh
```

Скрипт настраивает репликацию для каждого шарда, собирает кластер, вставляет 1000 документов и выводит статистику по шардам и репликам. Повторный запуск безопасен.

Проверка в любой момент:

```shell
./scripts/check.sh
```

## Шаги настройки вручную

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

**2. Репликация шарда 1: replica set из трёх узлов**

```shell
docker compose exec -T shard1-1 mongosh --port 27018 --quiet <<EOF
rs.initiate({
  _id: "shard1",
  members: [
    { _id: 0, host: "shard1-1:27018" },
    { _id: 1, host: "shard1-2:27018" },
    { _id: 2, host: "shard1-3:27018" }
  ]
})
EOF
```

**3. Репликация шарда 2: replica set из трёх узлов**

```shell
docker compose exec -T shard2-1 mongosh --port 27019 --quiet <<EOF
rs.initiate({
  _id: "shard2",
  members: [
    { _id: 0, host: "shard2-1:27019" },
    { _id: 1, host: "shard2-2:27019" },
    { _id: 2, host: "shard2-3:27019" }
  ]
})
EOF
```

Подожди несколько секунд, пока в каждом replica set выберется PRIMARY. Проверка: `rs.status()`.

**4. Роутер: регистрация шардов-реплик и шардирование коллекции**

```shell
docker compose exec -T mongos_router mongosh --port 27020 --quiet <<EOF
sh.addShard("shard1/shard1-1:27018,shard1-2:27018,shard1-3:27018")
sh.addShard("shard2/shard2-1:27019,shard2-2:27019,shard2-3:27019")
sh.enableSharding("somedb")
sh.shardCollection("somedb.helloDoc", { name: "hashed" })
EOF
```

**5. Наполнение данными**

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

**Количество документов на шардах**

```shell
docker compose exec -T shard1-1 mongosh --port 27018 --quiet <<EOF
use somedb
db.helloDoc.countDocuments()
EOF

docker compose exec -T shard2-1 mongosh --port 27019 --quiet <<EOF
use somedb
db.helloDoc.countDocuments()
EOF
```

**Количество реплик в шарде**

```shell
docker compose exec -T shard1-1 mongosh --port 27018 --quiet <<EOF
rs.status().members.map(m => m.name + " " + m.stateStr)
EOF
```

Ожидаемо:
- всего 1000 документов;
- shard1 ~492, shard2 ~508;
- по 3 реплики в каждом шарде.

**Приложение**

http://localhost:8080 - в поле `shards` видны оба replica set со всеми тремя репликами:

```json
"shards": {
  "shard1": "shard1/shard1-1:27018,shard1-2:27018,shard1-3:27018",
  "shard2": "shard2/shard2-1:27019,shard2-2:27019,shard2-3:27019"
}
```

**Отказоустойчивость**

```shell
docker compose stop shard1-1
curl -s localhost:8080/helloDoc/count   # -> items_count: 1000, PRIMARY переехал на другую реплику
docker compose start shard1-1
```

## Остановка

```shell
docker compose down -v
```
