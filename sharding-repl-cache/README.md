# sharding-repl-cache - задание 4. Шардирование + репликация + кеширование

Итоговый стенд: решения заданий 2, 3 и 4 вместе.

## Состав стенда

| Сервис | Роль | Replica set | Порт |
|---|---|---|---|
| `configSrv` | сервер конфигурации | `config_server` | 27017 |
| `shard1-1`, `shard1-2`, `shard1-3` | 3 реплики шарда 1 | `shard1` | 27018 |
| `shard2-1`, `shard2-2`, `shard2-3` | 3 реплики шарда 2 | `shard2` | 27019 |
| `mongos_router` | роутер mongos | - | 27020 |
| `redis` | кеш ответов приложения | - | 6379 (внутри сети) |
| `pymongo_api` | приложение `kazhem/pymongo_api:1.0.0` | - | 8080 |

Кеширование включено переменной окружения приложения:

```yaml
REDIS_URL: "redis://redis:6379"
```

Схема кеширования - Cache Aside:
1. Приложение ищет ответ эндпоинта `/<collection_name>/users` в Redis.
2. При промахе идёт в MongoDB через `mongos_router` и сохраняет результат в Redis на 60 с.

Для Redis задан лимит памяти 256 МБ с вытеснением LRU. Персистентность отключена: это кеш, а не хранилище.

## Быстрый запуск

```shell
docker compose up -d
./scripts/mongo-init.sh
./scripts/check-cache.sh
```

| Скрипт | Что делает |
|---|---|
| `mongo-init.sh` | настраивает replica set каждого шарда, собирает кластер, вставляет 1000 документов, выводит статистику по шардам и репликам |
| `check.sh` | количество документов: всего, по шардам и по каждой реплике |
| `check-cache.sh` | очищает кеш и пять раз вызывает `/helloDoc/users`; первый вызов ~1 с, остальные < 100 мс |

## Шаги настройки вручную

Шаги MongoDB такие же, как в [`../mongo-sharding-repl/README.md`](../mongo-sharding-repl/README.md).

**1. Сервер конфигурации**

```shell
docker compose exec -T configSrv mongosh --port 27017 --quiet <<EOF
rs.initiate({ _id: "config_server", configsvr: true, members: [{ _id: 0, host: "configSrv:27017" }] })
EOF
```

**2. Replica set шардов**

```shell
docker compose exec -T shard1-1 mongosh --port 27018 --quiet <<EOF
rs.initiate({ _id: "shard1", members: [
  { _id: 0, host: "shard1-1:27018" },
  { _id: 1, host: "shard1-2:27018" },
  { _id: 2, host: "shard1-3:27018" }
]})
EOF

docker compose exec -T shard2-1 mongosh --port 27019 --quiet <<EOF
rs.initiate({ _id: "shard2", members: [
  { _id: 0, host: "shard2-1:27019" },
  { _id: 1, host: "shard2-2:27019" },
  { _id: 2, host: "shard2-3:27019" }
]})
EOF
```

**3. Роутер**

```shell
docker compose exec -T mongos_router mongosh --port 27020 --quiet <<EOF
sh.addShard("shard1/shard1-1:27018,shard1-2:27018,shard1-3:27018")
sh.addShard("shard2/shard2-1:27019,shard2-2:27019,shard2-3:27019")
sh.enableSharding("somedb")
sh.shardCollection("somedb.helloDoc", { name: "hashed" })
EOF
```

**4. Данные**

```shell
docker compose exec -T mongos_router mongosh --port 27020 --quiet <<EOF
use somedb
for (var i = 0; i < 1000; i++) db.helloDoc.insertOne({ age: i, name: "ly" + i })
EOF
```

## Проверка

**MongoDB**

```shell
./scripts/check.sh
```

Ожидаемо:
- всего 1000 документов;
- shard1 ~492, shard2 ~508;
- по 3 реплики в каждом шарде, данные есть на каждой.

**Кеш**

```shell
curl -s -o /dev/null -w "%{time_total}\n" http://localhost:8080/helloDoc/users   # ~1 с (MongoDB)
curl -s -o /dev/null -w "%{time_total}\n" http://localhost:8080/helloDoc/users   # < 0.1 с (Redis)
```

**Приложение**

http://localhost:8080 - JSON, в котором:
- `mongo_topology_type: "Sharded"`;
- `collections.helloDoc.documents_count: 1000`;
- в `shards` два replica set по 3 реплики;
- `cache_enabled: true`.

## Остановка

```shell
docker compose down -v
```
