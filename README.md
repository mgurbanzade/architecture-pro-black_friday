# «Мобильный мир»: масштабирование pymongo-api и MongoDB

Проектная работа 4 спринта. Исходный стенд (одна нода MongoDB + приложение) лежит в корне. Решения разложены по директориям.

## Что где лежит

| Задание | Результат |
|---|---|
| 1, 5, 6. Схемы | [`schemes/mobile-world.drawio`](schemes/mobile-world.drawio) - 5 страниц, итоговая - **«5. CDN (итог)»**; PNG в [`schemes/png/`](schemes/png) |
| 2. Шардирование | [`mongo-sharding/`](mongo-sharding) |
| 3. Репликация | [`mongo-sharding-repl/`](mongo-sharding-repl) |
| 4. Кеширование - **итоговый стенд** | [`sharding-repl-cache/`](sharding-repl-cache) |
| 7-10. Архитектурный документ | [`docs/architecture.md`](docs/architecture.md) |

## Как запустить итоговый стенд

Стенд объединяет решения заданий 2-4 и состоит из 11 контейнеров:

| Сервис | Количество |
|---|---|
| `configSrv` | 1 |
| `shard1-1`...`shard1-3` (replica set `shard1`) | 3 |
| `shard2-1`...`shard2-3` (replica set `shard2`) | 3 |
| `mongos_router` | 1 |
| `redis` | 1 |
| `pymongo_api` (`kazhem/pymongo_api:1.0.0`) | 1 |

Нужно минимум 2 CPU и 4 ГБ RAM.

```shell
cd sharding-repl-cache
docker compose up -d
./scripts/mongo-init.sh
```

`mongo-init.sh`:
1. настраивает replica set сервера конфигурации и обоих шардов;
2. регистрирует шарды в `mongos_router`, шардирует `somedb.helloDoc` по `{ name: "hashed" }`;
3. вставляет 1000 документов;
4. печатает статистику.

Повторный запуск безопасен.

Ожидаемый вывод в конце:

```text
Всего документов (mongos_router): 1000
Документов на shard1:             492
Документов на shard2:             508

Реплики:
  shard1: 3 реплики
    shard1-1:27018  PRIMARY  docs=492
    shard1-2:27018  SECONDARY  docs=492
    shard1-3:27018  SECONDARY  docs=492
  shard2: 3 реплики
    shard2-1:27019  PRIMARY  docs=508
    shard2-2:27019  SECONDARY  docs=508
    shard2-3:27019  SECONDARY  docs=508
```

## Как проверить

**Статус сервисов** - все контейнеры `Up`, MongoDB и Redis `healthy`:

```shell
docker compose ps
```

**Приложение:** http://localhost:8080. JSON с информацией о MongoDB и кеше:
- `mongo_topology_type: "Sharded"`;
- `collections.helloDoc.documents_count: 1000`;
- в `shards` - два replica set по 3 реплики;
- `cache_enabled: true`.

Swagger: http://localhost:8080/docs

**Документы по шардам и репликам:**

```shell
./scripts/check.sh
```

**Кеширование:** первый вызов `/helloDoc/users` ~1 с, повторные < 100 мс.

```shell
./scripts/check-cache.sh
```

```text
Вызов 1: 1.027 c
Вызов 2: 0.007 c
Вызов 3: 0.006 c
...
```

Для запуска на виртуальной машине замените `localhost` на её внешний IP: `curl --silent http://ifconfig.me`.

**Остановка:**

```shell
docker compose down -v
```

Пошаговые команды настройки без скриптов описаны в README каждой директории.

## Исходный стенд

Корневые `compose.yaml`, `api_app/` и `scripts/` - исходное приложение с одной нодой MongoDB, без изменений:

```shell
docker compose up -d
./scripts/mongo-init.sh
```
