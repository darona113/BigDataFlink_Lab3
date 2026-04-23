

## Что реализовано

- 10 CSV-файлов с исходными данными в папке `data/`
- Python-приложение `kafka-producer/producer.py`, которое читает CSV, преобразует каждую строку в JSON и отправляет сообщения в Kafka
- Flink streaming job `flink/jobs/stream_to_star.py`, которая читает Kafka topic `sales_json`
- трансформация в модель **звезда**:
  - `dim_customer`
  - `dim_seller`
  - `dim_store`
  - `dim_supplier`
  - `dim_product`
  - `dim_date`
  - `fact_sales`
- PostgreSQL и SQL-инициализация таблиц
- Docker Compose для запуска всей инфраструктуры

## Структура проекта

```text
BigDataFlink_ready/
├── data/
├── docker-compose.yml
├── flink/
│   ├── Dockerfile
│   └── jobs/
│       └── stream_to_star.py
├── kafka-producer/
│   ├── Dockerfile
│   ├── producer.py
│   └── requirements.txt
├── postgres/
│   └── init/
│       ├── 01_init.sql
│       └── 02_checks.sql
├── scripts/
│   ├── run_producer.sh
│   └── submit_flink_job.sh
└── README.md
```

## Как запускать

### 1. Собрать и поднять контейнеры

```bash
docker compose up --build -d postgres zookeeper kafka jobmanager taskmanager
```

### 2. Отправить Flink job

```bash
docker compose run --rm flink-submit
```

### 3. Запустить producer и отправить данные в Kafka

```bash
docker compose run --rm producer
```

### 4. Проверить результат в PostgreSQL

Подключение в DBeaver:

- Host: `localhost`
- Port: `5432`
- Database: `labdb`
- User: `postgres`
- Password: `postgres`

Проверочные запросы:

```sql
SELECT COUNT(*) FROM fact_sales;
SELECT COUNT(*) FROM dim_customer;
SELECT COUNT(*) FROM dim_product;

SELECT source_file, COUNT(*)
FROM fact_sales
GROUP BY source_file
ORDER BY source_file;
```

## Логика преобразования в звезду

Каждое Kafka-сообщение содержит одну продажу в формате JSON.

Во Flink job для каждой записи выполняется:

1. upsert в `dim_customer`
2. upsert в `dim_seller`
3. upsert в `dim_store`
4. upsert в `dim_supplier`
5. upsert в `dim_product`
6. upsert в `dim_date`
7. insert в `fact_sales`

Для сущностей без явного стабильного идентификатора используются natural key / hash key:

- `customer_email`
- `seller_email`
- `store_key = md5(...)`
- `supplier_key = md5(...)`
- `product_key = md5(...)`
- `event_id = <имя_файла>:<id>`

## Остановка

```bash
docker compose down -v
```
