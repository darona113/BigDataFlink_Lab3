import hashlib
import json
import os
from datetime import datetime

import psycopg2
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import RichSinkFunction


POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'postgres')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
POSTGRES_DB = os.getenv('POSTGRES_DB', 'labdb')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgres')
KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sales_json')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'flink-star-loader')


def parse_date(value):
    if value in (None, ''):
        return None
    return datetime.strptime(value, '%m/%d/%Y').date()


def norm_text(value):
    if value is None:
        return ''
    return str(value).strip()


def build_hash(*parts):
    raw = '|'.join(norm_text(part) for part in parts)
    return hashlib.md5(raw.encode('utf-8')).hexdigest()


class PostgresStarSink(RichSinkFunction):
    def open(self, runtime_context):
        self.conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        self.conn.autocommit = False
        self.cur = self.conn.cursor()

    def upsert_customer(self, record):
        self.cur.execute(
            """
            INSERT INTO dim_customer (
                customer_email, first_name, last_name, age, country, postal_code,
                pet_type, pet_name, pet_breed, pet_category
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (customer_email) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                age = EXCLUDED.age,
                country = EXCLUDED.country,
                postal_code = EXCLUDED.postal_code,
                pet_type = EXCLUDED.pet_type,
                pet_name = EXCLUDED.pet_name,
                pet_breed = EXCLUDED.pet_breed,
                pet_category = EXCLUDED.pet_category
            RETURNING customer_id
            """,
            (
                record.get('customer_email'),
                record.get('customer_first_name'),
                record.get('customer_last_name'),
                record.get('customer_age'),
                record.get('customer_country'),
                record.get('customer_postal_code'),
                record.get('customer_pet_type'),
                record.get('customer_pet_name'),
                record.get('customer_pet_breed'),
                record.get('pet_category'),
            ),
        )
        return self.cur.fetchone()[0]

    def upsert_seller(self, record):
        self.cur.execute(
            """
            INSERT INTO dim_seller (
                seller_email, first_name, last_name, country, postal_code
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (seller_email) DO UPDATE SET
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                country = EXCLUDED.country,
                postal_code = EXCLUDED.postal_code
            RETURNING seller_id
            """,
            (
                record.get('seller_email'),
                record.get('seller_first_name'),
                record.get('seller_last_name'),
                record.get('seller_country'),
                record.get('seller_postal_code'),
            ),
        )
        return self.cur.fetchone()[0]

    def upsert_store(self, record):
        store_key = build_hash(
            record.get('store_name'), record.get('store_email'), record.get('store_phone'),
            record.get('store_city'), record.get('store_country'), record.get('store_location'),
            record.get('store_state')
        )
        self.cur.execute(
            """
            INSERT INTO dim_store (
                store_key, store_name, location, city, state, country, phone, email
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (store_key) DO UPDATE SET
                store_name = EXCLUDED.store_name,
                location = EXCLUDED.location,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                country = EXCLUDED.country,
                phone = EXCLUDED.phone,
                email = EXCLUDED.email
            RETURNING store_id
            """,
            (
                store_key,
                record.get('store_name'),
                record.get('store_location'),
                record.get('store_city'),
                record.get('store_state'),
                record.get('store_country'),
                record.get('store_phone'),
                record.get('store_email'),
            ),
        )
        return self.cur.fetchone()[0]

    def upsert_supplier(self, record):
        supplier_key = build_hash(
            record.get('supplier_name'), record.get('supplier_email'),
            record.get('supplier_phone'), record.get('supplier_city')
        )
        self.cur.execute(
            """
            INSERT INTO dim_supplier (
                supplier_key, supplier_name, contact_name, email, phone, address, city, country
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (supplier_key) DO UPDATE SET
                supplier_name = EXCLUDED.supplier_name,
                contact_name = EXCLUDED.contact_name,
                email = EXCLUDED.email,
                phone = EXCLUDED.phone,
                address = EXCLUDED.address,
                city = EXCLUDED.city,
                country = EXCLUDED.country
            RETURNING supplier_id
            """,
            (
                supplier_key,
                record.get('supplier_name'),
                record.get('supplier_contact'),
                record.get('supplier_email'),
                record.get('supplier_phone'),
                record.get('supplier_address'),
                record.get('supplier_city'),
                record.get('supplier_country'),
            ),
        )
        return self.cur.fetchone()[0]

    def upsert_product(self, record, supplier_id):
        product_key = build_hash(
            record.get('product_name'), record.get('product_category'), record.get('product_brand'),
            record.get('product_color'), record.get('product_size'), record.get('product_material'),
            supplier_id
        )
        self.cur.execute(
            """
            INSERT INTO dim_product (
                product_key, product_name, category, price, quantity, weight, color, size,
                brand, material, description, rating, reviews, release_date, expiry_date, supplier_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_key) DO UPDATE SET
                product_name = EXCLUDED.product_name,
                category = EXCLUDED.category,
                price = EXCLUDED.price,
                quantity = EXCLUDED.quantity,
                weight = EXCLUDED.weight,
                color = EXCLUDED.color,
                size = EXCLUDED.size,
                brand = EXCLUDED.brand,
                material = EXCLUDED.material,
                description = EXCLUDED.description,
                rating = EXCLUDED.rating,
                reviews = EXCLUDED.reviews,
                release_date = EXCLUDED.release_date,
                expiry_date = EXCLUDED.expiry_date,
                supplier_id = EXCLUDED.supplier_id
            RETURNING product_id
            """,
            (
                product_key,
                record.get('product_name'),
                record.get('product_category'),
                record.get('product_price'),
                record.get('product_quantity'),
                record.get('product_weight'),
                record.get('product_color'),
                record.get('product_size'),
                record.get('product_brand'),
                record.get('product_material'),
                record.get('product_description'),
                record.get('product_rating'),
                record.get('product_reviews'),
                parse_date(record.get('product_release_date')),
                parse_date(record.get('product_expiry_date')),
                supplier_id,
            ),
        )
        return self.cur.fetchone()[0]

    def upsert_date(self, record):
        sale_date = parse_date(record.get('sale_date'))
        self.cur.execute(
            """
            INSERT INTO dim_date (
                full_date, day_num, month_num, year_num, quarter_num, day_of_week_num
            ) VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (full_date) DO UPDATE SET
                day_num = EXCLUDED.day_num,
                month_num = EXCLUDED.month_num,
                year_num = EXCLUDED.year_num,
                quarter_num = EXCLUDED.quarter_num,
                day_of_week_num = EXCLUDED.day_of_week_num
            RETURNING date_id
            """,
            (
                sale_date,
                sale_date.day,
                sale_date.month,
                sale_date.year,
                ((sale_date.month - 1) // 3) + 1,
                sale_date.isoweekday(),
            ),
        )
        return self.cur.fetchone()[0]

    def insert_fact(self, record, customer_id, seller_id, store_id, product_id, supplier_id, date_id):
        self.cur.execute(
            """
            INSERT INTO fact_sales (
                event_id, source_file, source_row_id, customer_id, seller_id, store_id,
                product_id, supplier_id, sale_date_id, sale_quantity, sale_total_price
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (event_id) DO NOTHING
            """,
            (
                record.get('_event_id'),
                record.get('_source_file'),
                record.get('id'),
                customer_id,
                seller_id,
                store_id,
                product_id,
                supplier_id,
                date_id,
                record.get('sale_quantity'),
                record.get('sale_total_price'),
            ),
        )

    def invoke(self, value, context):
        record = json.loads(value)
        try:
            customer_id = self.upsert_customer(record)
            seller_id = self.upsert_seller(record)
            store_id = self.upsert_store(record)
            supplier_id = self.upsert_supplier(record)
            product_id = self.upsert_product(record, supplier_id)
            date_id = self.upsert_date(record)
            self.insert_fact(record, customer_id, seller_id, store_id, product_id, supplier_id, date_id)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def close(self):
        if hasattr(self, 'cur') and self.cur:
            self.cur.close()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10000)

    source = KafkaSource.builder()         .set_bootstrap_servers(KAFKA_BOOTSTRAP)         .set_topics(KAFKA_TOPIC)         .set_group_id(KAFKA_GROUP_ID)         .set_starting_offsets(KafkaOffsetsInitializer.earliest())         .set_value_only_deserializer(SimpleStringSchema())         .build()

    stream = env.from_source(
        source,
        watermark_strategy=None,
        source_name='Kafka sales_json source',
    )

    stream.add_sink(PostgresStarSink())

    env.execute('stream-json-to-postgres-star-schema')


if __name__ == '__main__':
    main()
