CREATE TABLE IF NOT EXISTS dim_customer (
    customer_id BIGSERIAL PRIMARY KEY,
    customer_email TEXT NOT NULL UNIQUE,
    first_name TEXT,
    last_name TEXT,
    age INTEGER,
    country TEXT,
    postal_code TEXT,
    pet_type TEXT,
    pet_name TEXT,
    pet_breed TEXT,
    pet_category TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id BIGSERIAL PRIMARY KEY,
    seller_email TEXT NOT NULL UNIQUE,
    first_name TEXT,
    last_name TEXT,
    country TEXT,
    postal_code TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_store (
    store_id BIGSERIAL PRIMARY KEY,
    store_key TEXT NOT NULL UNIQUE,
    store_name TEXT,
    location TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    phone TEXT,
    email TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id BIGSERIAL PRIMARY KEY,
    supplier_key TEXT NOT NULL UNIQUE,
    supplier_name TEXT,
    contact_name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_id BIGSERIAL PRIMARY KEY,
    product_key TEXT NOT NULL UNIQUE,
    product_name TEXT,
    category TEXT,
    price NUMERIC(12, 2),
    quantity INTEGER,
    weight NUMERIC(12, 2),
    color TEXT,
    size TEXT,
    brand TEXT,
    material TEXT,
    description TEXT,
    rating NUMERIC(4, 2),
    reviews INTEGER,
    release_date DATE,
    expiry_date DATE,
    supplier_id BIGINT REFERENCES dim_supplier(supplier_id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id BIGSERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_num INTEGER,
    month_num INTEGER,
    year_num INTEGER,
    quarter_num INTEGER,
    day_of_week_num INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_sales (
    fact_id BIGSERIAL PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    source_file TEXT,
    source_row_id BIGINT,
    customer_id BIGINT REFERENCES dim_customer(customer_id),
    seller_id BIGINT REFERENCES dim_seller(seller_id),
    store_id BIGINT REFERENCES dim_store(store_id),
    product_id BIGINT REFERENCES dim_product(product_id),
    supplier_id BIGINT REFERENCES dim_supplier(supplier_id),
    sale_date_id BIGINT REFERENCES dim_date(date_id),
    sale_quantity INTEGER,
    sale_total_price NUMERIC(12, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_id ON fact_sales(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_seller_id ON fact_sales(seller_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_store_id ON fact_sales(store_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_id ON fact_sales(product_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_supplier_id ON fact_sales(supplier_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_sale_date_id ON fact_sales(sale_date_id);
