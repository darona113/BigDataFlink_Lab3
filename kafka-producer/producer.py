import json
import math
import os
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer


def clean_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if hasattr(value, 'item'):
        value = value.item()
    return value


def row_to_payload(row, source_file):
    payload = {column: clean_value(value) for column, value in row.items()}
    payload['_source_file'] = source_file
    payload['_event_id'] = f"{source_file}:{payload.get('id')}"
    return payload


def main():
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    topic = os.getenv('KAFKA_TOPIC', 'sales_json')
    data_dir = Path(os.getenv('DATA_DIR', '/app/data'))
    delay = float(os.getenv('SEND_DELAY_SECONDS', '0.01'))

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode('utf-8'),
        retries=10,
        linger_ms=10,
    )

    csv_files = sorted(data_dir.glob('MOCK_DATA (*.csv'))
    if not csv_files:
        raise FileNotFoundError(f'No CSV files found in {data_dir}')

    total_sent = 0
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        for _, row in df.iterrows():
            payload = row_to_payload(row.to_dict(), csv_file.name)
            producer.send(topic, payload)
            total_sent += 1
            if delay > 0:
                time.sleep(delay)
        print(f'Sent {len(df)} messages from {csv_file.name}')

    producer.flush()
    producer.close()
    print(f'Done. Total messages sent: {total_sent}')


if __name__ == '__main__':
    main()
