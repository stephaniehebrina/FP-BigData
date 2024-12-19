import pandas as pd
from kafka import KafkaConsumer
import json
from minio import Minio
from datetime import datetime
import io
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize MinIO client
minio_client = Minio(
    "localhost:9000",
    access_key="minio_access_key",
    secret_key="minio_secret_key",
    secure=False
)

# Ensure bucket exists
bucket_name = "bank-dataz"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    logging.info(f"Bucket '{bucket_name}' created.")
else:
    logging.info(f"Bucket '{bucket_name}' already exists.")

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'bank-topicz',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
logging.info("Kafka consumer initialized.")

def save_to_minio(data_list, batch_number):
    """
    Save a list of records to MinIO as a CSV file with a batch number.
    """
    try:
        # Convert list of dictionaries to DataFrame
        df = pd.DataFrame(data_list)
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # Create a unique filename with timestamp and batch number
        filename = f"streamed_data_batch_{batch_number}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

        # Save to MinIO
        minio_client.put_object(
            bucket_name,  # Bucket name
            filename,
            io.BytesIO(csv_buffer.getvalue().encode()),
            len(csv_buffer.getvalue())
        )
        logging.info(f"Saved batch {batch_number} to MinIO: {filename}")
    except Exception as e:
        logging.error(f"Error saving to MinIO: {e}")


# Main loop for consuming Kafka messages
batch_data = []
batch_size = 20  # Simpan setiap 20 data
batch_counter = 1  # Hitungan batch
start_time = time.time()  # Waktu mulai per batch

logging.info("Started consuming messages from Kafka.")
try:
    for message in consumer:
        try:
            record = message.value  # Each message is a dictionary like the dataset rows
            if record:
                batch_data.append(record)

            # Cek apakah sudah mencapai 20 data atau 10 menit
            if len(batch_data) >= batch_size or (time.time() - start_time) >= 600:
                save_to_minio(batch_data, batch_counter)  # Simpan batch ke MinIO
                batch_counter += 1  # Increment batch number
                batch_data = []  # Clear data untuk batch berikutnya
                start_time = time.time()  # Reset waktu mulai
        except Exception as e:
            logging.error(f"Error processing message: {e}")

except KeyboardInterrupt:
    logging.info("Stopped consuming messages from Kafka.")
except Exception as e:
    logging.error(f"Unexpected error occurred during Kafka consumption: {e}")
finally:
    # Simpan data yang tersisa jika ada sebelum keluar
    if batch_data:
        save_to_minio(batch_data, batch_counter)
        logging.info("Saved remaining data to MinIO.")
    logging.info("Kafka consumer has stopped.")
