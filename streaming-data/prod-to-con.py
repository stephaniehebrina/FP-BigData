from kafka import KafkaProducer
import json
import time
import pandas as pd
from datetime import datetime

# Serializer untuk mengonversi data ke format JSON
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Sesuaikan dengan alamat Kafka Anda
    value_serializer=json_serializer
)

# Membaca file CSV
df = pd.read_csv('Bank-full.csv')

# Fungsi untuk mengirim 20 data setiap 10 menit
def stream_data_in_batches(batch_size=20, interval=600):
    total_rows = len(df)
    num_batches = (total_rows + batch_size - 1) // batch_size  # Total batch yang dibutuhkan
    
    for i in range(num_batches):
        # Ambil 20 data per batch
        start_idx = i * batch_size
        end_idx = min(start_idx + batch_size, total_rows)
        batch = df[start_idx:end_idx]
        
        # Kirim setiap baris di batch ke Kafka
        for _, row in batch.iterrows():
            data = row.to_dict()  # Mengonversi baris ke dictionary
            producer.send('bank-topicz', value=data)  # Mengirim data ke Kafka topic
            print(f"Sent data: {data}")  # Menampilkan data yang dikirim
        
        print(f"Completed batch {i + 1}/{num_batches} ({len(batch)} records sent)")
        
        # Jika masih ada batch berikutnya, tunggu 10 menit
        if i < num_batches - 1:
            print("Waiting for 10 minutes before sending the next batch...")
            time.sleep(interval)  # Tunggu 10 menit

if __name__ == "__main__":
    print("Starting Kafka Producer...")
    stream_data_in_batches(batch_size=20, interval=600)  # 20 data setiap 10 menit
    print("Kafka Producer has finished sending all data.")
