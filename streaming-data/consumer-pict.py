from kafka import KafkaConsumer
from minio import Minio
from datetime import datetime
import io

# Kafka consumer setup
consumer = KafkaConsumer(
    'image-topic',  # Sesuaikan dengan Kafka topic Anda
    bootstrap_servers=['localhost:9092'],  # Sesuaikan dengan Kafka broker Anda
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# MinIO client setup
minio_client = Minio(
    "localhost:9000",
    access_key="minio_access_key",  # Ganti dengan access key MinIO Anda
    secret_key="minio_secret_key",  # Ganti dengan secret key MinIO Anda
    secure=False
)

# Bucket name
bucket_name = "bank-pictures"

# Fungsi untuk menerima gambar dari Kafka dan langsung mengunggah ke MinIO
def receive_images_and_upload_to_minio():
    # Pastikan bucket MinIO ada, buat jika belum ada
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
        print(f"Bucket {bucket_name} created successfully.")
    else:
        print(f"Bucket {bucket_name} already exists.")

    print(f"Listening for images on Kafka topic 'image-topic'...")
    for message in consumer:
        image_bytes = message.value

        # Buat nama unik untuk setiap gambar
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        object_name = f"received_image_{message.offset}_{timestamp}.jpg"

        # Upload gambar langsung ke MinIO
        try:
            minio_client.put_object(
                bucket_name=bucket_name,
                object_name=object_name,
                data=io.BytesIO(image_bytes),
                length=len(image_bytes),
                content_type="image/jpeg"  # Sesuaikan tipe file jika bukan JPEG
            )
            print(f"Uploaded image to MinIO: {object_name}")
        except Exception as e:
            print(f"Failed to upload image to MinIO: {e}")

if __name__ == "__main__":
    try:
        receive_images_and_upload_to_minio()
    except Exception as e:
        print(f"Error: {e}")
