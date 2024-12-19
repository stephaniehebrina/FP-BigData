from kafka import KafkaProducer
import os
import time

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092']  # Sesuaikan dengan Kafka broker Anda
)

# Fungsi untuk membaca file gambar dan mengirim ke Kafka
def send_images_to_kafka(folder_path, topic_name):
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"Folder {folder_path} tidak ditemukan.")

    image_files = [f for f in os.listdir(folder_path) if f.endswith(('.jpg', '.png', '.jpeg'))]
    if not image_files:
        print("Tidak ada file gambar di folder.")
        return

    print(f"Found {len(image_files)} image(s) in {folder_path}. Sending to Kafka...")
    for image_file in image_files:
        image_path = os.path.join(folder_path, image_file)
        with open(image_path, 'rb') as img:
            image_bytes = img.read()
            producer.send(topic_name, value=image_bytes)
            print(f"Sent image: {image_file}")
        time.sleep(0.2)  # Jeda antar pengiriman untuk kontrol

    print("All images sent successfully.")

if __name__ == "__main__":
    folder_path = "lakehouse-3/streaming-data/pictures/Happy"  # Ganti dengan folder tempat gambar berada
    topic_name = "image-topic"  # Ganti dengan topic Kafka Anda
    try:
        send_images_to_kafka(folder_path, topic_name)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        producer.close()
