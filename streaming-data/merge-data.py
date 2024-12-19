import pandas as pd
from minio import Minio
from io import BytesIO

# Inisialisasi client MinIO
minio_client = Minio(
    "localhost:9000",  # Ganti dengan alamat MinIO Anda
    access_key="minio_access_key",  # Ganti dengan access key MinIO Anda
    secret_key="minio_secret_key",  # Ganti dengan secret key MinIO Anda
    secure=False  # Jika menggunakan HTTP, set secure=False
)

# Nama bucket di MinIO tempat hasil gabungan akan disimpan
output_bucket_name = "merged-bank-data"

# Pastikan bucket output ada
if not minio_client.bucket_exists(output_bucket_name):
    minio_client.make_bucket(output_bucket_name)
    print(f"Bucket '{output_bucket_name}' created.")
else:
    print(f"Bucket '{output_bucket_name}' already exists.")

# Nama bucket untuk file CSV yang akan digabung
input_bucket_name = "bank-data"

# List file CSV yang ada di bucket input
csv_files = minio_client.list_objects(input_bucket_name)

# DataFrame kosong untuk menggabungkan data
df_combined = pd.DataFrame()

# Mengambil dan menggabungkan data dari setiap file CSV
for obj in csv_files:
    # Download CSV file dari MinIO
    data = minio_client.get_object(input_bucket_name, obj.object_name)
    
    # Membaca data CSV ke dalam DataFrame
    csv_data = pd.read_csv(data)
    
    # Gabungkan data ke DataFrame utama
    df_combined = pd.concat([df_combined, csv_data], ignore_index=True)

# Menyimpan hasil penggabungan ke CSV di memory
csv_buffer = BytesIO()
df_combined.to_csv(csv_buffer, index=False)
csv_buffer.seek(0)

# Nama file hasil gabungan
output_filename = "all_bank_data.csv"

# Upload hasil gabungan ke bucket yang berbeda
minio_client.put_object(
    output_bucket_name,  # Nama bucket output
    output_filename,     # Nama file di MinIO
    csv_buffer,          # Data dalam memory
    len(csv_buffer.getvalue())  # Ukuran file
)

print(f"File '{output_filename}' berhasil diunggah ke bucket '{output_bucket_name}'.")
