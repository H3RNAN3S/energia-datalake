import boto3
import pandas as pd
from datetime import datetime
import os

class S3DataLoader:
    def __init__(self, bucket_name, aws_region='us-east-1'):
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.bucket_name = bucket_name
        
    def upload_csv_with_partition(self, file_path, data_type):
        """Carga archivos CSV con particionamiento por fecha"""
        current_date = datetime.now()
        year = current_date.year
        month = current_date.month
        day = current_date.day
        
        s3_key = f"{data_type}/year={year}/month={month:02d}/day={day:02d}/{os.path.basename(file_path)}"
        
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            print(f"Archivo cargado exitosamente: s3://{self.bucket_name}/{s3_key}")
        except Exception as e:
            print(f"Error cargando archivo: {e}")
    
    def load_all_files(self):
        """Carga todos los archivos CSV"""
        files_to_upload = [
            ('proveedores.csv', 'proveedores'),
            ('clientes.csv', 'clientes'),
            ('transacciones.csv', 'transacciones')
        ]
        
        for file_name, data_type in files_to_upload:
            if os.path.exists(file_name):
                self.upload_csv_with_partition(file_name, data_type)

loader = S3DataLoader('storage-raw-dev')
loader.load_all_files()