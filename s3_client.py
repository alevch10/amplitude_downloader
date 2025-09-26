import boto3
import os
from botocore.client import Config
import logging

# Логгер создается после импортов
logger = logging.getLogger(__name__)

class S3Client:
    def __init__(self):
        endpoint_url = os.getenv("S3_ENDPOINT_URL")
        self.client = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_DEFAULT_REGION"),
            endpoint_url=endpoint_url,
            config=Config(
                signature_version="s3",
                s3={
                    "addressing_style": "path",
                    "payload_signing_enabled": False,
                },
            ),
        )
        self.bucket = os.getenv("S3_BUCKET_NAME")
        logger.info(f"☁️ S3 клиент инициализирован для бакета: {self.bucket}")

    def upload_file(self, file_path: str, object_name: str):
        try:
            file_size = os.path.getsize(file_path) / 1024 / 1024
            logger.info(f"☁️ Загрузка файла {file_path} размером {file_size:.2f} MB в S3")
            logger.debug(f"📤 Начало загрузки в S3: {object_name} из {file_path}")
            
            self.client.upload_file(file_path, self.bucket, object_name)
            
            logger.info(f"✅ Файл {object_name} успешно загружен в S3")
            logger.debug(f"📥 Загрузка в S3 завершена: {object_name}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки в S3: {e}")
            raise

    def generate_presigned_url(self, object_name: str, expiration: int = 3600):
        try:
            url = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": object_name},
                ExpiresIn=expiration,
            )
            logger.debug(f"🔗 Сгенерирован presigned URL для {object_name}")
            return url
        except Exception as e:
            logger.error(f"❌ Ошибка генерации URL: {e}")
            raise