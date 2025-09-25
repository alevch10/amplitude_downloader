import boto3
from dotenv import load_dotenv
import os
from botocore.client import Config  # Для custom config

load_dotenv()


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
                signature_version="s3",  # Переход на SigV3 без SHA256 (использует MD5)
                s3={
                    "addressing_style": "path",  # Path-style для custom endpoints
                    "payload_signing_enabled": False,  # Отключаем signing payload (no SHA256 header)
                },
            ),
        )
        self.bucket = os.getenv("S3_BUCKET_NAME")

    def upload_file(self, file_path: str, object_name: str):
        """Загружает файл в S3."""
        try:
            self.client.upload_file(file_path, self.bucket, object_name)
            return True
        except Exception as e:
            raise ValueError(f"Ошибка загрузки в S3: {e}")

    def generate_presigned_url(self, object_name: str, expiration: int = 3600):
        """Генерирует presigned URL для доступа к объекту в S3."""
        try:
            url = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": object_name},
                ExpiresIn=expiration,
            )
            return url
        except Exception as e:
            raise ValueError(f"Ошибка генерации URL: {e}")
