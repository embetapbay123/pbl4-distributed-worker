FROM python:3.11-slim
WORKDIR /app

# copy đúng tên & đúng chỗ
COPY worker.py /app/worker.py

# deps cho MinIO/S3
RUN pip install --no-cache-dir boto3

ENV PYTHONUNBUFFERED=1
CMD ["python", "worker.py"]
