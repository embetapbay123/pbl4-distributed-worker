# pbl4_demo/line_reverser.py
import argparse
import json
import os
import sys
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

# --- MinIO/S3 Configuration Block ---
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', '127.0.0.1:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
VFS_BUCKET = 'pbl4-vfs'

s3_client = boto3.client(
    's3',
    endpoint_url=f'http://{MINIO_ENDPOINT}',
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

def parse_vfs_path(path):
    if not path.startswith('fs://'):
        raise ValueError(f"Invalid VFS path: {path}")
    return VFS_BUCKET, path[5:]

def main():
    parser = argparse.ArgumentParser(description="Reverses each line in a file from MinIO.")
    parser.add_argument('--in', required=True, dest='input_file', help="Input VFS path.")
    parser.add_argument('--out', required=True, dest='output_file', help="Output VFS path.")
    args = parser.parse_args()

    try:
        in_bucket, in_key = parse_vfs_path(args.input_file)
        out_bucket, out_key = parse_vfs_path(args.output_file)

        # Đọc file input từ S3
        response = s3_client.get_object(Bucket=in_bucket, Key=in_key)
        content = response['Body'].read().decode('utf-8')
        
        # Xử lý
        reversed_lines = [line.strip()[::-1] for line in content.splitlines()]
        output_content = '\n'.join(reversed_lines)

        # Ghi file output lên S3
        s3_client.put_object(
            Bucket=out_bucket, 
            Key=out_key, 
            Body=output_content.encode('utf-8')
        )
        
        print(json.dumps([args.output_file]))

    except ClientError as e:
        print(f"S3 Error processing {args.input_file}: {e}", file=sys.stderr)
        exit(1)
    except Exception as e:
        print(f"General Error processing {args.input_file}: {e}", file=sys.stderr)
        exit(1)

if __name__ == "__main__":
    main()