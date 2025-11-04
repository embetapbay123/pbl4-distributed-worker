# pbl4_demo/merge_files.py
import argparse
import os
import json
import sys
import io
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
    parser = argparse.ArgumentParser(description="Merges all files in a directory into a single output file.")
    parser.add_argument('--in_dir', required=True, help="Input directory VFS path.")
    parser.add_argument('--out', required=True, help="Final single output VFS path.")
    args = parser.parse_args()

    in_bucket, in_prefix = parse_vfs_path(args.in_dir)
    if not in_prefix.endswith('/'):
        in_prefix += '/'
        
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=in_bucket, Prefix=in_prefix)
    input_keys = sorted([obj['Key'] for page in pages for obj in page.get('Contents', [])])

    out_bucket, out_key = parse_vfs_path(args.out)

    if not input_keys:
        print(f"Warning: No files found in {args.in_dir}", file=sys.stderr)
        s3_client.put_object(Bucket=out_bucket, Key=out_key, Body=b'')
        print(json.dumps([args.out]))
        return

    # Dùng buffer in-memory để ghép nối nội dung
    output_buffer = io.BytesIO()
    for key in input_keys:
        try:
            response = s3_client.get_object(Bucket=in_bucket, Key=key)
            output_buffer.write(response['Body'].read())
            output_buffer.write(b'\n') # Thêm dòng mới giữa các file
        except ClientError as e:
            print(f"Error reading file {key}, skipping. Error: {e}", file=sys.stderr)

    # Ghi buffer lên MinIO
    s3_client.put_object(Bucket=out_bucket, Key=out_key, Body=output_buffer.getvalue())
    
    print(json.dumps([args.out]))
    print(f"Successfully merged {len(input_keys)} files into {args.out}", file=sys.stderr)

if __name__ == "__main__":
    main()