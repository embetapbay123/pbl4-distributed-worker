# pbl4_demo/wc_reduce.py
import sys
import argparse
import json
from collections import defaultdict
import os
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
    parser = argparse.ArgumentParser(description="Reduce function for WordCount")
    parser.add_argument('--inputs', required=True, help="Comma-separated list of input VFS paths")
    parser.add_argument('--out', required=True, help="Final output VFS path")
    args = parser.parse_args()

    final_counts = defaultdict(int)
    input_vfs_paths = args.inputs.split(',')

    for vfs_path in input_vfs_paths:
        try:
            bucket, key = parse_vfs_path(vfs_path)
            response = s3_client.get_object(Bucket=bucket, Key=key)
            data = json.load(response['Body'])
            for word, count in data.items():
                final_counts[word] += count
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                print(f"Warning: Input file not found, skipping: {vfs_path}", file=sys.stderr)
                continue
            raise e
    
    sorted_counts = sorted(final_counts.items(), key=lambda item: item[1], reverse=True)

    output_content = ""
    for word, count in sorted_counts:
        output_content += f"{word}: {count}\n"
        
    out_bucket, out_key = parse_vfs_path(args.out)
    s3_client.put_object(
        Bucket=out_bucket, 
        Key=out_key, 
        Body=output_content.encode('utf-8')
    )
            
    print(json.dumps([args.out]))

if __name__ == "__main__":
    main()