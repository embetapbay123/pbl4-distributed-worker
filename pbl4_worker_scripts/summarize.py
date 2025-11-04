# pbl4_demo/summarize.py
import argparse
import os
from collections import defaultdict
import sys
import json
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
    parser = argparse.ArgumentParser(description="Summarize word counts from multiple part-files.")
    parser.add_argument('--in_dir', required=True, help="Input directory VFS path containing part-files.")
    parser.add_argument('--out', required=True, help="Final single output VFS path.")
    args = parser.parse_args()

    in_bucket, in_prefix = parse_vfs_path(args.in_dir)
    if not in_prefix.endswith('/'):
        in_prefix += '/'

    # Liệt kê các object trong thư mục input trên S3
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=in_bucket, Prefix=in_prefix)
    
    input_keys = [obj['Key'] for page in pages for obj in page.get('Contents', [])]

    if not input_keys:
        print(f"Warning: No part-files found in {args.in_dir}", file=sys.stderr)
        out_bucket, out_key = parse_vfs_path(args.out)
        s3_client.put_object(Bucket=out_bucket, Key=out_key, Body=b'')
        print(json.dumps([args.out]))
        return

    final_counts = defaultdict(int)

    for key in input_keys:
        try:
            response = s3_client.get_object(Bucket=in_bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            for line in content.splitlines():
                if not line.strip(): continue
                try:
                    word, count_str = line.split(': ')
                    count = int(count_str)
                    final_counts[word] += count
                except ValueError:
                    print(f"Warning: Could not parse line in {key}: {line}", file=sys.stderr)
                    continue
        except ClientError as e:
            print(f"Error reading {key}: {e}", file=sys.stderr)

    sorted_words = sorted(final_counts.keys())
    
    output_content = ""
    for word in sorted_words:
        output_content += f"{word}: {final_counts[word]}\n"

    out_bucket, out_key = parse_vfs_path(args.out)
    s3_client.put_object(Bucket=out_bucket, Key=out_key, Body=output_content.encode('utf-8'))
    
    print(f"Successfully summarized and aggregated counts from {len(input_keys)} files into {args.out}", file=sys.stderr)
    print(json.dumps([args.out]))

if __name__ == "__main__":
    main()