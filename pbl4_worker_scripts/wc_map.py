# pbl4_demo/wc_map.py
import sys
import argparse
import json
import re
from collections import defaultdict
import os
import boto3
from botocore.client import Config

# --- MinIO/S3 Configuration Block (Thêm vào mỗi script) ---
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
    """Phân tích 'fs://' path thành (bucket, key)."""
    if not path.startswith('fs://'):
        raise ValueError(f"Invalid VFS path: {path}")
    return VFS_BUCKET, path[5:]

def iter_range_lines_s3(bucket, key, start, end):
    """Đọc các dòng trong một khoảng byte từ S3, căn chỉnh theo dòng."""
    # Lấy một chunk lớn hơn một chút để xử lý dòng bị cắt ở đầu
    range_header = f"bytes={start}-{end}"
    
    response = s3_client.get_object(Bucket=bucket, Key=key, Range=range_header)
    body = response['Body']
    
    # Nếu start > 0, chúng ta có thể đang ở giữa một dòng.
    # Đọc và bỏ qua dòng đó để tránh xử lý một phần.
    # Logic này cần được kiểm tra kỹ vì S3 stream không hỗ trợ seek.
    # Cách tiếp cận đơn giản hơn là chỉ xử lý các dòng hoàn chỉnh trong range.
    # Đoạn code dưới đây giả định các dòng không quá lớn.
    content = body.read().decode('utf-8', errors='ignore')
    lines = content.splitlines()

    # Nếu bắt đầu từ 0, dòng đầu tiên luôn hợp lệ.
    # Nếu không, dòng đầu tiên có thể không hoàn chỉnh, nên bỏ qua.
    start_index = 0 if start == 0 else 1
    
    for line in lines[start_index:]:
        yield line

def main():
    parser = argparse.ArgumentParser(description="Map function for WordCount")
    parser.add_argument('--uri', required=True, help="Input file VFS URI")
    parser.add_argument('--start', type=int, default=0, help="Start byte")
    parser.add_argument('--end', type=int, required=True, help="End byte")
    parser.add_argument('--task_id', required=True, help="Task ID for output naming")
    parser.add_argument('--out_dir', required=True, help="Output directory VFS path")
    parser.add_argument('--num_reducers', type=int, required=True, help="Number of reducers")
    args = parser.parse_args()

    bucket, key = parse_vfs_path(args.uri)
    intermediate_data = [defaultdict(int) for _ in range(args.num_reducers)]

    for line in iter_range_lines_s3(bucket, key, args.start, args.end):
        words = re.findall(r'\w+', line.lower())
        for word in words:
            reducer_index = hash(word) % args.num_reducers
            intermediate_data[reducer_index][word] += 1
    
    output_files = []
    out_bucket, out_prefix = parse_vfs_path(args.out_dir)
    
    for i in range(args.num_reducers):
        if intermediate_data[i]:
            output_key = f"{out_prefix.strip('/')}/{args.task_id}-part-{i}.json"
            output_content = json.dumps(dict(intermediate_data[i])).encode('utf-8')
            
            s3_client.put_object(Bucket=out_bucket, Key=output_key, Body=output_content)
            
            output_files.append(f"fs://{output_key}")
            
    print(json.dumps(output_files))

if __name__ == "__main__":
    main()