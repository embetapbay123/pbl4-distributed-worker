# pbl4_demo/worker.py
#docker run --rm --name worker2 -e MASTER_HOST=host.docker.internal -e MINIO_ENDPOINT=host.docker.internal:9000 -e MINIO_ACCESS_KEY=minioadmin -e MINIO_SECRET_KEY=minioadmin pbl4-worker
import socket
import json
import struct
import time
import threading
import subprocess
import uuid
import os
import re
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

SERVER_HOST = os.getenv('MASTER_HOST', '127.0.0.1')
SERVER_PORT = 65432
WORKER_ID = f"W-{uuid.uuid4().hex[:6]}"
SLOTS = 10
HEARTBEAT_INTERVAL = 10
SCRIPT_CACHE_DIR = "pbl4_worker_scripts" # Thư mục cache script trên worker

# --- Khối cấu hình MinIO/S3 (Thêm vào worker) ---
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
    """Phân tích đường dẫn 'fs://' thành (bucket, key)."""
    if not path.startswith('fs://'):
        raise ValueError(f"Đường dẫn VFS không hợp lệ: {path}")
    return VFS_BUCKET, path[5:]

# --- Các hàm Helper (send_message, receive_message giữ nguyên) ---
def send_message(sock, message):
    try:
        json_message = json.dumps(message).encode('utf-8')
        len_prefix = struct.pack('>I', len(json_message))
        sock.sendall(len_prefix + json_message)
    except (ConnectionResetError, OSError): pass

def receive_message(sock):
    try:
        len_prefix = sock.recv(4)
        if not len_prefix: return None
        msg_len = struct.unpack('>I', len_prefix)[0]
        return json.loads(sock.recv(msg_len))
    except (ConnectionResetError, struct.error, json.JSONDecodeError, OSError):
        return None

def run_task(task_info, sock):
    task_id = task_info['taskId']
    original_cmd = task_info['cmd']
    
    print(f"[*] Received task {task_id}: {original_cmd}")
    send_message(sock, {"type": "TASK_UPDATE", "taskId": task_id, "status": "PREPARING"})

    try:
        # --- LOGIC MỚI: Tải script từ MinIO ---
        final_cmd = original_cmd
        # Tìm kiếm đường dẫn script VFS trong câu lệnh (ví dụ: fs://scripts/wc_map.py)
        match = re.search(r'(fs://\S+\.py)', original_cmd)
        if match:
            script_vfs_path = match.group(1)
            script_filename = os.path.basename(script_vfs_path)
            local_script_path = os.path.join(SCRIPT_CACHE_DIR, script_filename)

            # Kiểm tra cache, nếu chưa có thì tải về
            if not os.path.exists(local_script_path):
                print(f"[*] Script '{script_filename}' not in cache. Downloading from {script_vfs_path}...")
                try:
                    bucket, key = parse_vfs_path(script_vfs_path)
                    s3_client.download_file(bucket, key, local_script_path)
                    print(f"[*] Download complete: {local_script_path}")
                except ClientError as e:
                    raise Exception(f"Failed to download script from S3: {e}")

            # Thay thế đường dẫn VFS bằng đường dẫn local trong câu lệnh
            final_cmd = original_cmd.replace(script_vfs_path, local_script_path)
        # --- KẾT THÚC LOGIC MỚI ---

        print(f"[*] Starting task {task_id}: {final_cmd}")
        send_message(sock, {"type": "TASK_UPDATE", "taskId": task_id, "status": "RUNNING"})
        
        proc = subprocess.Popen(final_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
        stdout, stderr = proc.communicate()

        if proc.returncode == 0:
            try: outputs = json.loads(stdout)
            except json.JSONDecodeError:
                outputs = []
                if stdout: print(f"[*] Task {task_id} STDOUT:\n{stdout.strip()}")
                
            print(f"[+] Task {task_id} SUCCEEDED.")
            send_message(sock, {
                "type": "TASK_UPDATE", "taskId": task_id, "status": "SUCCEEDED",
                "outputs": outputs, "stdout": stdout, "stderr": stderr
            })
        else:
            print(f"[!] Task {task_id} FAILED. Stderr: {stderr.strip()}")
            send_message(sock, {
                "type": "TASK_UPDATE", "taskId": task_id, "status": "FAILED",
                "error": stderr.strip(), "stdout": stdout, "stderr": stderr
            })

    except Exception as e:
        error_str = str(e)
        print(f"[!] Exception while running task {task_id}: {error_str}")
        send_message(sock, {
            "type": "TASK_UPDATE", "taskId": task_id, "status": "FAILED",
            "error": error_str, "stdout": "", "stderr": error_str
        })

# ... (send_heartbeat giữ nguyên) ...
def send_heartbeat(sock):
    while True:
        try:
            send_message(sock, {"type": "HEARTBEAT", "workerId": WORKER_ID})
            time.sleep(HEARTBEAT_INTERVAL)
        except:
            print("[!] Heartbeat failed. Disconnecting.")
            break

def main():
    # Đảm bảo thư mục cache script tồn tại
    os.makedirs(SCRIPT_CACHE_DIR, exist_ok=True)
    print(f"[*] Script cache directory is '{os.path.abspath(SCRIPT_CACHE_DIR)}'")
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.connect((SERVER_HOST, SERVER_PORT))
            print(f"[*] Worker {WORKER_ID} connected to server.")
        except ConnectionRefusedError:
            print("[!] Connection refused. Is the server running?"); return

        send_message(s, {"type": "HELLO", "workerId": WORKER_ID, "slots": SLOTS})
        
        heartbeat_thread = threading.Thread(target=send_heartbeat, args=(s,), daemon=True)
        heartbeat_thread.start()
        
        while True:
            msg = receive_message(s)
            if msg is None: print("[!] Server disconnected."); break
            
            if msg['type'] == 'WELCOME': print(f"[*] Server says: {msg['message']}")
            elif msg['type'] == 'TASK_ASSIGN':
                threading.Thread(target=run_task, args=(msg, s), daemon=True).start()

if __name__ == "__main__":
    main()