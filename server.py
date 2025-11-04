# pbl4_demo/server.py
import socket
import threading
import json
import struct
import os
import time
import re
import shutil
from collections import deque, defaultdict
import uuid
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
from werkzeug.serving import make_server
from werkzeug.utils import secure_filename
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
import io

# --- Constants ---
# HOST = '127.0.0.1'
HOST = '0.0.0.0'
PORT = 65432
WEB_PORT = 5000
MAX_TASK_RETRIES = 3
WORKER_TIMEOUT = 30

# --- MinIO/S3 Configuration ---
# Sử dụng biến môi trường để bảo mật và linh hoạt hơn
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', '127.0.0.1:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
VFS_BUCKET = 'pbl4-vfs'  # Tên bucket chung cho VFS

# --- Global State Management ---
job = {}
workers = {}
task_queue = deque()
tasks_state = {}
stage_state = {}
stage_outputs = defaultdict(list)
stage_dependencies = {}
sorted_stages = []
JOB_IN_PROGRESS = threading.Lock()
JOB_CANCEL_EVENT = threading.Event()

# --- S3 Client Initialization ---
try:
    s3_client = boto3.client(
        's3',
        endpoint_url=f'http://{MINIO_ENDPOINT}',
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4')
    )
    # Kiểm tra xem bucket có tồn tại không
    s3_client.head_bucket(Bucket=VFS_BUCKET)
    print(f"[*] Successfully connected to MinIO and found bucket '{VFS_BUCKET}'.")
except Exception as e:
    print(f"[!!!] CRITICAL: Could not connect to MinIO or find bucket '{VFS_BUCKET}'. Please check your configuration.")
    print(f"[!!!] Error: {e}")
    os._exit(1)


# --- Helper Functions ---
def parse_vfs_path(path):
    """Phân tích một đường dẫn 'fs://' thành (bucket, key)."""
    if path.startswith('fs://'):
        return VFS_BUCKET, path[5:]
    return None, None # Không phải là VFS path

def resolve_script_path(script_name):
    """
    Trả về đường dẫn VFS đầy đủ đến script.
    Worker sẽ chịu trách nhiệm tải về và thực thi nó.
    """
    vfs_path = f"fs://scripts/{script_name}"
    print(f"[*] Resolving script '{script_name}' to VFS path: {vfs_path}")
    return vfs_path
    
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

# --- Job State & DAG Logic ---
def reset_job_state():
    global job, task_queue, tasks_state, stage_state, stage_outputs, stage_dependencies, sorted_stages
    job = {}; task_queue.clear(); tasks_state.clear(); stage_state.clear(); stage_outputs.clear(); stage_dependencies.clear(); sorted_stages.clear()
    print("[*] Job state has been reset.")

def load_dag_from_payload(dag_payload):
    global job, stage_state, stage_dependencies, sorted_stages
    print("[*] Loading DAG from received payload...")
    job = dag_payload; adj = {s['stageId']: [] for s in job['stages']}; in_degree = {s['stageId']: 0 for s in job['stages']}
    for stage in job['stages']:
        sid = stage['stageId']; stage_state[sid] = 'PENDING'; stage_dependencies[sid] = stage.get('deps', [])
        for dep in stage_dependencies[sid]:
            if dep not in adj: raise Exception(f"Dependency '{dep}' not found for stage '{sid}'")
            adj[dep].append(sid); in_degree[sid] += 1
    queue = deque([s_id for s_id, deg in in_degree.items() if deg == 0])
    while queue:
        u = queue.popleft(); sorted_stages.append(u)
        for v in adj.get(u, []):
            in_degree[v] -= 1
            if in_degree[v] == 0: queue.append(v)
    if len(sorted_stages) != len(job['stages']): raise Exception("DAG has a cycle or is invalid.")
    print(f"[*] DAG loaded successfully. Execution order: {', '.join(sorted_stages)}")

def generate_tasks_for_stage(stage_id):
    print(f"[*] Generating tasks for stage: {stage_id}")
    stage = next((s for s in job['stages'] if s['stageId'] == stage_id), None)
    if not stage: print(f"[!] CRITICAL: Stage '{stage_id}' not found. Aborting."); os._exit(1)
    spec = stage['tasksSpec']

    if stage['type'] == 'mapreduce':
        if spec['phase'] == 'map':
            num_reducers = spec['reducers']
            map_out_dir_vfs = f"fs://job/{job['jobId']}/{stage_id}/" # VFS path
            mapper_script = resolve_script_path(spec["mapper"])
            for input_vfs_path in spec['inputs']:
                bucket, key = parse_vfs_path(input_vfs_path)
                try:
                    head = s3_client.head_object(Bucket=bucket, Key=key)
                    file_size = head['ContentLength']
                except ClientError:
                    print(f"[!] Input file not found in S3: {input_vfs_path}"); continue
                
                if file_size == 0: continue
                policy = spec.get('splitPolicy', {})
                threshold = policy.get('thresholdBytes', 128*1024*1024)
                split_size = policy.get('splitSizeBytes', 64*1024*1024)
                
                # Command gửi cho worker bây giờ chứa VFS paths.
                # Script của worker sẽ chịu trách nhiệm download/upload từ MinIO.
                if file_size <= threshold:
                    task_id = f"T-{uuid.uuid4().hex[:8]}"
                    cmd = f'python {mapper_script} --uri "{input_vfs_path}" --start 0 --end {file_size-1} --task_id {task_id} --out_dir "{map_out_dir_vfs}" --num_reducers {num_reducers}'
                    task_queue.append({'taskId': task_id, 'stageId': stage_id, 'cmd': cmd}); tasks_state[task_id] = {'status': 'PENDING', 'stageId': stage_id, 'cmd': cmd, 'retries': 0}
                else:
                    num_splits = (file_size + split_size - 1) // split_size
                    for i in range(num_splits):
                        start = i * split_size; end = min((i + 1) * split_size - 1, file_size - 1)
                        task_id = f"T-{uuid.uuid4().hex[:8]}"
                        cmd = f'python {mapper_script} --uri "{input_vfs_path}" --start {start} --end {end} --task_id {task_id} --out_dir "{map_out_dir_vfs}" --num_reducers {num_reducers}'
                        task_queue.append({'taskId': task_id, 'stageId': stage_id, 'cmd': cmd}); tasks_state[task_id] = {'status': 'PENDING', 'stageId': stage_id, 'cmd': cmd, 'retries': 0}

        elif spec['phase'] == 'reduce':
            num_partitions = spec['partitions']; manifest_stage = spec['manifestFrom']['stageId']
            partitions = [[] for _ in range(num_partitions)]
            for map_output_file in stage_outputs[manifest_stage]:
                match = re.search(r'-part-(\d+)\.json$', map_output_file)
                if match:
                    try:
                        partition_index = int(match.group(1))
                        if 0 <= partition_index < num_partitions: partitions[partition_index].append(map_output_file)
                    except (ValueError, IndexError): print(f"[!] Warning: Invalid partition index parsed from {map_output_file}")
                else: print(f"[!] Warning: Could not parse partition from map output file name: {map_output_file}")
            
            reducer_script = resolve_script_path(spec["reducer"])
            for i in range(num_partitions):
                if not partitions[i]: continue
                task_id = f"T-{uuid.uuid4().hex[:8]}"; inputs_str = ','.join(f'"{p}"' for p in partitions[i])
                output_vfs_path = f"fs://job/{job['jobId']}/{stage_id}/part-{i}.txt"
                cmd = f'python {reducer_script} --inputs {inputs_str} --out "{output_vfs_path}"'
                task_queue.append({'taskId': task_id, 'stageId': stage_id, 'cmd': cmd}); tasks_state[task_id] = {'status': 'PENDING', 'stageId': stage_id, 'cmd': cmd, 'retries': 0}

    # Các stage 'single' và 'bot' cũng sử dụng VFS paths trong command
    elif stage['type'] == 'single':
        raw_cmd = spec['cmd']
        # Thay thế các "fs://..." bằng chính nó, vì worker sẽ xử lý
        def replacer(match): return f'"{match.group(1)}"'
        resolved_cmd = re.sub(r'"(fs://[^"]+)"', replacer, raw_cmd)
        task_id = f"T-{uuid.uuid4().hex[:8]}"
        task_queue.append({'taskId': task_id, 'stageId': stage_id, 'cmd': resolved_cmd}); tasks_state[task_id] = {'status': 'PENDING', 'stageId': stage_id, 'cmd': resolved_cmd, 'retries': 0}

    elif stage['type'] == 'bot':
        cmd_template = spec['cmd_template']
        output_dir_vfs = spec['output_dir']
        for input_vfs_path in spec['inputs']:
            # Lấy tên file từ VFS path để tạo output path
            base_name = input_vfs_path.split('/')[-1]
            output_vfs_path = f"{output_dir_vfs.rstrip('/')}/{base_name}"
            cmd = cmd_template.replace('{input}', f'"{input_vfs_path}"').replace('{output}', f'"{output_vfs_path}"')
            task_id = f"T-{uuid.uuid4().hex[:8]}"
            task_queue.append({'taskId': task_id, 'stageId': stage_id, 'cmd': cmd}); tasks_state[task_id] = {'status': 'PENDING', 'stageId': stage_id, 'cmd': cmd, 'retries': 0}
            print(f"[*] Generated BoT task {task_id}")

def check_stage_completion(stage_id):
    tasks_in_stage = [tid for tid, t in tasks_state.items() if t.get('stageId') == stage_id]
    if not tasks_in_stage: return False
    if all(tasks_state.get(tid, {}).get('status') == 'SUCCEEDED' for tid in tasks_in_stage):
        stage_state[stage_id] = 'SUCCEEDED'; print(f"\n[+] Stage '{stage_id}' completed successfully!\n"); return True
    return False

def job_controller(dag_payload, is_resumed=False):
    JOB_CANCEL_EVENT.clear()
    
    try:
        if not is_resumed:
            reset_job_state()
            load_dag_from_payload(dag_payload)

        checkpoint_bucket, checkpoint_key = parse_vfs_path(f"fs://job/{job['jobId']}/checkpoint.json")

        for stage_id in sorted_stages:
            if stage_state.get(stage_id) == 'SUCCEEDED':
                print(f"[*] Skipping already completed stage: {stage_id}")
                continue
            
            if JOB_CANCEL_EVENT.is_set():
                print("[*] Job cancelled by user."); stage_state[stage_id] = 'CANCELLED'; break
            
            print(f"[*] Starting stage '{stage_id}'..."); stage_state[stage_id] = 'RUNNING'
            for dep_id in stage_dependencies.get(stage_id, []):
                while stage_state.get(dep_id) != 'SUCCEEDED':
                    if JOB_CANCEL_EVENT.is_set(): break
                    time.sleep(1)
            
            if JOB_CANCEL_EVENT.is_set(): break
            
            if not any(t for t in tasks_state.values() if t['stageId'] == stage_id):
                generate_tasks_for_stage(stage_id)
            
            last_checkpoint_time = time.time()
            
            while stage_state.get(stage_id) != 'SUCCEEDED':
                if JOB_CANCEL_EVENT.is_set(): break
                if check_stage_completion(stage_id): break
                
                tasks_for_stage_exist = any(t['stageId'] == stage_id for t in tasks_state.values())
                if not tasks_for_stage_exist and not any(t['stageId'] == stage_id for t in task_queue):
                    stage_state[stage_id] = 'SUCCEEDED'; print(f"\n[+] Stage '{stage_id}' completed (no tasks generated).\n"); break
                
                current_time = time.time()
                if current_time - last_checkpoint_time > 10:
                    try:
                        checkpoint_data = {
                            "dag_payload": job, "tasks_state": make_serializable(tasks_state), "stage_state": stage_state,
                            "stage_outputs": stage_outputs, "task_queue": list(task_queue), "sorted_stages": sorted_stages,
                            "stage_dependencies": stage_dependencies
                        }
                        # Ghi checkpoint lên MinIO
                        s3_client.put_object(
                            Bucket=checkpoint_bucket, 
                            Key=checkpoint_key, 
                            Body=json.dumps(checkpoint_data).encode('utf-8')
                        )
                        last_checkpoint_time = current_time
                    except Exception as e:
                        print(f"[!!!] Failed to save checkpoint to S3 for job '{job['jobId']}': {e}")

                time.sleep(1)

        if not JOB_CANCEL_EVENT.is_set():
            try:
                # Dọn dẹp checkpoint trên MinIO
                s3_client.delete_object(Bucket=checkpoint_bucket, Key=checkpoint_key)
                print(f"[*] Checkpoint for job '{job['jobId']}' cleaned up from S3.")
            except ClientError as e:
                if e.response['Error']['Code'] != 'NoSuchKey': raise e # Bỏ qua lỗi nếu file không tồn tại
            print("[***] JOB COMPLETED SUCCESSFULLY! [***]")
        else:
            print("[***] JOB CANCELLED! [***]")
    
    except Exception as e:
        print(f"[!!!] JOB FAILED: {e}")
    
    finally:
        if job and job.get("jobId"):
            try:
                final_job_status = "COMPLETED"
                if JOB_CANCEL_EVENT.is_set(): final_job_status = "CANCELLED"
                history_data = {
                    "jobId": job.get("jobId"), "finalStatus": final_job_status,
                    "endTime": time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                    "sorted_stages": sorted_stages, "stage_state": stage_state,
                    "tasks_state": make_serializable(tasks_state) 
                }
                # Ghi history lên MinIO
                history_bucket, history_key = parse_vfs_path(f"fs://history/{job['jobId']}.json")
                s3_client.put_object(
                    Bucket=history_bucket, 
                    Key=history_key, 
                    Body=json.dumps(history_data, indent=4).encode('utf-8')
                )
                print(f"[*] Job history for '{job['jobId']}' saved to S3.")
            except Exception as e:
                print(f"[!!!] CRITICAL: Failed to save job history to S3 for '{job['jobId']}': {e}")
        
        if JOB_IN_PROGRESS.locked(): JOB_IN_PROGRESS.release()
        print("[*] Job lock released.")

# --- Worker & Task Management ---
def assign_tasks():
    while True:
        if not task_queue: time.sleep(0.5); continue
        for worker_id, worker_info in list(workers.items()):
            if worker_info['slots'] > 0 and task_queue:
                try: 
                    task = task_queue.popleft()
                    worker_info['slots'] -= 1; tasks_state[task['taskId']]['status'] = 'ASSIGNED'; tasks_state[task['taskId']]['workerId'] = worker_id
                    message = {"type": "TASK_ASSIGN", **task}; send_message(worker_info['socket'], message)
                    print(f"[*] Assigned task {task['taskId']} to worker {worker_id}")
                except IndexError: continue
        time.sleep(0.2)
        
def handle_connection(conn, addr):
    print(f"[+] New connection from {addr}"); worker_id_for_this_connection = None
    try:
        initial_msg = receive_message(conn)
        if not initial_msg: return
        if initial_msg['type'] == 'HELLO':
            worker_id = initial_msg['workerId']; worker_id_for_this_connection = worker_id
            workers[worker_id] = {'socket': conn, 'slots': initial_msg.get('slots', 1), 'addr': addr, 'last_heartbeat': time.time()}
            print(f"[*] Worker {worker_id} registered."); send_message(conn, {"type": "WELCOME", "message": "Connected"})
            while True:
                msg = receive_message(conn)
                if msg is None: break
                if msg['type'] == 'HEARTBEAT':
                    if worker_id in workers: workers[worker_id]['last_heartbeat'] = time.time()
                elif msg['type'] == 'TASK_UPDATE':
                    task_id, status = msg['taskId'], msg['status']
                    if task_id not in tasks_state: continue
                    tasks_state[task_id]['status'] = status; print(f"[*] Task update: {task_id} -> {status}")
                    if status in ['SUCCEEDED', 'FAILED']:
                        # Ghi log task lên MinIO
                        log_content = "--- STDOUT ---\n" + msg.get('stdout', '') + "\n\n--- STDERR ---\n" + msg.get('stderr', '')
                        log_bucket, log_key = parse_vfs_path(f"fs://job/{job['jobId']}/logs/{task_id}.log")
                        s3_client.put_object(Bucket=log_bucket, Key=log_key, Body=log_content.encode('utf-8'))
                        
                        if worker_id in workers: workers[worker_id]['slots'] += 1
                        if status == 'SUCCEEDED':
                            stage_id = tasks_state[task_id]['stageId']; stage_outputs[stage_id].extend(msg.get('outputs', []))
                        else:
                            print(f"[!] Task {task_id} FAILED. Error: {msg.get('error')}")
                            task_info = tasks_state[task_id]
                            if task_info['retries'] < MAX_TASK_RETRIES:
                                task_info['retries'] += 1; task_info['status'] = 'PENDING'; task_info['workerId'] = None
                                requeue_task = {'taskId': task_id, 'stageId': task_info['stageId'], 'cmd': task_info['cmd']}
                                task_queue.appendleft(requeue_task)
                                print(f"[*] Re-queueing task {task_id}. Attempt {task_info['retries']}/{MAX_TASK_RETRIES}.")
                            else:
                                print(f"[!!!] CRITICAL: Task {task_id} FAILED after {MAX_TASK_RETRIES} attempts. Aborting job.")
                                JOB_CANCEL_EVENT.set()
        elif initial_msg['type'] == 'SUBMIT_JOB':
            if JOB_IN_PROGRESS.acquire(blocking=False):
                send_message(conn, {"type": "ACK", "status": "OK", "message": "Job accepted."})
                dag_payload = initial_msg['payload']; threading.Thread(target=job_controller, args=(dag_payload,)).start()
            else: send_message(conn, {"type": "ACK", "status": "FAIL", "message": "Server busy."})
    finally:
        if worker_id_for_this_connection and worker_id_for_this_connection in workers:
            print(f"[-] Worker {worker_id_for_this_connection} disconnected."); del workers[worker_id_for_this_connection]
        conn.close()

def monitor_workers():
    while True:
        time.sleep(WORKER_TIMEOUT / 2)
        dead_workers = [wid for wid, info in list(workers.items()) if time.time() - info.get('last_heartbeat', 0) > WORKER_TIMEOUT]
        for worker_id in dead_workers:
            print(f"[!!!] Worker {worker_id} timed out. Declared dead.")
            if worker_id in workers: del workers[worker_id]
            for tid, tinfo in list(tasks_state.items()):
                if tinfo.get('workerId') == worker_id and tinfo['status'] in ['ASSIGNED', 'RUNNING']:
                    print(f"[*] Re-queueing task {tid} from dead worker {worker_id}.")
                    tinfo['status'] = 'PENDING'; tinfo['workerId'] = None
                    requeue_task = {'taskId': tid, 'stageId': tinfo['stageId'], 'cmd': tinfo['cmd']}
                    task_queue.appendleft(requeue_task)

# --- Flask Web Server ---
app = Flask(__name__)
CORS(app)
def make_serializable(data):
    if isinstance(data, dict): return {k: make_serializable(v) for k, v in data.items() if k != 'socket'}
    if isinstance(data, list): return [make_serializable(i) for i in data]
    return data

@app.route('/api/job', methods=['GET'])
def get_job_status():
    with threading.Lock():
        if not job: return jsonify({"status": "No job running"})
        return jsonify({"jobId": job.get("jobId"), "sorted_stages": sorted_stages, "stage_state": stage_state, "tasks_state": make_serializable(tasks_state), "task_queue_size": len(task_queue)})

@app.route('/api/workers', methods=['GET'])
def get_workers():
    with threading.Lock():
        return jsonify(make_serializable(workers))

@app.route('/api/tasks/<task_id>/log', methods=['GET'])
def get_task_log(task_id):
    if not job: return "No job running", 404
    log_bucket, log_key = parse_vfs_path(f"fs://job/{job['jobId']}/logs/{task_id}.log")
    try:
        response = s3_client.get_object(Bucket=log_bucket, Key=log_key)
        content = response['Body'].read().decode('utf-8')
        return content, 200, {'Content-Type': 'text/plain; charset=utf-8'}
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return "Log not found.", 404
        return f"Error retrieving log: {e}", 500

@app.route('/api/vfs/upload', methods=['POST'])
def upload_file():
    target_folder = request.form.get('target', 'raw') 
    if target_folder not in ['raw', 'scripts']:
        return jsonify({"status": "FAIL", "message": "Invalid target folder."}), 400
    if 'file' not in request.files:
        return jsonify({"status": "FAIL", "message": "No file part in the request."}), 400
    file = request.files['file']
    if not file or not file.filename:
        return jsonify({"status": "FAIL", "message": "No file selected."}), 400
    
    filename = secure_filename(file.filename)
    object_key = f"{target_folder}/{filename}"
    try:
        s3_client.upload_fileobj(file, VFS_BUCKET, object_key)
        print(f"[*] User uploaded {filename} to S3 bucket '{VFS_BUCKET}' with key '{object_key}'")
        return jsonify({"status": "OK", "message": f"File '{filename}' uploaded successfully to fs://{object_key}."})
    except Exception as e:
        return jsonify({"status": "FAIL", "message": f"Upload to S3 failed: {e}"}), 500

@app.route('/api/vfs/download', methods=['GET'])
def download_file():
    vfs_path = request.args.get('path')
    if not vfs_path: return "Missing 'path' parameter.", 400
    
    bucket, key = parse_vfs_path(vfs_path)
    # Thêm kiểm tra cho cả key để làm hài lòng Pylance
    if not bucket or not key: 
        return "Invalid VFS path.", 400
        
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        def generate():
            for chunk in response['Body'].iter_chunks(): yield chunk
        
        return Response(generate(), mimetype=response['ContentType'], headers={
            "Content-Disposition": f"attachment;filename={os.path.basename(key)}"
        })
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey': return "File not found.", 404
        return f"An error occurred: {e}", 500

@app.route('/api/vfs/list', methods=['GET'])
def list_vfs_path():
    vfs_path_str = request.args.get('path', 'fs://')
    bucket, prefix = parse_vfs_path(vfs_path_str)
    if not bucket: return jsonify({"error": "Invalid VFS path"}), 400

    if prefix and not prefix.endswith('/'): prefix += '/'
    if prefix == '/': prefix = ''

    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')
        items = []
        for page in pages:
            for common_prefix in page.get('CommonPrefixes', []):
                items.append({
                    "name": os.path.basename(os.path.normpath(common_prefix.get('Prefix'))),
                    "type": "directory", "size": 0
                })
            for obj in page.get('Contents', []):
                if obj.get('Key') != prefix: # Don't list the directory itself
                    items.append({
                        "name": os.path.basename(obj.get('Key')),
                        "type": "file", "size": obj.get('Size', 0)
                    })
        return jsonify({"path": vfs_path_str, "contents": sorted(items, key=lambda x: x['name'])})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/vfs/delete', methods=['POST'])
@app.route('/api/vfs/delete', methods=['POST'])
def delete_vfs_path():
    data = request.get_json(silent=True)
    if not data: return jsonify({"status": "FAIL", "message": "Invalid or missing JSON payload."}), 400
    vfs_path_str = data.get('path')
    if not vfs_path_str: return jsonify({"status": "FAIL", "message": "Path is required."}), 400
    
    bucket, key = parse_vfs_path(vfs_path_str)
    if not bucket or not key: 
        return jsonify({"status": "FAIL", "message": "Invalid VFS path."}), 400

    try:
        if key.endswith('/'):
            objects_to_delete = []
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=key):
                for obj in page.get('Contents', []):
                    objects_to_delete.append({'Key': obj['Key']})
            if objects_to_delete:
                s3_client.delete_objects(Bucket=bucket, Delete={'Objects': objects_to_delete})
            message = f"Directory '{vfs_path_str}' and its contents deleted."
        else:
            s3_client.delete_object(Bucket=bucket, Key=key)
            message = f"File '{vfs_path_str}' deleted successfully."

        print(f"[*] Deleted VFS path: {vfs_path_str}")
        return jsonify({"status": "OK", "message": message})
    except Exception as e:
        return jsonify({"status": "FAIL", "message": str(e)}), 500

# ... (các API job/submit, job/kill không đổi) ...
@app.route('/api/job/submit', methods=['POST'])
def submit_job_api():
    if JOB_IN_PROGRESS.acquire(blocking=False):
        try:
            dag_payload = request.json
            if not dag_payload or 'jobId' not in dag_payload:
                JOB_IN_PROGRESS.release(); return jsonify({"status": "FAIL", "message": "Invalid DAG JSON."}), 400
            threading.Thread(target=job_controller, args=(dag_payload, False)).start()
            return jsonify({"status": "OK", "message": f"Job '{dag_payload['jobId']}' accepted."})
        except Exception as e:
            JOB_IN_PROGRESS.release(); return jsonify({"status": "FAIL", "message": f"Error: {e}"}), 500
    else: return jsonify({"status": "FAIL", "message": "Server busy."}), 409

@app.route('/api/job/kill', methods=['POST'])
def kill_job():
    if not JOB_IN_PROGRESS.locked(): return jsonify({"status": "OK", "message": "No job is running."})
    print("[!!!] Received KILL signal for the current job.")
    JOB_CANCEL_EVENT.set(); task_queue.clear()
    return jsonify({"status": "OK", "message": "Kill signal sent."})


@app.route('/api/history', methods=['GET'])
def get_history_list():
    bucket, prefix = parse_vfs_path('fs://history/')
    jobs = []
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                filename = os.path.basename(obj['Key'])
                if filename.endswith('.json'):
                    try:
                        response = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
                        data = json.load(response['Body'])
                        jobs.append({"jobId": data.get("jobId", filename.replace('.json', '')), "status": data.get("finalStatus", "UNKNOWN"), "endTime": data.get("endTime", "N/A"), "filename": filename})
                    except Exception:
                        jobs.append({"jobId": filename, "status": "ERROR", "endTime": "N/A", "filename": filename})
        jobs.sort(key=lambda x: x['endTime'], reverse=True)
        return jsonify(jobs)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/history/<job_id>', methods=['GET'])
def get_history_detail(job_id):
    safe_filename = secure_filename(f"{job_id}.json")
    bucket, key = parse_vfs_path(f'fs://history/{safe_filename}')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        data = json.load(response['Body'])
        return jsonify(data)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return jsonify({"error": "Job history not found"}), 404
        return jsonify({"error": f"Failed to read history file from S3: {e}"}), 500

# --- Server Startup ---
def run_web_server():
    class ServerThread(threading.Thread):
        def __init__(self, app, host, port):
            threading.Thread.__init__(self)
            self.server = make_server(host, port, app, threaded=True)
            self.ctx = app.app_context(); self.ctx.push()
        def run(self): print(f"[*] Starting Dashboard API server on http://{HOST}:{WEB_PORT}"); self.server.serve_forever()
        def shutdown(self): self.server.shutdown()
    global server_thread
    server_thread = ServerThread(app, HOST, WEB_PORT)
    server_thread.start()

def resume_crashed_jobs():
    print("[*] Checking for crashed jobs to resume from S3...")
    bucket, prefix = parse_vfs_path('fs://job/')
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        # Tìm các job directories bằng cách list các "thư mục con" trong 'job/'
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter='/')
        job_prefixes = [p['Prefix'] for page in pages for p in page.get('CommonPrefixes', [])]
    except Exception as e:
        print(f"[!] Could not list jobs in S3 to check for resume: {e}")
        return

    for job_prefix in job_prefixes:
        checkpoint_key = f"{job_prefix}checkpoint.json"
        try:
            # Kiểm tra sự tồn tại của checkpoint.json
            s3_client.head_object(Bucket=bucket, Key=checkpoint_key)
            print(f"[!] Found S3 checkpoint for crashed job: {job_prefix}. Attempting to resume.")
            if JOB_IN_PROGRESS.acquire(blocking=False):
                try:
                    response = s3_client.get_object(Bucket=bucket, Key=checkpoint_key)
                    checkpoint_data = json.load(response['Body'])
                    
                    global job, tasks_state, stage_state, stage_outputs, task_queue, sorted_stages, stage_dependencies
                    job = checkpoint_data['dag_payload']
                    tasks_state = checkpoint_data['tasks_state']
                    stage_state = checkpoint_data['stage_state']
                    stage_outputs = defaultdict(list, checkpoint_data['stage_outputs'])
                    task_queue = deque(checkpoint_data['task_queue'])
                    sorted_stages = checkpoint_data['sorted_stages']
                    stage_dependencies = checkpoint_data['stage_dependencies']

                    for tid, tinfo in list(tasks_state.items()):
                        if tinfo.get('status') in ['ASSIGNED', 'RUNNING']:
                            print(f"[*] Re-queueing task {tid} from resumed job.")
                            tinfo['status'] = 'PENDING'; tinfo['workerId'] = None
                            task_queue.appendleft({'taskId': tid, 'stageId': tinfo['stageId'], 'cmd': tinfo['cmd']})

                    print(f"[*] Resuming job controller for '{job['jobId']}'...")
                    threading.Thread(target=job_controller, args=(job, True)).start()
                    return # Chỉ resume một job mỗi lần khởi động
                except Exception as e:
                    print(f"[!!!] CRITICAL: Failed to resume job from S3 checkpoint {checkpoint_key}: {e}")
                    if JOB_IN_PROGRESS.locked(): JOB_IN_PROGRESS.release()
            else:
                print("[!] Server is already busy. Cannot resume job at this time.")
                break
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                print(f"[!] Error checking checkpoint {checkpoint_key}: {e}")
            continue # Bỏ qua nếu không có checkpoint

def main():
    threading.Thread(target=run_web_server, daemon=True).start()
    threading.Thread(target=monitor_workers, daemon=True).start()
    threading.Thread(target=assign_tasks, daemon=True).start()
    
    resume_crashed_jobs()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind((HOST, PORT)); s.listen()
        print(f"[*] Server listening on {HOST}:{PORT}, waiting for workers and job submissions...")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_connection, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    main()