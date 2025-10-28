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
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from werkzeug.serving import make_server
from werkzeug.utils import secure_filename

HOST = '127.0.0.1'
PORT = 65432
WEB_PORT = 5000
VFS_ROOT = os.path.abspath('./pbl4_vfs')
MAX_TASK_RETRIES = 3
WORKER_TIMEOUT = 30

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

# --- Helper Functions ---
def resolve_vfs(path):
    if path.startswith('fs://'):
        return os.path.join(VFS_ROOT, path[5:].replace('/', os.sep))
    return path

def resolve_script_path(script_name):
    vfs_script_path = f"fs://scripts/{script_name}"
    real_script_path = resolve_vfs(vfs_script_path)
    if os.path.exists(real_script_path):
        print(f"[*] Found user-provided script: {real_script_path}")
        return f'"{real_script_path}"'
    print(f"[*] Using local script: {script_name}")
    return script_name

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
            map_out_dir_raw = resolve_vfs(f"fs://job/{job['jobId']}/{stage_id}/")
            os.makedirs(map_out_dir_raw, exist_ok=True)
            map_out_dir = map_out_dir_raw.rstrip(os.sep)
            mapper_script = resolve_script_path(spec["mapper"])
            for input_path in spec['inputs']:
                full_path = resolve_vfs(input_path)
                if not os.path.exists(full_path) or os.path.isdir(full_path):
                    print(f"[!] Input file not found or is a directory: {full_path}"); continue
                file_size = os.path.getsize(full_path)
                if file_size == 0: continue
                policy = spec.get('splitPolicy', {})
                threshold = policy.get('thresholdBytes', 128*1024*1024)
                split_size = policy.get('splitSizeBytes', 64*1024*1024)
                if file_size <= threshold:
                    task_id = f"T-{uuid.uuid4().hex[:8]}"
                    cmd = f'python {mapper_script} --uri "{full_path}" --start 0 --end {file_size-1} --task_id {task_id} --out_dir "{map_out_dir}" --num_reducers {num_reducers}'
                    task_queue.append({'taskId': task_id, 'stageId': stage_id, 'cmd': cmd}); tasks_state[task_id] = {'status': 'PENDING', 'stageId': stage_id, 'cmd': cmd, 'retries': 0}
                else:
                    num_splits = (file_size + split_size - 1) // split_size
                    for i in range(num_splits):
                        start = i * split_size; end = min((i + 1) * split_size - 1, file_size - 1)
                        task_id = f"T-{uuid.uuid4().hex[:8]}"
                        cmd = f'python {mapper_script} --uri "{full_path}" --start {start} --end {end} --task_id {task_id} --out_dir "{map_out_dir}" --num_reducers {num_reducers}'
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
                output_file = resolve_vfs(f"fs://job/{job['jobId']}/{stage_id}/part-{i}.txt")
                os.makedirs(os.path.dirname(output_file), exist_ok=True)
                cmd = f'python {reducer_script} --inputs {inputs_str} --out "{output_file}"'
                task_queue.append({'taskId': task_id, 'stageId': stage_id, 'cmd': cmd}); tasks_state[task_id] = {'status': 'PENDING', 'stageId': stage_id, 'cmd': cmd, 'retries': 0}

    elif stage['type'] == 'single':
        raw_cmd = spec['cmd']
        def resolve_cmd_script(match):
            prefix = match.group(1)
            script = match.group(2)
            return f"{prefix}{resolve_script_path(script)}"
        cmd_with_resolved_script = re.sub(r'(python\s+)([a-zA-Z0-9_.\-]+)', resolve_cmd_script, raw_cmd, 1)
        def replacer(match): path = resolve_vfs(match.group(1)).rstrip(os.sep); return f'"{path}"'
        resolved_cmd = re.sub(r'"(fs://[^"]+)"', replacer, cmd_with_resolved_script)
        task_id = f"T-{uuid.uuid4().hex[:8]}"
        task_queue.append({'taskId': task_id, 'stageId': stage_id, 'cmd': resolved_cmd}); tasks_state[task_id] = {'status': 'PENDING', 'stageId': stage_id, 'cmd': resolved_cmd, 'retries': 0}

    elif stage['type'] == 'bot':
        cmd_template = spec['cmd_template']
        def resolve_cmd_script(match):
            prefix = match.group(1)
            script = match.group(2)
            return f"{prefix}{resolve_script_path(script)}"
        resolved_template = re.sub(r'(python\s+)([a-zA-Z0-9_.\-]+)', resolve_cmd_script, cmd_template, 1)
        output_dir = resolve_vfs(spec['output_dir'])
        os.makedirs(output_dir, exist_ok=True)
        for input_vfs_path in spec['inputs']:
            input_real_path = resolve_vfs(input_vfs_path); base_name = os.path.basename(input_real_path)
            output_real_path = os.path.join(output_dir, base_name)
            cmd = resolved_template.replace('{input}', f'"{input_real_path}"').replace('{output}', f'"{output_real_path}"')
            task_id = f"T-{uuid.uuid4().hex[:8]}"
            task_queue.append({'taskId': task_id, 'stageId': stage_id, 'cmd': cmd}); tasks_state[task_id] = {'status': 'PENDING', 'stageId': stage_id, 'cmd': cmd, 'retries': 0}
            print(f"[*] Generated BoT task {task_id}")

def check_stage_completion(stage_id):
    tasks_in_stage = [tid for tid, t in tasks_state.items() if t.get('stageId') == stage_id]
    if not tasks_in_stage: return False
    if all(tasks_state.get(tid, {}).get('status') == 'SUCCEEDED' for tid in tasks_in_stage):
        stage_state[stage_id] = 'SUCCEEDED'; print(f"\n[+] Stage '{stage_id}' completed successfully!\n"); return True
    return False

def job_controller(dag_payload):
    JOB_CANCEL_EVENT.clear()
    try:
        reset_job_state(); load_dag_from_payload(dag_payload)
        for stage_id in sorted_stages:
            if JOB_CANCEL_EVENT.is_set(): print("[*] Job cancelled by user."); stage_state[stage_id] = 'CANCELLED'; break
            print(f"[*] Starting stage '{stage_id}'..."); stage_state[stage_id] = 'RUNNING'
            for dep_id in stage_dependencies.get(stage_id, []):
                while stage_state.get(dep_id) != 'SUCCEEDED':
                    if JOB_CANCEL_EVENT.is_set(): break
                    time.sleep(1)
            if JOB_CANCEL_EVENT.is_set(): break
            generate_tasks_for_stage(stage_id)
            while stage_state.get(stage_id) != 'SUCCEEDED':
                if JOB_CANCEL_EVENT.is_set(): break
                if check_stage_completion(stage_id): break
                tasks_for_stage_exist = any(t['stageId'] == stage_id for t in tasks_state.values())
                if not tasks_for_stage_exist and not any(t['stageId'] == stage_id for t in task_queue):
                     stage_state[stage_id] = 'SUCCEEDED'; print(f"\n[+] Stage '{stage_id}' completed (no tasks generated).\n"); break
                time.sleep(1)
        if JOB_CANCEL_EVENT.is_set(): print("[***] JOB CANCELLED! [***]")
        else: print("[***] JOB COMPLETED SUCCESSFULLY! [***]")
    except Exception as e: print(f"[!!!] JOB FAILED: {e}")
    finally:
        if JOB_IN_PROGRESS.locked(): JOB_IN_PROGRESS.release()
        print("[*] Job lock released.")

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
                        log_dir = resolve_vfs(f"fs://job/{job['jobId']}/logs/")
                        os.makedirs(log_dir, exist_ok=True)
                        with open(os.path.join(log_dir, f"{task_id}.log"), 'w', encoding='utf-8') as f:
                            f.write("--- STDOUT ---\n"); f.write(msg.get('stdout', '')); f.write("\n\n--- STDERR ---\n"); f.write(msg.get('stderr', ''))
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
    log_file_path = resolve_vfs(f"fs://job/{job['jobId']}/logs/{task_id}.log")
    if os.path.exists(log_file_path):
        with open(log_file_path, 'r', encoding='utf-8') as f: content = f.read()
        return content, 200, {'Content-Type': 'text/plain; charset=utf-8'}
    else: return "Log not found.", 404

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
    upload_vfs_path = f'fs://{target_folder}/'
    upload_folder_real = resolve_vfs(upload_vfs_path)
    os.makedirs(upload_folder_real, exist_ok=True)
    file_path = os.path.join(upload_folder_real, filename)
    file.save(file_path)
    print(f"[*] User uploaded {filename} to {target_folder}")
    return jsonify({"status": "OK", "message": f"File '{filename}' uploaded successfully to fs://{target_folder}/."})

@app.route('/api/vfs/download', methods=['GET'])
def download_file():
    vfs_path = request.args.get('path')
    if not vfs_path:
        return "Missing 'path' parameter.", 400
    try:
        real_path = resolve_vfs(vfs_path)
        if not os.path.abspath(real_path).startswith(os.path.abspath(VFS_ROOT)):
             return "Access denied: Cannot download files outside the VFS root.", 403
        if os.path.exists(real_path) and os.path.isfile(real_path):
            directory = os.path.dirname(real_path)
            filename = os.path.basename(real_path)
            return send_from_directory(directory, filename, as_attachment=True)
        else:
            return "File not found.", 404
    except Exception as e:
        return f"An error occurred: {e}", 500

@app.route('/api/vfs/list', methods=['GET'])
def list_vfs_path():
    vfs_path_str = request.args.get('path', 'fs://')
    if not vfs_path_str.startswith('fs://'):
        return jsonify({"error": "Invalid VFS path"}), 400
    try:
        real_path = resolve_vfs(vfs_path_str)
        if not os.path.abspath(real_path).startswith(os.path.abspath(VFS_ROOT)):
            return jsonify({"error": "Access denied"}), 403
        if not os.path.exists(real_path) or not os.path.isdir(real_path):
            return jsonify({"error": "Path not found or is not a directory"}), 404
        items = []
        for name in sorted(os.listdir(real_path)):
            item_path = os.path.join(real_path, name)
            is_dir = os.path.isdir(item_path)
            items.append({"name": name, "type": "directory" if is_dir else "file", "size": os.path.getsize(item_path) if not is_dir else 0})
        return jsonify({"path": vfs_path_str, "contents": items})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/vfs/delete', methods=['POST'])
def delete_vfs_path():
    data = request.get_json(silent=True)
    if not data:
        return jsonify({"status": "FAIL", "message": "Invalid or missing JSON payload."}), 400
    vfs_path_str = data.get('path')
    if not vfs_path_str:
        return jsonify({"status": "FAIL", "message": "Path is required in JSON payload."}), 400
    try:
        real_path = resolve_vfs(vfs_path_str)
        if not os.path.abspath(real_path).startswith(os.path.abspath(VFS_ROOT)):
            return jsonify({"status": "FAIL", "message": "Access denied."}), 403
        if not os.path.exists(real_path):
            return jsonify({"status": "FAIL", "message": "File or directory not found."}), 404
        if os.path.isdir(real_path):
            shutil.rmtree(real_path)
            message = f"Directory '{vfs_path_str}' deleted successfully."
        else:
            os.remove(real_path)
            message = f"File '{vfs_path_str}' deleted successfully."
        print(f"[*] Deleted VFS path: {vfs_path_str}")
        return jsonify({"status": "OK", "message": message})
    except Exception as e:
        return jsonify({"status": "FAIL", "message": str(e)}), 500

@app.route('/api/job/submit', methods=['POST'])
def submit_job_api():
    if JOB_IN_PROGRESS.acquire(blocking=False):
        try:
            dag_payload = request.json
            if not dag_payload or 'jobId' not in dag_payload:
                JOB_IN_PROGRESS.release(); return jsonify({"status": "FAIL", "message": "Invalid DAG JSON."}), 400
            threading.Thread(target=job_controller, args=(dag_payload,)).start()
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

def main():
    threading.Thread(target=run_web_server, daemon=True).start()
    threading.Thread(target=monitor_workers, daemon=True).start()
    threading.Thread(target=assign_tasks, daemon=True).start()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1); s.bind((HOST, PORT)); s.listen()
        print(f"[*] Server listening on {HOST}:{PORT}, waiting for workers and job submissions...")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_connection, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    main()