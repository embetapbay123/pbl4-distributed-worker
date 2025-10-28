# pbl4_demo/worker.py
import socket
import json
import struct
import time
import threading
import subprocess
import uuid

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 65432
WORKER_ID = f"W-{uuid.uuid4().hex[:6]}"
SLOTS = 10 
HEARTBEAT_INTERVAL = 10

# --- Helper Functions ---
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
    cmd = task_info['cmd']
    
    print(f"[*] Starting task {task_id}: {cmd}")
    send_message(sock, {"type": "TASK_UPDATE", "taskId": task_id, "status": "RUNNING"})

    try:
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding='utf-8', errors='ignore')
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

def send_heartbeat(sock):
    while True:
        try:
            send_message(sock, {"type": "HEARTBEAT", "workerId": WORKER_ID})
            time.sleep(HEARTBEAT_INTERVAL)
        except:
            print("[!] Heartbeat failed. Disconnecting.")
            break

def main():
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