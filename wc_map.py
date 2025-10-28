# pbl4_demo/wc_map.py
import sys
import argparse
import json
import re
from collections import defaultdict
import os

def iter_range_lines(path, start, end):
    """Đọc các dòng trong một khoảng byte, căn chỉnh theo dòng."""
    with open(path, 'rb') as f:
        f.seek(start)
        # Nếu start > 0, chúng ta có thể đang ở giữa một dòng.
        # Đọc và bỏ qua dòng đó để tránh xử lý một phần.
        if start > 0:
            f.readline()
        
        while f.tell() <= end:
            line = f.readline()
            if not line:
                break
            # Chỉ xử lý dòng nếu nó bắt đầu trong khoảng [start, end]
            if f.tell() - len(line) > end:
                break
            yield line.decode('utf-8', errors='ignore')

def main():
    parser = argparse.ArgumentParser(description="Map function for WordCount")
    parser.add_argument('--uri', required=True, help="Input file URI")
    parser.add_argument('--start', type=int, default=0, help="Start byte")
    parser.add_argument('--end', type=int, required=True, help="End byte")
    parser.add_argument('--task_id', required=True, help="Task ID for output naming")
    parser.add_argument('--out_dir', required=True, help="Output directory for intermediate files")
    parser.add_argument('--num_reducers', type=int, required=True, help="Number of reducers")
    args = parser.parse_args()

    # Tạo thư mục output nếu chưa tồn tại
    os.makedirs(args.out_dir, exist_ok=True)

    # Dùng dict để nhóm các từ cho mỗi reducer
    # intermediate_data[reducer_index] = {word: count}
    intermediate_data = [defaultdict(int) for _ in range(args.num_reducers)]

    # Xử lý từng dòng trong khoảng byte được giao
    for line in iter_range_lines(args.uri, args.start, args.end):
        words = re.findall(r'\w+', line.lower())
        for word in words:
            # Phân phối từ cho reducer dựa trên hash
            reducer_index = hash(word) % args.num_reducers
            intermediate_data[reducer_index][word] += 1
    
    output_files = []
    # Ghi kết quả trung gian ra các file JSON
    for i in range(args.num_reducers):
        if intermediate_data[i]: # Chỉ ghi nếu có dữ liệu
            output_path = os.path.join(args.out_dir, f"{args.task_id}-part-{i}.json")
            with open(output_path, 'w') as f:
                json.dump(dict(intermediate_data[i]), f)
            output_files.append(output_path)
            
    # In ra stdout danh sách các file đã tạo, server sẽ đọc output này
    print(json.dumps(output_files))

if __name__ == "__main__":
    main()