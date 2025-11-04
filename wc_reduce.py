# pbl4_demo/wc_reduce.py
import sys
import argparse
import json
from collections import defaultdict

def main():
    parser = argparse.ArgumentParser(description="Reduce function for WordCount")
    parser.add_argument('--inputs', required=True, help="Comma-separated list of input files")
    parser.add_argument('--out', required=True, help="Final output file path")
    args = parser.parse_args()

    final_counts = defaultdict(int)
    input_files = args.inputs.split(',')

    # Đọc tất cả các file trung gian được gán cho reducer này
    for file_path in input_files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                for word, count in data.items():
                    final_counts[word] += count
        except FileNotFoundError:
            # Bỏ qua nếu file không tồn tại (có thể do mapper không tạo ra output cho partition này)
            continue
    
    # Sắp xếp kết quả theo số lần xuất hiện
    sorted_counts = sorted(final_counts.items(), key=lambda item: item[1], reverse=True)

    # Ghi kết quả cuối cùng
    with open(args.out, 'w') as f:
        for word, count in sorted_counts:
            f.write(f"{word}: {count}\n")
            
    # In ra stdout đường dẫn file output, server sẽ đọc để xác nhận
    print(json.dumps([args.out]))

if __name__ == "__main__":
    main()