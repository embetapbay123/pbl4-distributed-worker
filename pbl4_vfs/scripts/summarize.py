# pbl4_demo/summarize.py
import argparse
import os
import glob
from collections import defaultdict

def main():
    parser = argparse.ArgumentParser(description="Summarize word counts from multiple part-files.")
    parser.add_argument('--in_dir', required=True, help="Input directory containing part-files.")
    parser.add_argument('--out', required=True, help="Final single output file path.")
    args = parser.parse_args()

    input_files = sorted(glob.glob(os.path.join(args.in_dir, "part-*.txt")))

    if not input_files:
        print(f"Warning: No part-files found in {args.in_dir}")
        open(args.out, 'w').close()
        return

    # Sử dụng một dictionary để tổng hợp lại tất cả các count
    # defaultdict(int) sẽ tự động khởi tạo giá trị là 0 cho key mới
    final_counts = defaultdict(int)

    # Lặp qua từng file và cập nhật vào dictionary
    for filename in input_files:
        with open(filename, 'r') as infile:
            for line in infile:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    # Tách dòng thành từ và số đếm, ví dụ: "hello: 225"
                    word, count_str = line.split(': ')
                    count = int(count_str)
                    
                    # Cộng dồn vào tổng cuối cùng
                    final_counts[word] += count
                except ValueError:
                    print(f"Warning: Could not parse line in {filename}: {line}")
                    continue

    # Sắp xếp kết quả theo từ (alphabetical) trước khi ghi ra file
    sorted_words = sorted(final_counts.keys())

    # Ghi kết quả cuối cùng đã được tổng hợp
    with open(args.out, 'w') as outfile:
        for word in sorted_words:
            outfile.write(f"{word}: {final_counts[word]}\n")
    
    print(f"Successfully summarized and aggregated counts from {len(input_files)} files into {args.out}")

if __name__ == "__main__":
    main()