# pbl4_demo/merge_files.py
import argparse
import os
import glob
import json
import sys

def main():
    parser = argparse.ArgumentParser(description="Merges/concatenates all files in a directory into a single output file.")
    parser.add_argument('--in_dir', required=True, help="Input directory containing files to merge.")
    parser.add_argument('--out', required=True, help="Final single output file path.")
    args = parser.parse_args()

    # Tìm tất cả các file trong thư mục input (dùng '*' để lấy tất cả)
    input_files = sorted(glob.glob(os.path.join(args.in_dir, "*")))

    if not input_files:
        print(f"Warning: No files found in {args.in_dir}")
        open(args.out, 'w').close()
        # Vẫn phải in ra stdout để server biết task đã xong
        print(json.dumps([args.out]))
        return

    # Mở file output và ghi nội dung từ tất cả các file input
    with open(args.out, 'w', encoding='utf-8') as outfile:
        for filename in input_files:
            with open(filename, 'r', encoding='utf-8') as infile:
                outfile.write(infile.read())
                # Thêm một dòng mới giữa các file cho dễ đọc
                outfile.write('\n')
    
    # In ra stdout đường dẫn file output để server xác nhận
    print(json.dumps([args.out]))
    print(f"Successfully merged {len(input_files)} files into {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()