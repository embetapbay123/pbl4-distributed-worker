import argparse
import json
import os
import sys

def main():
    parser = argparse.ArgumentParser(description="Reverses each line in a file.")
    parser.add_argument('--in', required=True, dest='input_file', help="Input file path.")
    parser.add_argument('--out', required=True, dest='output_file', help="Output file path.")
    args = parser.parse_args()
    # sys.exit(1) # check xem loi co retry k
    try:
        # check xem ton tai folder output ch
        os.makedirs(os.path.dirname(args.output_file), exist_ok=True)
        with open(args.input_file, 'r', encoding='utf-8') as infile, \
             open(args.output_file, 'w', encoding='utf-8') as outfile:
            for line in infile:
                reversed_line = line.strip()[::-1]
                outfile.write(reversed_line + '\n')
        # Quan trọng: In ra danh sách file output để server theo dõi
        print(json.dumps([args.output_file]))

    except Exception as e:
        # in loi
        print(f"Error processing {args.input_file}: {e}", file=sys.stderr)
        exit(1)

if __name__ == "__main__":
    main()