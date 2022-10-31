import argparse
import glob
import os

import pandas


def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--src', type=str, required=True,
                        metavar='./source/',
                        help='source path')
    parser.add_argument('--dst', type=str, required=True,
                        metavar='./destination/',
                        help='destination path')
    args = parser.parse_args()

    print(args)

    all_src_file_paths = [x for x in sorted(glob.glob(os.path.join(args.src, '*')))]

    for i, src_file_path in enumerate(all_src_file_paths):
        dst_file_name = os.path.split(src_file_path)[-1][:-4] + "parquet"  # get file name, drop json, append parquet
        dst_file_path = os.path.join(args.dst, dst_file_name)

        print(f"Processing {dst_file_name}")

        with open(src_file_path, 'r') as src_file, open(dst_file_path, 'wb') as dst_file:
            df = pandas.read_json(src_file, lines=True)
            df.to_parquet(dst_file)


if __name__ == '__main__':
    main()
