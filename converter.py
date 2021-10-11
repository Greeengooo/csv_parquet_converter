import pandas as pd
import pyarrow.parquet as pq
import argparse
import os


def main(func=None, files=None):
    if func == 'csv2parquet':
        validate_files(files)
        csv_to_parquet(files[0], files[1])
    elif func == 'parquet2csv':
        validate_files(files)
        parquet_to_csv(files[0], files[1])
    elif func == 'get_schema':
        validate_files(files[0])
        get_schema(files[0][0])


def csv_to_parquet(source, destination):
    df = pd.read_csv(source)
    print(df.head)
    df.to_parquet(destination)


def parquet_to_csv(source, destination):
    df = pd.read_parquet(source)
    print(df.head)
    df.to_csv(destination)


def get_schema(source):
    pfile = pq.read_table(source)
    print('Schema: {}'.format(pfile.schema))


def validate_files(file_lst):
    if not os.path.isfile(file_lst[0]):
        raise Exception('File not found')
    if len(file_lst) > 1 and os.path.isfile(file_lst[1]):
        raise Exception('File already exists')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='converter', argument_default=argparse.SUPPRESS)
    parser.add_argument('--csv2parquet', action='store', nargs=2, metavar=('src', 'dst'),
                        help='convert csv file to parquet')
    parser.add_argument('--parquet2csv', action='store', nargs=2, metavar=('src', 'dst'),
                        help='convert parquet file to parquet')
    parser.add_argument('--get-schema', action='append', nargs=1, metavar='src',
                        help='get the schema of parquet file')
    args = vars(parser.parse_args())
    command = list(args.keys())[0]
    files = list(args.values())[0]
    main(command, files)
