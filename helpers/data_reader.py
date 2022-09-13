
'''
Helper functions to read through raw data for each corpus. Each
is a generator that yields a single dataset and metadata in the form:

{
    'df'
    'locator',
    'dataset_id'
}
'''
import os
from os import listdir
from os.path import join
from collections import OrderedDict
import argparse
import gzip
import json
# import chardet
import traceback

import numpy as np
import pandas as pd
# from feature_extraction.general_helpers import clean_chunk
# from feature_extraction.type_detection import detect_field_type, data_type_to_general_type, data_types, general_types

raw_data_dir = '/home/cc/my_mounting_point/dataset_storage'
output_dir = '/home/cc/opendata_extracted'

data_dirs = {
    'plotly': join(raw_data_dir, 'plotly'),
    'manyeyes': join(raw_data_dir, 'manyeyes'),
    'webtables': join(raw_data_dir, 'webtables'),
    'opendata': join(raw_data_dir, 'open_data_portals')
}


CHUNK_SIZE = 500

def is_gz_file(filepath):
    with open(filepath, 'rb') as test_f:
        return test_f.read(2) == b'\x1f\x8b'

def is_html(full_dataset_path):
    with open(full_dataset_path, 'r', errors='ignore') as f:
        head = f.readline().rstrip()
        for t in [ '<body>', 'html', 'DOCTYPE' ]:
            if t in head:
                return True
    return False

def get_opendata_dfs_portal(portal_dir):
    corpus = 'opendata'
    base_dir = data_dirs[corpus]       
    files = []
    gzip_log = open('~/metadata/gzip_csv_{}.txt'.format(portal_dir), 'w')
    html_log = open('~/metadata/html_csv_{}.txt'.format(portal_dir), 'w')
    non_utf8_log = open('~/metadata/non_utf8_csv_{}.txt'.format(portal_dir), 'w')
    continue_point = None
    skip = True
    full_portal_dir = join(base_dir, portal_dir)
    for dataset_id_dir in listdir(full_portal_dir):
        full_dataset_id_dir = join(full_portal_dir, dataset_id_dir)
        for dataset_name in listdir(full_dataset_id_dir): 
            full_dataset_path = join(full_dataset_id_dir, dataset_name)
            if continue_point and full_dataset_path == continue_point:
                skip = False
            if continue_point and skip:
                continue

            locator = join(portal_dir, dataset_id_dir)
            dataset_id = '{}__{}'.format(dataset_id_dir, dataset_name)

            engine = 'c'
            encoding = 'utf-8'
            sep=','
    
            print("processing", full_dataset_path)

            try:
                if is_gz_file(full_dataset_path):
                    print("met gzip csv!")
                    gzip_log.write(full_dataset_path + '\n')
                    continue
        
                if is_html(full_dataset_path):
                    print("met html file!")
                    html_log.write(full_dataset_path + '\n')
                    continue
            except Exception as e:
                    print(e)
                    continue

            while True:
                try:
                    # print(sep)
                    df = pd.read_csv(
                        full_dataset_path,
                        engine=engine,  # https://github.com/pandas-dev/pandas/issues/11166
                        error_bad_lines=False,
                        warn_bad_lines=False,
                        encoding=encoding,
                        sep=sep
                    )

                    num_fields = len(df.columns)
                    print(num_fields)
                    if num_fields == 1 and sep != ':':
                        if sep == ',': 
                            sep=';'
                            continue
                        if sep == ';': 
                            sep='\t'
                            continue
                        if sep == '\t': 
                            sep=':'
                            continue

                    else:
                        # if exact_num_fields:
                        #     if num_fields != exact_num_fields: continue
                        # if max_fields:
                        #     if num_fields > max_fields: continue
                        result = {
                            'df': df,
                            'dataset_id': dataset_name,
                            'locator': locator
                        }

                        yield result
                        break

                except UnicodeDecodeError as ude:
                    print("encoding error")
                    non_utf8_log.write(full_dataset_path + '\n')
                    break
                    # print("try latin1")
                    # encoding = 'latin-1'

                except pd.errors.ParserError as cpe:
                    print("parse error")
                    if engine != 'python':
                        print("try engine python")
                        engine = 'python'
                    else:
                        break
                except Exception as e:
                    print(e)
                    break
def get_opendata_dfs(exact_num_fields=None, min_fields=None, max_fields=None):
    corpus = 'opendata'
    base_dir = data_dirs[corpus]       
    files = []
    gzip_log = open('gzip_csv.txt', 'w')
    html_log = open('html_csv.txt', 'w')
    non_utf8_log = open('non_utf8_csv.txt', 'w')
    continue_point = None
    skip = True
    
    for portal_dir in listdir(base_dir):
        full_portal_dir = join(base_dir, portal_dir)
        for dataset_id_dir in listdir(full_portal_dir):
            full_dataset_id_dir = join(full_portal_dir, dataset_id_dir)
            for dataset_name in listdir(full_dataset_id_dir): 
                full_dataset_path = join(full_dataset_id_dir, dataset_name)
                if continue_point and full_dataset_path == continue_point:
                    skip = False
                if continue_point and skip:
                    continue

                locator = join(portal_dir, dataset_id_dir)
                dataset_id = '{}__{}'.format(dataset_id_dir, dataset_name)

                engine = 'c'
                encoding = 'utf-8'
                sep=','
      
                print("processing", full_dataset_path)

                try:
                    if is_gz_file(full_dataset_path):
                        print("met gzip csv!")
                        gzip_log.write(full_dataset_path + '\n')
                        continue
            
                    if is_html(full_dataset_path):
                        print("met html file!")
                        html_log.write(full_dataset_path + '\n')
                        continue
                except Exception as e:
                        print(e)
                        continue
    
                while True:
                    try:
                        # print(sep)
                        df = pd.read_csv(
                            full_dataset_path,
                            engine=engine,  # https://github.com/pandas-dev/pandas/issues/11166
                            error_bad_lines=False,
                            warn_bad_lines=False,
                            encoding=encoding,
                            sep=sep
                        )

                        num_fields = len(df.columns)
                        print(num_fields)
                        if num_fields == 1 and sep != ':':
                            if sep == ',': 
                                sep=';'
                                continue
                            if sep == ';': 
                                sep='\t'
                                continue
                            if sep == '\t': 
                                sep=':'
                                continue

                        else:
                            # if exact_num_fields:
                            #     if num_fields != exact_num_fields: continue
                            # if max_fields:
                            #     if num_fields > max_fields: continue
                            result = {
                                'df': df,
                                'dataset_id': dataset_name,
                                'locator': locator
                            }

                            yield result
                            break

                    except UnicodeDecodeError as ude:
                        print("encoding error")
                        non_utf8_log.write(full_dataset_path + '\n')
                        break
                        # print("try latin1")
                        # encoding = 'latin-1'

                    except pd.errors.ParserError as cpe:
                        print("parse error")
                        if engine != 'python':
                            print("try engine python")
                            engine = 'python'
                        else:
                            break
                    except Exception as e:
                        print(e)
                        break

# get_dfs_by_corpus = {
#     'plotly': get_plotly_dfs,
#     'manyeyes': get_manyeyes_dfs,
#     'webtables': get_webtables_dfs,
#     'opendata': get_opendata_dfs
# }

# res = is_html('/home/cc/my_mounting_point/dataset_storage/open_data_portals/www_yorkopendata_org/ffe87c2d-76c9-4c7a-acdf-a1f00b9fdd32/www_yorkopendata_org___ffe87c2d-76c9-4c7a-acdf-a1f00b9fdd32___0_0.csv')
# print(res)
cnt = 0
for obj in get_opendata_dfs():
    df = obj['df']
    dataset_name = obj['dataset_id']
    file_out_path = '{}/{}'.format(output_dir, dataset_name)
    print("saving", file_out_path)
    df.to_csv(file_out_path, sep=',')
    cnt += 1
    print('datasets cnt:', cnt)