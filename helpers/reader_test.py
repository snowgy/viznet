import pandas as pd 
import os
from os import listdir
from os.path import join
from collections import OrderedDict
import argparse
import gzip
import json

raw_data_dir = '/home/cc/my_mounting_point/dataset_storage'
output_dir = '/home/cc/opendata_extracted'

data_dirs = {
    'plotly': join(raw_data_dir, 'plotly'),
    'manyeyes': join(raw_data_dir, 'manyeyes'),
    'webtables': join(raw_data_dir, 'webtables'),
    'opendata': join(raw_data_dir, 'open_data_portals')
}

def is_html(full_dataset_path):
    with open(full_dataset_path, 'r', errors='ignore') as f:
        head = f.readline().rstrip()
        for t in [ '<body>', 'html', 'DOCTYPE' ]:
            if t in head:
                return True
    return False

def get_all_portals():
    corpus = 'opendata'
    base_dir = data_dirs[corpus] 
    f = open('portals.txt', 'w')     
    for portal_dir in listdir(base_dir):
        f.write(portal_dir + '\n')

get_all_portals()
# full_dataset_path = '/home/cc/my_mounting_point/dataset_storage/open_data_portals/www_data_gouv_fr/53ba561ba3a729219b7beae4/www_data_gouv_fr___53ba561ba3a729219b7beae4___0_7.csv'
# print(is_html(full_dataset_path))
# sep = ','
# engine = 'c'
# encoding = 'utf-8'
# df = pd.read_csv(
#                 full_dataset_path,
#                 engine=engine,  # https://github.com/pandas-dev/pandas/issues/11166
#                 error_bad_lines=False,
#                 warn_bad_lines=False,
#                 encoding=encoding,
#                 sep=sep
#             )
# print(len(df.columns))

        
