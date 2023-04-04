from pathlib import Path
from collections import OrderedDict

def resolve_path(path, as_str=True):
    abspath = Path(path).resolve()
    if as_str:
        return abspath.as_posix()
    return abspath


def read_list(file_path, drop_duplicates=False):
    with open(file_path,'r') as f:
        url_list = f.read().splitlines()

    if drop_duplicates:
        return [*OrderedDict.fromkeys(url_list)]
    
    return url_list

def write_list(item_list, file_path, drop_duplicates=False):
    if drop_duplicates:
        item_list = [*OrderedDict.fromkeys(item_list)]
    
    with open(file_path, 'w') as f:
        f.writelines([item.strip()+'\n' for item in item_list])
        