import MDSplus
from traceback import print_exc
from collections.abc import Iterable
import argparse
import numpy as np
import os
import h5py
import yaml


"""
read_mdsplus_channel(shot_numbers=31779, tree_names='KSTAR',
                     point_names='EP53:FOO', server='203.230.126.231:8005',
                     resample=None, verbose=False)

Mostly copied from connection_test.py by D. Eldon
"""
def read_mdsplus_channel(shot_numbers=31779, tree_names='KSTAR',
                         point_names='EP53:FOO', server='203.230.126.231:8005',
                         resample=None, verbose=False):
    if isinstance(shot_numbers, int):
        shot_numbers = [shot_numbers]
    if isinstance(point_names, str):
        point_names = [point_names]
    point_names = [add_slash(pn) for pn in point_names]
    if isinstance(tree_names, str):
        tree_names = [tree_names] * len(point_names)
    elif len(tree_names) != len(point_names):
        raise ValueError('tree_names and point_names must be the same length')
    
    try:
        conn = MDSplus.Connection(server)
        if verbose:
            print(f"MDSplus connection status = {conn}")
    except BaseException:
        print_exc()
        return None
    data_dict = {}
    for sn in shot_numbers:
        data_dict[sn] = {tree: {} for tree in tree_names}
        for tree, pn in zip(tree_names, point_names):
            try:
                if verbose:
                    print(f"    Opening tree {tree} at shot number {sn}...")
                conn.openTree(tree, sn)
            except BaseException:
                print_exc()
                return None
            try:
                if verbose:
                    print(f"        Reading signal {pn}")
                signal = conn.get(add_resample(pn, resample))
                data = signal.data()
                units = conn.get(units_of(pn)).data()
                data_dict[sn][tree][pn] = {'data': data, 'units': units}
                for ii in range(np.ndim(data)):
                    try:
                        dim = conn.get(add_resample(dim_of(pn, ii), resample)).data()
                        data_dict[sn][tree][pn][f'dim{ii}'] = dim
                    except BaseException:
                        print_exc()
                        pass
            except BaseException:
                print_exc()
                return None
    return data_dict


def add_slash(s):
    ss = "\\" + s
    return r'' + ss.encode('unicode_escape').decode('utf-8')[1:]


def add_resample(pn, resample):
    if resample is None:
        return pn
    if isinstance(resample, dict):
        resample = [resample['start'], resample['stop'], resample['increment']]
    return f"resample({pn}, {resample[0]}, {resample[1]}, {resample[2]})"


def dim_of(pn, ii):
    return f"dim_of({pn}, {ii})"


def units_of(pn):
    return f"units_of({pn})"


def get_args():
    parser = argparse.ArgumentParser(description='Read MDSplus channel')
    parser.add_argument('-n', '--shot_numbers', type=int, help='Shot number(s)')
    parser.add_argument('-t', '--tree_names', nargs='+', help='Tree name(s)')
    parser.add_argument('-p', '--point_names', nargs='+', help='Point name(s)')
    parser.add_argument('-s', '--server', default='203.230.126.231:8005',
                        help='Server address. Default is 203.230.126.231:8005')
    parser.add_argument('-r', '--resample', nargs='+', type=float, default=None,
                        help='Resample signal(s) by providing a list of start, stop, '
                             'and increment values. For negative value, enclose them '
                             'withing double quotes and add a space at the beginning.'
                             'Example: --resample " -0.1" 10.0 0.1')
    parser.add_argument('-o', '--out_filename', default=None,
                        help='Output filename for saving data in file. Default is '
                             'None. in which case it does not save files.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Print verbose messages')
    parser.add_argument('-c', '--config', default=None, type=str,
                        help='Configuration file containing shot_numbers, tree_names, '
                             'point_names, and server. If provided, these arguments '
                             'are ignored.')
    args = parser.parse_args()
    if args.config is not None:
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)
        for key in config:
            if key in args:
                setattr(args, key, config[key])
    return args


if __name__ == '__main__':
    args = get_args()
    data_dict = read_mdsplus_channel(shot_numbers=args.shot_numbers,
                                     tree_names=args.tree_names,
                                     point_names=args.point_names,
                                     server=args.server,
                                     resample=args.resample,
                                     verbose=args.verbose)
    if args.out_filename is not None:
        with h5py.File(args.out_filename, 'w') as f:
            for sn in data_dict:
                f.create_group(str(sn))
                for tree in data_dict[sn]:
                    f[str(sn)].create_group(tree)
                    for pn in data_dict[sn][tree]:
                        f[str(sn)][tree].create_group(pn)
                        for key in data_dict[sn][tree][pn]:
                            if key != 'units':
                                f[str(sn)][tree][pn].create_dataset(key,
                                        data=data_dict[sn][tree][pn][key])
                            else:
                                f[str(sn)][tree][pn].attrs[key] = \
                                                            data_dict[sn][tree][pn][key]