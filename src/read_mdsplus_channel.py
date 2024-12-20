import MDSplus
from traceback import print_exc
from collections.abc import Iterable
import argparse
import numpy as np
import os
import h5py
import yaml

def check_red(sn, tree, pn, red_dict):
    sns = str(sn)
    if sns in red_dict:
        if tree in red_dict[sns]:
            for red_pn in red_dict[sns][tree]:
                if add_slash(red_pn) == pn:
                    # print(f"Skipping {tree}: {pn} on shot {sns}")
                    return True


"""
read_mdsplus_channel(shot_numbers=31779, trees='KSTAR',
                     point_names='EP53:FOO', server='203.230.126.231:8005',
                     resample=None, verbose=False)

Mostly copied from connection_test.py by D. Eldon
"""
def read_mdsplus_channel(shot_numbers=31779, trees='KSTAR',
                         point_names='EP53:FOO', server='203.230.126.231:8005',
                         resample=None, verbose=False, config=None, red_config=None):
    if config is not None:
        with open(config, 'r') as f:
            config = yaml.safe_load(f)
        if 'shot_numbers' in config:
            shot_numbers = config['shot_numbers']
        if 'trees' in config:
            trees = config['trees']
        if 'point_names' in config:
            point_names = config['point_names']
        if 'server' in config:
            server = config['server']
        if 'resample' in config:
            resample = config['resample']
        if 'verbose' in config:
            verbose = config['verbose']
    if isinstance(shot_numbers, int):
        shot_numbers = [shot_numbers]
    if isinstance(point_names, str):
        point_names = [point_names]
    if isinstance(point_names, Iterable):
        point_names = [add_slash(pn) for pn in point_names]
    if isinstance(trees, str):
        tree_dict = {trees: point_names}
        # trees = [trees] * len(point_names)
    elif isinstance(trees, list):
        if len(trees) != len(point_names):
            raise ValueError('trees and point_names must be the same length')
        tree_dict = {tree: [] for tree in trees}
        for tree, pn in zip(trees, point_names):
            tree_dict[tree].append(pn)
    elif isinstance(trees, dict):
        tree_dict = {tree: [] for tree in trees}
        for tree in trees:
            if tree != "PTDATA":
                if isinstance(trees[tree], str):
                    tree_dict[tree] = [add_slash(trees[tree])]
                else:
                    tree_dict[tree] = [add_slash(pn) for pn in trees[tree]]    
    if red_config is not None:
        with open(red_config, 'r') as f:
            red_config = yaml.safe_load(f)
    else:
        red_config = {}
    try:
        conn = MDSplus.Connection(server)
        if verbose:
            print(f"MDSplus connection status = {conn}")
    except BaseException:
        print_exc()
        return None
    data_dict = {}
    for sn in shot_numbers:
        data_dict[sn] = {tree: {} for tree in tree_dict}
        for tree in tree_dict:
            if tree != "PTDATA":
                try:
                    if verbose:
                        print(f"    Opening tree {tree} at shot number {sn}...")
                    conn.openTree(tree, sn)
                except BaseException:
                    print_exc()
                    return None
            for pn in tree_dict[tree]:
                if check_red(sn, tree, pn, red_config):
                    continue
                try:
                    if verbose:
                        print(f"        Reading signal {pn}")
                    if tree == "PTDATA":
                        pn = 'PTDATA("' + pn + '")'
                        print(f"        Reading signal {pn}")
                    if pn.startswith("PTDATA"):
                        signal = conn.get(add_resample(pn[:-1] + f", {sn})", resample))
                    else:
                        signal = conn.get(add_resample(pn, resample))
                    data = signal.data()
                    units = conn.get(units_of(pn)).data()
                    data_dict[sn][tree][pn] = {'data': data, 'units': units}
                    for ii in range(np.ndim(data)):
                        try:
                            if resample is None or ii != 0:
                                dim = conn.get(dim_of(pn, ii)).data()
                            else:
                                dim = get_time_array(resample)
                            data_dict[sn][tree][pn][f'dim{ii}'] = dim
                        except BaseException as exc:
                            print("-------------------------------------------------")
                            print(f"Error in reading dim of {tree}: {pn} in shot "
                                  + f"number {sn}")
                            print(exc)
                            # print_exc()
                            print("-------------------------------------------------")
                            pass
                except BaseException as exc:
                    print("-------------------------------------------------")
                    print(f"Error in reading {tree}: {pn} in shot number {sn}")
                    print(exc)
                    print("-------------------------------------------------")
                    # print_exc()
                    pass
    return data_dict


def add_slash(s):
    if s.startswith("\\") or s.startswith("PTDATA"):
        return s
    ss = "\\" + s
    return r'' + ss.encode('unicode_escape').decode('utf-8')[1:]


def add_resample(pn, resample):
    if resample is None:
        return pn
    if isinstance(resample, dict):
        resample = [resample['start'], resample['stop'], resample['increment']]
    return f"resample({pn}, {resample[0]}, {resample[1]}, {resample[2]})"

def get_time_array(resample):
    if isinstance(resample, dict):
        resample = [resample['start'], resample['stop'], resample['increment']]
    return np.arange(resample[0], resample[1] + resample[2]*0.1, resample[2])


def dim_of(pn, ii):
    return f"dim_of({pn}, {ii})"


def units_of(pn):
    return f"units_of({pn})"


def get_args():
    parser = argparse.ArgumentParser(description='Read MDSplus channel')
    parser.add_argument('-n', '--shot_numbers', type=int, nargs='+', help='Shot number(s)')
    parser.add_argument('-t', '--trees', nargs='+', help='Tree name(s)')
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
                        help='Configuration file containing shot_numbers, trees, '
                             'point_names, and server. If provided, these arguments '
                             'are ignored.')
    parser.add_argument('-d', '--red_config', default=None, type=str,
                        help='Yaml file containing shot_numbers, trees, '
                             'point_names to not read.')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = get_args()
    data_dict = read_mdsplus_channel(shot_numbers=args.shot_numbers,
                                     trees=args.trees,
                                     point_names=args.point_names,
                                     server=args.server,
                                     resample=args.resample,
                                     verbose=args.verbose, config=args.config,
                                     red_config=args.red_config)
    if args.out_filename is not None:
        with h5py.File(args.out_filename, 'w') as f:
            for sn in data_dict:
                f.create_group(str(sn))
                for tree in data_dict[sn]:
                    f[str(sn)].create_group(tree)
                    for pn in data_dict[sn][tree]:
                        f[str(sn)][tree].create_group(pn)
                        for key in data_dict[sn][tree][pn]:
                            if isinstance(data_dict[sn][tree][pn][key], np.str_):
                                f[str(sn)][tree][pn].attrs[key] = \
                                            data_dict[sn][tree][pn][key].__repr__()
                            else:
                                f[str(sn)][tree][pn].create_dataset(key,
                                        data=data_dict[sn][tree][pn][key])