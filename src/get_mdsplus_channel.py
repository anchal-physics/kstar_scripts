import os
import tempfile
import argparse
import yaml
from merge_h5 import merge_h5
import h5py


def execute_file_remotely(host, exec_file_path, additional_files=[], return_files=[],
                          verbose=False):
    if verbose:
        print(f"Executing {exec_file_path} remotely on {host}")

    # tmp_dir = tempfile.mkdtemp()
    os.system(f"ssh {host} 'mkdir -p temp_dir'")
    to_copy = ' '.join([exec_file_path] + additional_files)
    os.system(f"scp {to_copy} {host}:temp_dir/")
    # for file in additional_files:
    #     os.system(f"scp {file} {host}:temp_dir/{os.path.basename(file)}")
    os.system(f"ssh {host} 'cd temp_dir && chmod +x {os.path.basename(exec_file_path)}"
              + f" && ./{os.path.basename(exec_file_path)}'")
    for file in return_files:
        os.system(f"scp {host}:temp_dir/{os.path.basename(file)} {file}")
    os.system(f"ssh {host} 'rm -rf temp_dir'")


def check_exists(h5, shot_number, tree, point_name):
    if shot_number in h5:
        if tree in h5[shot_number]:
            pns = '\\' + point_name
            if pns in h5[shot_number][tree]:
                if 'data' in h5[shot_number][tree][pns]:
                    return True


def get_mdsplus_channel(shot_numbers=31779, trees='KSTAR',
                        point_names='EP53:FOO', out_filename='MDSplus_data.h5',
                        server='203.230.126.231:8005', host='iris', resample=None,
                        verbose=False, config=None, update_cache=False):
    add_files = [os.path.join(os.path.dirname(__file__), 'read_mdsplus_channel.py')]

    if config is None:
        if isinstance(shot_numbers, int):
            shot_numbers = [shot_numbers]
        if isinstance(trees, str):
            trees = [trees]
        if isinstance(point_names, str):
            point_names = [point_names]
        sn = ' '.join([str(shot_number) for shot_number in shot_numbers])
        tn = ' '.join(trees)
        nn = ' '.join(point_names)
        if resample is None:
            rs = ''
        else:
            if isinstance(resample, dict):
                resample = [resample['start'], resample['stop'], resample['increment']]
            rs = f'-r " {resample[0]}" " {resample[1]}" " {resample[2]}"'
        cs = f"-n {sn} -t {tn} -p {nn} -s {server} {rs}"
        if verbose:
            print(f"Reading MDSplus channel {nn} from tree {tn} at shot number {sn} "
                + f"from server {server} and resampling as {resample}...")
            vs = '-v'
        else:
            vs = ''
    else:
        cs = f"-c {config}"
        with open(config, 'r') as f:
            config_dict = yaml.safe_load(f)
        if 'host' in config_dict:
            host = config_dict['host']
        if 'out_filename' in config_dict:
            out_filename = config_dict['out_filename']
        if 'verbose' in config_dict:
            verbose = config_dict['verbose']
            print(f"Reading with configuration:")
            print(config_dict)
            vs = '-v'
        else:
            vs = ''
        add_files.append(config)
        if update_cache:
            ds = ''
        else:
            if os.path.exists(out_filename):
                red_dict = {}
                with h5py.File(out_filename, 'r') as h5:
                    for sn in config_dict['shot_numbers']:
                        sns = str(sn)
                        red_dict[sns] = {}
                        for tree in config_dict['trees']:
                            red_dict[sns][tree] = []
                            for pn in config_dict['trees'][tree]:
                                if check_exists(h5, sns, tree, pn):
                                    red_dict[str(sn)][tree] += [pn]
                with open('red_config.yml', "w") as yaml_file:
                    dump = yaml.dump(red_dict, default_flow_style = False, allow_unicode = True, encoding = None)
                    yaml_file.write(dump)
            add_files.append('red_config.yml')
            if os.path.exists('red_config.yml'):
                ds = f"-d red_config.yml"
            else:
                ds = ''

    
    require_merge = False
    if os.path.exists(out_filename):
        if verbose:
            print(f"File {out_filename} already exists. Will merge with new data.")
        require_merge = True
        keep_filename = out_filename
        out_filename += '.new'

    # Create a temporary file to execute remotely
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp_f:
        tmp_f.write(f"#!/bin/bash \n"
                    # + f"module load python/3 \n"
                    + f"python read_mdsplus_channel.py {cs} "
                    + f"-o {out_filename} {vs} {ds}")


    # Execute the temporary file remotely
    execute_file_remotely(host, tmp_f.name, additional_files=add_files,
                          return_files=[out_filename], verbose=verbose)
    
    # Clean up point names whose data was not read
    with h5py.File(out_filename, 'r+') as h5:
        for shot in h5:
            for tree in h5[shot]:
                for pn in h5[shot][tree]:
                    if 'data' not in h5[shot][tree][pn]:
                        del h5[shot][tree][pn]
                        continue
    
    if require_merge:
        if verbose:
            print(f"Merging {out_filename} with {keep_filename}")
        merge_h5(out_filename, keep_filename)
        os.remove(out_filename)


    # Delete the temporary file
    os.remove(tmp_f.name)
    if os.path.exists('red_config.yml'):
        os.remove("red_config.yml")


def get_args():
    parser = argparse.ArgumentParser(description='Read MDSplus channel through SSH')
    parser.add_argument('-n', '--shot_numbers', type=int, nargs='+',
                        help='Shot number(s)')
    parser.add_argument('-t', '--trees', nargs='+', help='Tree name(s)')
    parser.add_argument('-p', '--point_names', nargs='+', help='Point name(s)')
    parser.add_argument('-s', '--server', default='203.230.126.231:8005',
                        help='Server address. Default if KSTAR open server '
                             '203.230.126.231:8005')
    parser.add_argument('-i', '--host', default='omega.gat.com',
                        help='IP address of remote server. Default is omega')
    parser.add_argument('-r', '--resample', nargs='+', type=float, default=None,
                        help='Resample signal(s) by providing a list of start, stop, '
                             'and increment values. For negative value, enclose them '
                             'withing double quotes and add a space at the beginning.'
                             'Example: --resample " -0.1" 10.0 0.1')
    parser.add_argument('-o', '--out_filename', default='MDSplus_data.h5',
                        help='Output filename for saving data in file. Default is '
                             'MDSplus_data.h5.')
    parser.add_argument('-c', '--config', default=None, type=str,
                        help='Configuration file containing all arguments. '
                             'If provided, all other arguments are ignored.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Print verbose messages')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = get_args()
    get_mdsplus_channel(args.shot_numbers, args.trees, args.point_names,
                        out_filename=args.out_filename, server=args.server,
                        host=args.host, resample=args.resample, verbose=args.verbose,
                        config=args.config)
    