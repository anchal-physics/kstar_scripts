import os
import tempfile
import argparse
import yaml


def execute_file_remotely(host, exec_file_path, additional_files=[], return_files=[],
                          verbose=False):
    if verbose:
        print(f"Executing {exec_file_path} remotely on {host}")

    # tmp_dir = tempfile.mkdtemp()
    os.system(f"ssh {host} 'mkdir -p temp_dir'")
    os.system(f"scp {exec_file_path} "
              +f"{host}:temp_dir/{os.path.basename(exec_file_path)}")
    for file in additional_files:
        os.system(f"scp {file} {host}:temp_dir/{os.path.basename(file)}")
    os.system(f"ssh {host} 'cd temp_dir && chmod +x {os.path.basename(exec_file_path)}"
              + f" && ./{os.path.basename(exec_file_path)}'")
    for file in return_files:
        os.system(f"scp {host}:temp_dir/{os.path.basename(file)} {file}")


def get_mdsplus_channel(shot_numbers, tree_names, point_names, out_filename,
                        server='203.230.126.231:8005', host='iris',resample=None,
                        verbose=False):
    if isinstance(shot_numbers, int):
        shot_numbers = [shot_numbers]
    if isinstance(tree_names, str):
        tree_names = [tree_names]
    if isinstance(point_names, str):
        point_names = [point_names]
    sn = ' '.join([str(shot_number) for shot_number in shot_numbers])
    tn = ' '.join(tree_names)
    nn = ' '.join(point_names)
    if resample is None:
        rs = ''
    else:
        if isinstance(resample, dict):
            resample = [resample['start'], resample['stop'], resample['increment']]
        rs = f'-r " {resample[0]}" " {resample[1]}" " {resample[2]}"'
    if verbose:
        print(f"Reading MDSplus channel {nn} from tree {tn} at shot number {sn} "
              + f"from server {server} and resampling as {resample}...")
        vs = '-v'
    else:
        vs = ''

    # Create a temporary file to execute remotely
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        f.write(f"#!/bin/bash \n"
                + f"module load python/3 \n"
                + f"python3 read_mdsplus_channel.py "
                + f"-n {sn} -t {tn} -p {nn} -s {server} {rs} -o {out_filename} {vs}")

    # Execute the temporary file remotely
    add_files = [os.path.join(os.path.dirname(__file__), 'read_mdsplus_channel.py')]
    execute_file_remotely(host, f.name, additional_files=add_files,
                          return_files=[out_filename], verbose=verbose)

    # Delete the temporary file
    os.remove(f.name)


def get_args():
    parser = argparse.ArgumentParser(description='Read MDSplus channel through SSH')
    parser.add_argument('-n', '--shot_numbers', type=int, nargs='+',
                        help='Shot number(s)')
    parser.add_argument('-t', '--tree_names', nargs='+', help='Tree name(s)')
    parser.add_argument('-p', '--point_names', nargs='+', help='Point name(s)')
    parser.add_argument('-s', '--server', default='203.230.126.231:8005',
                        help='Server address. Default if KSTAR open server '
                             '203.230.126.231:8005')
    parser.add_argument('-i', '--host', default='iris.get.com',
                        help='IP address of remote server. Default is iris')
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
                             'If provided, these arguments are ignored.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Print verbose messages')
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
    print(args.point_names)
    get_mdsplus_channel(args.shot_numbers, args.tree_names,
                        args.point_names, out_filename=args.out_filename,
                        server=args.server,
                        host=args.host, resample=args.resample, verbose=args.verbose)
    