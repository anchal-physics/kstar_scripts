import h5py
import argparse

def copy_group(src_h5, src_group, des_h5):
    group_path = src_group.parent.name
    group_id = des_h5.require_group(group_path)
    src_h5.copy(src_group, group_id)


def merge_h5(src_file, des_file):
    with h5py.File(des_file, 'a') as des_h5:
            with h5py.File(src_file, 'r') as src_h5:
                for sn in src_h5:
                    if sn in des_h5:
                        for tree in src_h5[sn]:
                            if tree in des_h5[sn]:
                                for pn in src_h5[sn][tree]:
                                    if pn not in des_h5[sn][tree]:
                                        copy_group(src_h5, src_h5[sn][tree][pn], des_h5)
                                    else:
                                        try:
                                            des_len = len(des_h5[sn][tree][pn]['data'])
                                            src_len = len(src_h5[sn][tree][pn]['data'])
                                            if des_len < src_len:
                                                del des_h5[sn][tree][pn]
                                                copy_group(src_h5, src_h5[sn][tree][pn],
                                                           des_h5)
                                            else:
                                                print("Skipping", sn, tree, pn,
                                                      "because it already exists in ",
                                                      "the destination file with equal",
                                                      "or greater length.")
                                        except BaseException as exc:
                                            print("Ignoring", sn, tree, pn,
                                                  "because of error:", exc)
                            else:
                                copy_group(src_h5, src_h5[sn][tree], des_h5)
                    else:
                        copy_group(src_h5, src_h5[sn], des_h5)


def get_args():
    parser = argparse.ArgumentParser(description='Merge HDF5 files')
    parser.add_argument('src_file', type=str, help='File to merge from')
    parser.add_argument('des_file', type=str, help='File to merge to')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = get_args()
    merge_h5(args.src_file, args.des_file)