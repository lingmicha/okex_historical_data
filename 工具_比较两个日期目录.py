import os
import filecmp
from pathlib import Path
import glob


override = [
    'DASH-USDT-SWAP',
    'XMR-USDT-SWAP',
    'ZEN-USDT-SWAP',
    'ZEC-USDT-SWAP',
]


def compare_dirs(dir1, dir2):
    # Get sets of file names from both directories
    files1 = set(os.listdir(dir1))
    files2 = set(os.listdir(dir2))

    # Remove the Thumbs.db if it exists
    files1 = {x for x in files1 if not any(o in x for o in override)}
    files2 = {x for x in files2 if not any(o in x for o in override)}

    # Find files that are in both directories
    common_files = files1.intersection(files2)

    # Compare common files
    diffs = [f for f in common_files if not filecmp.cmp(os.path.join(dir1, f), os.path.join(dir2, f), shallow=False)]

    # Find files that are only in one directory or the other
    only_in_dir1 = files1 - files2
    only_in_dir2 = files2 - files1

    return diffs, only_in_dir1, only_in_dir2


# Replace with your actual directories
base_dir = Path(__file__).parent / 'data' / 'aggtrades'
target_dir = Path(__file__).parent / 'data' / 'aggtrades_20231225'

dir1_folders = [file for file in base_dir.iterdir() if file.is_dir()]
dir1_folders = sorted(dir1_folders)

for dir1_folder in dir1_folders:
    dir2_folder = target_dir / dir1_folder.name
    if not dir2_folder.exists():
        print(f'{dir2_folder} 不存在')
        continue
    diffs, only_in_dir1, only_in_dir2 = compare_dirs(dir1_folder, dir2_folder)
    if len(diffs) or len(only_in_dir1) or len(only_in_dir2):
        print(f'{dir1_folder} 与 {dir2_folder} 有差异')
        if len(diffs):
            print('Files with differences:', diffs)
        if len(only_in_dir1):
            print('Files only in', dir1_folder, ':', only_in_dir1)
        if len(only_in_dir2):
            print('Files only in', dir2_folder, ':', only_in_dir2)
    else:
        print(f'{dir1_folder} 与 {dir2_folder} 无差异')

# dir2_folders = [file for file in based_dir2.iterdir() if file.is_dir()]


# diffs, only_in_dir1, only_in_dir2 = compare_dirs(dir1, dir2)

# print('Files with differences:', diffs)
# print('Files only in', dir1, ':', only_in_dir1)
# print('Files only in', dir2, ':', only_in_dir2)