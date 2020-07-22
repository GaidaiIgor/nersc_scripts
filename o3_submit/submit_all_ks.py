#!/usr/bin/env python

import argparse
import copy
import itertools
import subprocess
import os
import os.path as path
from typing import List

from SpectrumConfig import SpectrumConfig


def parse_command_line_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launches a specified mode for all Ks")
    parser.add_argument("-c", "--config", default="spectrumsdt.config", help="Path to configuration file")
    parser.add_argument("stage", help="Stage name: basis, overlaps, diagonalization or properties")
    parser.add_argument("-o", "--options", default="", help="Custom submission parameters for a stage")
    parser.add_argument("-s", "--sym", choices=["even", "odd", "all"], default="all", help="Symmetry")
    parser.add_argument("-nc", "--no-cor", action="store_true", help="Disable coriolis coupling for diagonalization")
    parser.add_argument("-ao", "--allowed-only", action="store_true", help="Send only allowed combinations of parity and symmetry")
    parser.add_argument("-p", "--parity", type=int, choices=[0, 1], help="Send only specified parity")
    args = parser.parse_args()
    return args


def generate_paths(base_path: str, folder_names: List[List[str]]) -> List[str]:
    """ Generates all combinations of folders specified in folder_names. Returns a list of generated paths """
    name_combos = itertools.product(*folder_names)
    paths = list(map(lambda name_combo: path.join(base_path, *name_combo), name_combos))
    return paths


def main():
    args = parse_command_line_args()

    if args.sym == "all":
        symmetries = ["even", "odd"]
    elif args.sym == "even":
        symmetries = ["even"]
    else:
        symmetries = ["odd"]

    folder_names = [["K_{0}"], symmetries, [args.stage]]
    folder_names_coriolis = [["K_all"], ["parity_{0}"], symmetries, [args.stage]]

    config_path = path.abspath(args.config)
    config = SpectrumConfig(config_path)
    J = config.get_j()

    base_path = os.getcwd()
    target_folders = []
    if (args.stage == "diagonalization" or args.stage == "properties") and not args.no_cor:
        if args.parity is None:
            parity_range = range(min(J + 1, 2))
        else:
            parity_range = [args.parity]

        for p in parity_range:
            current_folder_names = copy.deepcopy(folder_names_coriolis)
            current_folder_names[1][0] = current_folder_names[1][0].format(p)
            current_target_folders = generate_paths(base_path, current_folder_names)
            if args.allowed_only:
                current_target_folders = [current_target_folders[p]]
            target_folders = target_folders + current_target_folders
    else:
        for i in range(J + 1):
            current_folder_names = copy.deepcopy(folder_names)
            current_folder_names[0][0] = current_folder_names[0][0].format(i)
            current_target_folders = generate_paths(base_path, current_folder_names)
            target_folders = target_folders + current_target_folders

    call_command = "o3_submit.py " + args.options
    for i in range(len(target_folders)):
        os.chdir(target_folders[i])
        subprocess.check_call(call_command, shell=True)


main()
