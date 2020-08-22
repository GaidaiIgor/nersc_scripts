#!/usr/bin/env python

import argparse
import os
import os.path as path
import subprocess
from typing import List


def parse_command_line_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launches a specified mode for all Js")
    parser.add_argument("stage", choices=["basis", "overlaps", "diagonalization", "properties"], help="Type of calculation")
    parser.add_argument("-js", help="Range of Js to process")
    parser.add_argument("-npb", "--nodes-per-block", type=int, help="Number of nodes per K-block")
    parser.add_argument("-o", "--options", default="", help="Custom submission parameters for a stage")
    args = parser.parse_args()
    return args


def main():
    args = parse_command_line_args()
    js_tokens = list(map(lambda x: int(x), args.js.split(":")))
    js = range(js_tokens[0], js_tokens[1] + 1)
    base_path = os.getcwd()

    for j in js:
        next_dir = path.join(base_path, "J_" + str(j))
        os.chdir(next_dir)
        for p in [0, 1]:
            if j == 0 and p == 1:
                continue
            num_blocks = j + 1 - (j + p) % 2
            num_nodes = num_blocks * args.nodes_per_block
            options = args.options + f" -n {num_nodes}"
            call_command = f"submit_all_ks.py {args.stage} --allowed-only -p {p} -o '{options}'"
            subprocess.check_call(call_command, shell=True)


main()
