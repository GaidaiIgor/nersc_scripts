#!/usr/bin/env python

import argparse
import subprocess
import os


def parse_command_line_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generates and chains sbatch input files for ozone calculations")
    parser.add_argument("-l", "--level", type=int, choices={0, 1}, default=1,
                        help="Calculation depth: 0 - even, 1 - both sym")
    parser.add_argument("-so", "--stage-options", help="Custom submission parameters for each stage")

    args = parser.parse_args()
    return args


def main():
    args = parse_command_line_args()
    # init folder structure
    subprocess.check_call("init_spectrum_folders.py -l{0}".format(args.level), shell=True)
    # initiate call sequence
    chain_call_command = "chain_call_next_stage.py -so \"{0}\"".format(args.stage_options)
    os.chdir("even/basis")
    subprocess.check_call(chain_call_command, shell=True)
    os.chdir("../../odd/basis")
    subprocess.check_call(chain_call_command, shell=True)


main()
