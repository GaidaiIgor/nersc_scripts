#!/usr/bin/env python
import argparse
import os
import subprocess


def parse_command_line_args():
    parser = argparse.ArgumentParser(description="Applies a given script to all specified SpectrumSDT folders")
    parser.add_argument("--K", required=True, help="Submits specified value of K")
    parser.add_argument("--sym", default="[0, 1]", help="Submits specified values of symmetry")
    parser.add_argument("--stage", required=True, choices=["eigensolve", "properties"], help="Submits specified stages")
    parser.add_argument("--command", required=True, help="Command to be executed in each target folder")

    args = parser.parse_args()
    return args


def eval_list(arg):
    new_arg = eval(arg)
    if isinstance(new_arg, range):
        new_arg = list(new_arg)
    elif not hasattr(new_arg, "__len__"):
        new_arg = [new_arg]
    return new_arg


def eval_args(args):
    # Transforms string descriptions to final objects
    if args.K is not None:
        args.K = eval_list(args.K)
    if args.sym is not None:
        args.sym = eval_list(args.sym)


def main():
    args = parse_command_line_args()
    eval_args(args)
    for k in args.K:
        os.chdir("K_{}".format(k))
        for sym in args.sym:
            os.chdir("symmetry_{}/{}".format(sym, args.stage))
            subprocess.call(args.command, shell=True)
            os.chdir("../..")
        os.chdir("..")


if __name__ == "__main__":
    main()
