#!/usr/bin/env python
import argparse
import os
import subprocess


def parse_command_line_args():
    parser = argparse.ArgumentParser(description="Applies a given script to all specified SpectrumSDT folders")
    parser.add_argument("--J", default="[None]", help="Submits specified values of J")
    parser.add_argument("--K", default="[None]", help="Submits specified values of K")
    parser.add_argument("--sym", default="[0, 1]", help="Submits specified values of symmetry")
    parser.add_argument("--stage", default="properties", choices=["eigensolve", "properties"], help="Submits specified stages")
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
    if args.J is not None:
        args.J = eval_list(args.J)
    if args.K is not None:
        args.K = eval_list(args.K)
    if args.sym is not None:
        args.sym = eval_list(args.sym)


def main():
    args = parse_command_line_args()
    eval_args(args)
    for j in args.J:
        if j is not None:
            os.chdir(f"J_{j}")

        for k in args.K:
            if k is not None:
                if k > j:
                    continue
                os.chdir(f"K_{k}")

            for sym in args.sym:
                os.chdir(f"symmetry_{sym}/{args.stage}")
                subprocess.call(args.command, shell=True)
                os.chdir("../..")

            if k is not None:
                os.chdir("..")

        if j is not None:
            os.chdir("..")


if __name__ == "__main__":
    main()
