#!/usr/bin/env python

import os
import subprocess


def main():
    cwd = os.getcwd()
    hpss_path = cwd.split("spectrumsdt/", 1)[1]  # take path after spectrumsdt/
    subprocess.call("htar -cPf {0}/data.tar .".format(hpss_path), shell=True)


main()
