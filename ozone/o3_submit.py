#!/usr/bin/env python

from __future__ import annotations

import argparse
import math
import os
import os.path as path
import itertools
import subprocess
from typing import List

import sys
sys.path.append("/global/u2/g/gaidai/SpectrumSDT_gfortran/scripts")
from SpectrumSDTConfig import SpectrumSDTConfig


class SubmissionScript:
    def __init__(self, filesystem: str, qos: str, nodes: str, time: str, job_name: str, out_name: str, node_type: str,
            n_procs: str, cores_per_proc: str, program_location: str, program_out_file_name: str, time_file_name: str, sbcast: bool):
        self.program_name = "spectrumsdt"
        self.filesystem = filesystem
        self.qos = qos
        self.nodes = nodes
        self.time = time
        self.job_name = job_name
        self.out_name = out_name
        self.node_type = node_type
        self.n_procs = n_procs
        self.cores_per_proc = cores_per_proc
        self.program_location = program_location
        self.program_out_file_name = program_out_file_name
        self.time_file_name = time_file_name
        self.sbcast = sbcast
        self.script_name = path.splitext(self.out_name)[0] + ".sbatch"

    @classmethod
    def assemble_script(cls, args: argparse.Namespace) -> SubmissionScript:
        """ collects parameters and assembles input script """
        time = "{:.0f}".format(args.time * 60)
        nodes = str(args.nodes)
        n_procs = str(args.nprocs)

        cores_per_proc = str(int(ParameterMaster.get_cores_per_node() * args.nodes / args.nprocs) * ParameterMaster.get_threads_per_core())
        return cls(args.filesystem, args.qos, nodes, time, args.jobname, args.outname, ParameterMaster.nodes_type, 
                n_procs, cores_per_proc, args.program_location, args.program_out_file_name, args.time_file_name, args.sbcast)

    def write(self):
        program_path = path.join(self.program_location, self.program_name)
        tmp_program_path = path.join("/tmp", self.program_name)
        call_location = tmp_program_path if self.sbcast else program_path

        filesystem_line = "#SBATCH -L SCRATCH\n" if self.filesystem == "scratch" else ""
        qos_line = "#SBATCH -q " + self.qos + "\n" if self.qos is not None else ""
        min_time_line = "#SBATCH --time-min " + str(min(int(self.time), 240)) + "\n" if self.qos == "overrun" else ""
        nodes_line = "#SBATCH -N " + self.nodes + "\n" if self.nodes is not None else ""
        cores_line = "#SBATCH -n " + self.n_procs + "\n" if self.qos == "shared" else ""
        time_line = "#SBATCH -t " + self.time + "\n" if self.time is not None else ""
        job_line = "#SBATCH -J " + self.job_name + "\n" if self.job_name is not None else ""
        out_line = "#SBATCH -o " + self.out_name + "\n" if self.out_name is not None else ""
        node_type_line = "#SBATCH -C " + self.node_type + "\n"
        export_pmi_line = "export PMI_MMAP_SYNC_WAIT_TIME=300\n" if self.sbcast else ""
        sbcast_line = "sbcast --compress=lz4 " + program_path + " " + tmp_program_path + "\n" if self.sbcast else ""
        script = ("#!/bin/bash\n"
                  + filesystem_line
                  + qos_line
                  + min_time_line
                  + nodes_line
                  + cores_line
                  + time_line
                  + job_line
                  + out_line
                  + node_type_line
                  + "\n"
                  + "date\n"
                  + "echo $SLURM_JOB_ID\n"
                  + "rm -f " + self.time_file_name + "\n"
                  + export_pmi_line
                  + sbcast_line
                  + "srun -n " + self.n_procs + " -c " + self.cores_per_proc + " --cpu_bind=cores time -ao " + self.time_file_name + " " 
                  + call_location + " > " + self.program_out_file_name + "\n")
        with open(self.script_name, "w") as output:
            output.write(script)

    def submit(self):
        subprocess.call("sbatch " + self.script_name, shell=True)


class ParameterMaster:
    """ Resolves implicit parameters """
    hyperthreading = True
    host_name = os.environ["HOST"][:-2]
    job_name_separator = "gaidai/"
    grid_file_names = ["grid_rho.dat", "grid_theta.dat", "grid_phi.dat"]
    pes_file_name = "pes.out"
    basis_results_folder = "out_basis"
    basis_2d_file = "nvec2.dat"
    overlap_results_folder = "out_overlaps"
    eigencalc_results_folder = "out_eigencalc"
    spectrum_filename = "states.fwc"

    @staticmethod
    def get_grid_path(config: SpectrumSDTConfig, grid_num: int) -> str:
        grid_folder = config.get_grid_path()
        grid_file_name = ParameterMaster.grid_file_names[grid_num - 1]
        return path.join(grid_folder, grid_file_name)

    @staticmethod
    def get_grid_points_num(config: SpectrumSDTConfig, grid_num: int) -> int:
        grid_path = ParameterMaster.get_grid_path(config, grid_num)
        with open(grid_path, "r") as grid_file:
            return int(grid_file.readline().split()[3])  # fourth number on first line

    @staticmethod
    def get_pes_path(config: SpectrumSDTConfig) -> str:
        grid_folder = config.get_grid_path()
        return path.join(grid_folder, ParameterMaster.pes_file_name)

    @staticmethod
    def get_sym_string(sym: int) -> str:
        return "even" if sym == 0 else "odd"

    @staticmethod
    def get_sym_folder(root_path: str, K: int, sym: int, parity: int = None) -> str:
        sym_str = ParameterMaster.get_sym_string(sym)
        if K == -1:
            K_folder = "K_all"
            sym_parent = path.join("K_all", "parity_{0}".format(parity))
        else:
            sym_parent = "K_{0}".format(K)
        return path.join(root_path, sym_parent, sym_str)

    @staticmethod
    def get_basis_folder(root_path: str, K: int, sym: int) -> str:
        return path.join(ParameterMaster.get_sym_folder(root_path, K, sym), "basis")

    @staticmethod
    def get_basis_results_folder(root_path: str, K: int, sym: int) -> str:
        return path.join(ParameterMaster.get_basis_folder(root_path, K, sym), ParameterMaster.basis_results_folder)

    @staticmethod
    def get_basis_2d_path(root_path: str, K: int, sym: int) -> str:
        return path.join(ParameterMaster.get_basis_results_folder(root_path, K, sym), ParameterMaster.basis_2d_file)

    @staticmethod
    def get_overlaps_folder(root_path: str, K: int, sym: int) -> str:
        return path.join(ParameterMaster.get_sym_folder(root_path, K, sym), "overlaps")

    @staticmethod
    def get_overlaps_results_folder(root_path: str, K: int, sym: int) -> str:
        return path.join(ParameterMaster.get_overlaps_folder(root_path, K, sym), ParameterMaster.overlap_results_folder)

    @staticmethod
    def get_eigencalc_folder(root_path: str, K: int, sym: int, parity: int = None) -> str:
        return path.join(ParameterMaster.get_sym_folder(root_path, K, sym, parity), "eigencalc")

    @staticmethod
    def get_eigencalc_results_folder(root_path: str, K: int, sym: int, parity: int = None) -> str:
        return path.join(ParameterMaster.get_eigencalc_folder(root_path, K, sym, parity), ParameterMaster.eigencalc_results_folder)

    @staticmethod
    def get_spectrum_path(root_path: str, K: int, sym: int, parity: int = None) -> str:
        return path.join(ParameterMaster.get_eigencalc_results_folder(root_path, K, sym, parity), ParameterMaster.spectrum_filename)

    @staticmethod
    def set_spectrumsdt_params(config_path: str, args: argparse.Namespace):
        """ Sets required number of cores/nodes based on logic of spectrumsdt program.
        :param config_path: Path to spectrumsdt config file
        :param args: parsed input arguments """

        config = SpectrumSDTConfig(config_path)
        stage = config.get_stage()
        if stage == "grids":
            ParameterMaster.set_pesprint_params(config, args)
        elif stage == "basis":
            ParameterMaster.set_basis_params(config, args)
        elif stage == "overlaps":
            ParameterMaster.set_overlap_params(config, args)
        elif stage == "eigencalc":
            ParameterMaster.set_eigencalc_params(config, args)
        elif stage == "properties":
            ParameterMaster.set_properties_params(config, args)

    @staticmethod
    def set_basis_params(config: SpectrumSDTConfig, args: argparse.Namespace):
        pes_path = ParameterMaster.get_pes_path(config)
        # make sure pesprint worked out ok
        assert path.isfile(pes_path) and path.getsize(pes_path) > 0, "pesprint is not completed"

        # set up parameters
        args.nprocs = ParameterMaster.get_grid_points_num(config, 1)
        args.nodes = ParameterMaster.compute_nodes(args.nprocs)

    @staticmethod
    def set_overlap_params(config: SpectrumSDTConfig, args: argparse.Namespace):
        args.nodes = 1
        args.nprocs = ParameterMaster.compute_cores(args.nodes)

    @staticmethod
    def set_eigencalc_params(config: SpectrumSDTConfig, args: argparse.Namespace):
        args.nodes = 1
        args.nprocs = ParameterMaster.compute_cores(args.nodes)

    @staticmethod
    def set_properties_params(config: SpectrumSDTConfig, args: argparse.Namespace):
        # set up parameters
        args.nprocs = config.get_number_of_states()
        args.nodes = ParameterMaster.compute_nodes(args.nprocs)

    @staticmethod
    def set_wfs_print_params(config: SpectrumSDTConfig, args: argparse.Namespace):
        diag_output_path = ParameterMaster.get_eigencalc_output_path(config)
        # make sure eigencalc worked out ok
        assert path.isfile(diag_output_path), "eigencalc is not completed"

        # set up parameters
        args.nprocs = config.get_number_of_wfs_to_print()
        args.nodes = ParameterMaster.compute_nodes(args.nprocs)

    @staticmethod
    def read_2d_basis_dist(basis_2d_dist_path: str) -> List[int]:
        """ Returns only non-zero size blocks """
        with open(basis_2d_dist_path, "r") as basis_2d_dist_file:
            file_lines = basis_2d_dist_file.readlines()
        basis_2d_sizes = list(filter(lambda x: x > 0, map(lambda x: int(x.split()[1]), file_lines)))
        return basis_2d_sizes

    @staticmethod
    def read_1d_basis_dist(basis_1d_dist_path: str) -> List[List[int]]:
        with open(basis_1d_dist_path, "r") as basis_1d_dist_file:
            file_lines = basis_1d_dist_file.readlines()[1:]  # ignore header line
        basis_1d_sizes = list(map(lambda line: list(map(int, line.split()[1:])), file_lines))
        return basis_1d_sizes

    @staticmethod
    def generate_out_name() -> str:
        return "out.slurm"

    @staticmethod
    def set_pesprint_params(config_path: str, args: argparse.Namespace):
        args.nodes = 1
        args.nprocs = ParameterMaster.compute_cores(args.nodes)

    @staticmethod
    def get_cores_per_node() -> int:
        return ParameterMaster.cores_per_node

    @staticmethod
    def get_max_shared_cores() -> int:
        factor = 2 if ParameterMaster.hyperthreading else 1
        return ParameterMaster.max_shared_cores * factor

    @staticmethod
    def get_max_debug_nodes() -> int:
        return ParameterMaster.max_debug_nodes

    @staticmethod
    def get_threads_per_core() -> int:
        return ParameterMaster.threads_per_core

    @staticmethod
    def is_hyperthreading() -> bool:
        return ParameterMaster.hyperthreading

    @staticmethod
    def compute_nodes(cores: int, hyperthreading: bool = None) -> int:
        """ returns required number of nodes for specified number of cores """
        if hyperthreading is None:
            hyperthreading = ParameterMaster.is_hyperthreading()
        factor = ParameterMaster.get_threads_per_core() if hyperthreading else 1
        return int(math.ceil(cores / (ParameterMaster.get_cores_per_node() * factor)))

    @staticmethod
    def compute_cores(nodes: int, hyperthreading: bool = None) -> int:
        """ returns number of cores in the specified number of nodes """
        if hyperthreading is None:
            hyperthreading = ParameterMaster.is_hyperthreading()
        factor = ParameterMaster.get_threads_per_core() if hyperthreading else 1
        return nodes * ParameterMaster.get_cores_per_node() * factor

    @staticmethod
    def generate_job_name(config_path: str) -> str:
        config_path_parts = config_path.split(ParameterMaster.job_name_separator, 1)
        job_name = path.dirname(config_path_parts[1])
        return job_name


def parse_command_line_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generates sbatch input file for ozone calculations")
    parser.add_argument("-c", "--config", help="Path to configuration file")
    parser.add_argument("-q", "--qos", help="Job QoS")
    parser.add_argument("-t", "--time", type=float, default=0.5, help="Requested job time")
    parser.add_argument("-n", "--nodes", type=int, help="Desired number of nodes")
    parser.add_argument("-np", "--nprocs", type=int, help="Desired number of processes")
    parser.add_argument("-jn", "--jobname", help="Job name")
    parser.add_argument("-on", "--outname", help="Slurm output file name")
    parser.add_argument("-pon", "--program-out-file-name", default="prg.out", help="Name of a separate file with program output only")
    parser.add_argument("-tfn", "--time-file-name", default="time.out", help="Name of a separate file with time output only")
    parser.add_argument("-go", "--gen-only", action="store_true", help="Generate sbatch without submission")
    parser.add_argument("-ht", "--hyperthreading", dest="hyperthreading", action="store_true",
                        help="Specify to enable hyperthreading")
    parser.add_argument("-nm", "--node-multiplier", dest="nodes_mult", type=float, help="Multiplier for implicitly computed number of nodes")
    parser.add_argument("-pm", "--procs-multiplier", dest="procs_mult", type=float, help="Multiplier for implicitly computed number of processors")
    parser.add_argument("-v", "--verbose", action="store_true", help="Makes the script print additional information")
    parser.add_argument("-pl", "--program-location", help="Explicit path to program folder")
    parser.add_argument("-hn", "--host-name", help="Explicit host name, used to determine node configuration")
    parser.add_argument("-sbc", "--sbcast", action="store_true", help="Specify to use sbcast (helps to speed up jobs with large number (1000+) of MPI tasks)")
    #  parser.add_argument("-bn", "--build-name", default="cori", help="Specifies name of the folder with build")
    parser.add_argument("-fs", "--filesystem", default="none", help="Controls filesystem requirements")
    parser.add_argument("-nt", "--node-type", default="haswell", choices=["haswell", "amd"], help="Node type")

    args = parser.parse_args()
    return args


def assume_haswell_configuration():
    ParameterMaster.cores_per_node = 32
    ParameterMaster.max_shared_cores = 16
    ParameterMaster.max_debug_nodes = 64
    ParameterMaster.threads_per_core = 2
    ParameterMaster.nodes_type = "haswell"


def assume_amd_configuration():
    ParameterMaster.cores_per_node = 32
    ParameterMaster.max_shared_cores = 16
    ParameterMaster.max_debug_nodes = 64
    ParameterMaster.threads_per_core = 2
    ParameterMaster.nodes_type = "amd"


def configure_parameter_master(args: argparse.Namespace):
    ParameterMaster.hyperthreading = args.hyperthreading
    #  ParameterMaster.nodes_mult = args.nodes_mult
    if args.host_name is not None:
        ParameterMaster.host_name = args.host_name

    if args.node_type == "haswell":
        assume_haswell_configuration()
    elif args.node_type == "amd":
        assume_amd_configuration()
    else:
        raise Exception("invalid node type")


def resolve_defaults(args: argparse.Namespace):
    # Some defaults depend on other values so they are set separately
    if args.config is None:
        args.config = "spectrumsdt.config"
    args.config = path.abspath(args.config)

    if args.nodes is None and args.nprocs is not None:
        args.nodes = ParameterMaster.compute_nodes(args.nprocs)
    if args.nprocs is None and args.nodes is not None:
        args.nprocs = ParameterMaster.compute_cores(args.nodes)
    if args.nprocs is None and args.nodes is None:
        ParameterMaster.set_spectrumsdt_params(args.config, args)

    if args.jobname is None:
        args.jobname = ParameterMaster.generate_job_name(args.config)
    if args.outname is None:
        args.outname = ParameterMaster.generate_out_name()

    if args.program_location is None:
        args.program_location = path.expandvars("$mybin")
        if len(args.program_location) == 0:
            raise "Program location cannot be determined. Specify full path via -pl"

    if args.procs_mult is None and args.nodes_mult is None:
        args.procs_mult = 1
        args.nodes_mult = 1
    if args.procs_mult is None and args.nodes_mult is not None:
        args.procs_mult = 1 if args.nodes_mult > 1 else args.nodes_mult
    if args.nodes_mult is None and args.procs_mult is not None:
        args.nodes_mult = args.procs_mult if args.procs_mult > 1 else 1


def postprocess_args(args: argparse.Namespace):
    args.nprocs = math.ceil(args.nprocs * args.procs_mult)
    args.nodes = math.ceil(args.nodes * args.nodes_mult)

    if args.qos is None:
        args.qos = "debug"
        if args.time > 0.5 or args.nodes > ParameterMaster.get_max_debug_nodes():
            args.qos = "regular"


def main():
    args = parse_command_line_args()
    configure_parameter_master(args)
    resolve_defaults(args)
    postprocess_args(args)
    script = SubmissionScript.assemble_script(args)
    script.write()
    if not args.gen_only:
        script.submit()
    if args.verbose:
        print("Program folder is " + args.program_location)
        print("Host name is " + ParameterMaster.host_name)
        print("Script name is " + script.script_name)


main()
