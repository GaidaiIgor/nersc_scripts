#!/usr/bin/env python

from __future__ import annotations

import argparse
import math
import os
import os.path as path
import itertools
import subprocess
from typing import List

from SpectrumConfig import SpectrumConfig


class SubmissionScript:
    def __init__(self, filesystem: str, qos: str, nodes: str, time: str, job_name: str, out_name: str, node_type: str,
            n_procs: str, cores_per_proc: str, program_location: str, prg_name: str, prg_out_file_name: str):
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
        self.prg_name = prg_name
        self.prg_out_file_name = prg_out_file_name
        self.script_name = path.splitext(self.out_name)[0] + ".sbatch"

    @classmethod
    def assemble_script(cls, args: argparse.Namespace) -> SubmissionScript:
        """ collects parameters and assembles input script """
        time = "{:.0f}".format(args.time * 60)
        nodes = str(args.nodes)
        n_procs = str(args.nprocs)

        cores_per_proc = str(int(ParameterMaster.get_cores_per_node() * args.nodes / args.nprocs) * ParameterMaster.get_threads_per_core())
        return cls(args.filesystem, args.qos, nodes, time, args.jobname, args.outname, ParameterMaster.nodes_type, 
                n_procs, cores_per_proc, args.program_location, args.prg_name, args.prg_out_file_name)

    def write(self):
        filesystem_line = "#SBATCH -L SCRATCH\n" if self.filesystem == "scratch" else ""
        qos_line = "#SBATCH -q " + self.qos + "\n" if self.qos is not None else ""
        min_time_line = "#SBATCH --time-min " + str(min(int(self.time), 240)) + "\n" if self.qos == "overrun" else ""
        nodes_line = "#SBATCH -N " + self.nodes + "\n" if self.nodes is not None else ""
        cores_line = "#SBATCH -n " + self.n_procs + "\n" if self.qos == "shared" else ""
        time_line = "#SBATCH -t " + self.time + "\n" if self.time is not None else ""
        job_line = "#SBATCH -J " + self.job_name + "\n" if self.job_name is not None else ""
        out_line = "#SBATCH -o " + self.out_name + "\n" if self.out_name is not None else ""
        node_type_line = "#SBATCH -C " + self.node_type + "\n"
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
                  + "srun -n " + self.n_procs + " -c " + self.cores_per_proc + " --cpu_bind=cores time " + self.prg_name + " > " + self.prg_out_file_name)
        with open(self.script_name, "w") as output:
            output.write(script)

    def submit(self):
        subprocess.call("sbatch " + self.script_name, shell=True)


class ParameterMaster:
    """ Resolves implicit parameters """
    hyperthreading = True
    #  nodes_mult = 1

    host_name = os.environ["HOST"][:-2]
    job_name_separator = "ozone/"

    grid_file_names = ["grid1.dat", "grid2.dat", "grid3.dat"]
    pes_file_name = "potvib.dat"

    basis_results_folder = "basis"
    basis_1d_file = "nvec1.dat"
    basis_2d_file = "nvec2.dat"

    overlap_results_folder = "overlap0"

    diagonalization_results_folder = "3dsdt"
    spectrum_filename = "spec.out"

    @staticmethod
    def get_grid_path(config: SpectrumConfig, grid_num: int) -> str:
        grid_folder = config.get_grid_folder_path()
        grid_file_name = ParameterMaster.grid_file_names[grid_num - 1]
        return path.join(grid_folder, grid_file_name)

    @staticmethod
    def get_grid_points_num(config: SpectrumConfig, grid_num: int) -> int:
        grid_path = ParameterMaster.get_grid_path(config, grid_num)
        with open(grid_path, "r") as grid_file:
            return int(grid_file.readline().split()[0])  # first number on first line

    @staticmethod
    def get_pes_path(config: SpectrumConfig) -> str:
        grid_folder = config.get_grid_folder_path()
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
    def get_basis_1d_path(root_path: str, K: int, sym: int) -> str:
        return path.join(ParameterMaster.get_basis_results_folder(root_path, K, sym), ParameterMaster.basis_1d_file)

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
    def get_diagonalization_folder(root_path: str, K: int, sym: int, parity: int = None) -> str:
        return path.join(ParameterMaster.get_sym_folder(root_path, K, sym, parity), "diagonalization")

    @staticmethod
    def get_diagonalization_results_folder(root_path: str, K: int, sym: int, parity: int = None) -> str:
        return path.join(ParameterMaster.get_diagonalization_folder(root_path, K, sym, parity), ParameterMaster.diagonalization_results_folder)

    @staticmethod
    def get_spectrum_path(root_path: str, K: int, sym: int, parity: int = None) -> str:
        return path.join(ParameterMaster.get_diagonalization_results_folder(root_path, K, sym, parity), ParameterMaster.spectrum_filename)

    @staticmethod
    def set_spectrumsdt_params(config_path: str, args: argparse.Namespace):
        """ Sets required number of cores/nodes based on logic of spectrumsdt program.
        :param config_path: Path to spectrumsdt config file
        :param args: parsed input arguments """

        config = SpectrumConfig(config_path)
        launch_mode = config.get_launch_mode()
        if launch_mode == "basis":
            ParameterMaster.set_basis_params(config, args)
        elif launch_mode == "overlaps":
            ParameterMaster.set_overlap_params(config, args)
        elif launch_mode == "diagonalization":
            ParameterMaster.set_diagonalization_params(config, args)
        elif launch_mode == "properties":
            ParameterMaster.set_properties_params(config, args)

    @staticmethod
    def set_basis_params(config: SpectrumConfig, args: argparse.Namespace):
        pes_path = ParameterMaster.get_pes_path(config)
        # make sure pesprint worked out ok
        assert path.isfile(pes_path) and path.getsize(pes_path) > 0, "pesprint is not completed"

        # set up parameters
        args.nprocs = ParameterMaster.get_grid_points_num(config, 1)
        args.nodes = ParameterMaster.compute_nodes(args.nprocs)

    @staticmethod
    def set_overlap_params(config: SpectrumConfig, args: argparse.Namespace):
        root_path = config.get_root_path()
        K = config.get_ks()[0]
        sym = config.get_symmetry()
        basis_1d_path = ParameterMaster.get_basis_1d_path(root_path, K, sym)
        basis_2d_path = ParameterMaster.get_basis_2d_path(root_path, K, sym)

        # make sure basis worked out ok
        assert path.isfile(basis_2d_path), "basis is not completed, path: " + basis_2d_path

        # set up parameters
        basis_2d_dist = ParameterMaster.read_2d_basis_dist(basis_2d_path)
        # sizes of all off-diagonal blocks in hamiltonian (in elements)
        block_sizes = list(map(lambda x: x[0] * x[1], itertools.combinations(basis_2d_dist, 2)))
        # we assume all blocks have average size
        average_block_size = sum(block_sizes) / len(block_sizes)
        # 2D array of number of basis functions for each pair of theta/rho
        basis_1d_dist = ParameterMaster.read_1d_basis_dist(basis_1d_path)
        # total number of 1d basis functions in each rho slice
        basis_sizes_1d = [sum(col) for col in zip(*basis_1d_dist)]
        average_basis_size = sum(basis_sizes_1d) / len(basis_sizes_1d)
        average_block_work = average_block_size * average_basis_size

        efficiency_level = 0.725  # by analyzing efficiency charts
        total_work = average_block_work * len(block_sizes)
        efficiency_derivative = -66.6881847337451 * total_work ** -0.357097330357440  # using fitting of job times
        args.nodes = round((efficiency_level - 1) / efficiency_derivative)
        # above coefficients are adjusted for edison so this is an ad hoc adjustment for cori
        args.nodes = round(args.nodes * 0.75)
        args.nprocs = ParameterMaster.compute_cores(args.nodes)

    @staticmethod
    def set_diagonalization_params(config: SpectrumConfig, args: argparse.Namespace):
        fix_basis_jk = config.get_fix_basis_jk()
        basis_K = config.get_basis_k() if fix_basis_jk == 1 else -1
        root_path = config.get_basis_root_path() if fix_basis_jk == 1 else config.get_root_path()
        ks = config.get_ks()
        k_start = ks[0]
        k_end = ks[1]
        sym = (config.get_symmetry() + k_start) % 2

        matrix_size = 0
        for k in range(k_start, k_end + 1):
            k_load = basis_K if fix_basis_jk == 1 else k
            basis_2d_path = ParameterMaster.get_basis_2d_path(root_path, k_load, sym)
            basis_2d_dist = ParameterMaster.read_2d_basis_dist(basis_2d_path)
            matrix_size += sum(basis_2d_dist)
            sym = 1 - sym

        # set up parameters
        ncv = config.get_ncv()
        args.nprocs = int(matrix_size / ncv)
        if args.nprocs > ParameterMaster.get_cores_per_node():
            args.nprocs = args.nprocs - args.nprocs % ParameterMaster.get_cores_per_node()
            args.nprocs = min(args.nprocs, ParameterMaster.get_cores_per_node() * 3)
        args.nodes = ParameterMaster.compute_nodes(args.nprocs)
        if (args.nodes < 1):
            print("Error: nodes < 1")
            print("Matrix size: {0}".format(matrix_size))
            print("ncv: {0}".format(ncv))
            exit()

    @staticmethod
    def set_properties_params(config: SpectrumConfig, args: argparse.Namespace):
        #  root_path = config.get_root_path()
        #  K = config.get_ks()[0]
        #  sym = config.get_symmetry()
        #  if K == -1:
        #      parity = config.get_parity()
        #      spectrum_path = ParameterMaster.get_spectrum_path(root_path, K, sym, parity)
        #  else:
        #      spectrum_path = ParameterMaster.get_spectrum_path(root_path, K, sym)
        #  # make sure diagonalization worked out ok
        #  assert path.isfile(spectrum_path), "diagonalization is not completed, file not found: " + spectrum_path

        # set up parameters
        args.nprocs = config.get_number_of_states()
        args.nodes = ParameterMaster.compute_nodes(args.nprocs)

    @staticmethod
    def set_wfs_print_params(config: SpectrumConfig, args: argparse.Namespace):
        diag_output_path = ParameterMaster.get_diagonalization_output_path(config)
        # make sure diagonalization worked out ok
        assert path.isfile(diag_output_path), "diagonalization is not completed"

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
        config = SpectrumConfig(config_path)
        grid1_points = ParameterMaster.get_grid_points_num(config, 1)
        grid2_points = ParameterMaster.get_grid_points_num(config, 2)
        grid3_points = ParameterMaster.get_grid_points_num(config, 3)
        total_points = grid1_points * grid2_points * grid3_points
        args.nodes = min(int(total_points / ParameterMaster.pes_points_per_node), ParameterMaster.pesprint_max_nodes)
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

    @staticmethod
    def guess_program_name() -> str:
        if path.isfile(ParameterMaster.grid_file_names[0]):
            return "pesprint"
        elif path.isfile("spectrumsdt.config"):
            return "spectrumsdt"
        else:
            raise FileNotFoundError("Could not determine program name automatically, specify program name manually")


def parse_command_line_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generates sbatch input file for ozone calculations")
    parser.add_argument("prg_name", metavar="program name", nargs="?", help="Program name")
    parser.add_argument("-c", "--config", help="Path to configuration file")
    parser.add_argument("-q", "--qos", help="Job QoS")
    parser.add_argument("-t", "--time", type=float, default=0.5, help="Requested job time")
    parser.add_argument("-n", "--nodes", type=int, help="Desired number of nodes")
    parser.add_argument("-np", "--nprocs", type=int, help="Desired number of processes")
    parser.add_argument("-jn", "--jobname", help="Job name")
    parser.add_argument("-on", "--outname", help="Slurm output file name")
    parser.add_argument("-pon", "--prg-out-file-name", default="prg.out", help="Name of a separate file with program output only")
    parser.add_argument("-go", "--gen-only", action="store_true", help="Generate sbatch without submission")
    parser.add_argument("-ht", "--hyperthreading", dest="hyperthreading", action="store_true",
                        help="Specify to enable hyperthreading")
    parser.add_argument("-nm", "--node-multiplier", dest="nodes_mult", type=float, help="Multiplier for implicitly computed number of nodes")
    parser.add_argument("-pm", "--procs-multiplier", dest="procs_mult", type=float, help="Multiplier for implicitly computed number of processors")
    parser.add_argument("-v", "--verbose", action="store_true", help="Makes the script to print additional information")
    parser.add_argument("-pl", "--program-location", help="Explicit path to program folder")
    parser.add_argument("-hn", "--host-name", help="Explicit host name, used to determine node configuration")

    #  parser.add_argument("-bn", "--build-name", default="cori", help="Specifies name of the folder with build")
    parser.add_argument("-fs", "--filesystem", default="none", help="Controls filesystem requirements")
    parser.add_argument("-O0", "--no-opt", action="store_true", help="Specify to use non-optimized version of the code")
    parser.add_argument("-knl", "--knl", dest="haswell", action="store_false", help="Specify to use KNL nodes")

    args = parser.parse_args()
    return args


def assume_haswell_configuration():
    ParameterMaster.cores_per_node = 32
    ParameterMaster.max_shared_cores = 16
    ParameterMaster.max_debug_nodes = 64
    ParameterMaster.threads_per_core = 2

    ParameterMaster.pes_points_per_node = 500000
    ParameterMaster.pesprint_max_nodes = 20
    ParameterMaster.overlaps_work_per_core = 6E+7  # abstract units
    ParameterMaster.overlaps_max_nodes = 100
    ParameterMaster.nodes_type = "haswell"


def assume_knl_configuration():
    ParameterMaster.cores_per_node = 68
    ParameterMaster.max_debug_nodes = 512
    ParameterMaster.threads_per_core = 4

    ParameterMaster.pes_points_per_node = 500000
    ParameterMaster.pesprint_max_nodes = 20
    ParameterMaster.overlaps_work_per_core = 6E+7  # abstract units
    ParameterMaster.overlaps_max_nodes = 100
    ParameterMaster.nodes_type = "knl"


def configure_parameter_master(args: argparse.Namespace):
    ParameterMaster.hyperthreading = args.hyperthreading
    #  ParameterMaster.nodes_mult = args.nodes_mult
    if args.host_name is not None:
        ParameterMaster.host_name = args.host_name
    if args.haswell:
        assume_haswell_configuration()
    else:
        assume_knl_configuration()


def resolve_defaults(args: argparse.Namespace):
    # Some defaults depend on other values so they are set separately
    if args.prg_name is None:
        args.prg_name = ParameterMaster.guess_program_name()
    if args.config is None:
        args.config = "spectrumsdt.config"
    args.config = path.abspath(args.config)

    if args.nodes is None and args.nprocs is not None:
        args.nodes = ParameterMaster.compute_nodes(args.nprocs)
    if args.nprocs is None and args.nodes is not None:
        args.nprocs = ParameterMaster.compute_cores(args.nodes)
    if args.nprocs is None and args.nodes is None:
        if args.prg_name == "spectrumsdt":
            ParameterMaster.set_spectrumsdt_params(args.config, args)
        if args.prg_name == "pesprint":
            ParameterMaster.set_pesprint_params(args.config, args)

    if args.qos is None:
        args.qos = "debug"
        if args.time > 0.5 or args.nodes > ParameterMaster.get_max_debug_nodes():
            args.qos = "regular"

    if args.jobname is None:
        args.jobname = ParameterMaster.generate_job_name(args.config)
    if args.outname is None:
        args.outname = ParameterMaster.generate_out_name()

    if args.program_location is None:
        if args.no_opt:
            args.program_location = path.expandvars("$mybin_O0")
        elif args.haswell:
            args.program_location = path.expandvars("$mybin")
        else:
            args.program_location = path.expandvars("$mybin_knl")

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
