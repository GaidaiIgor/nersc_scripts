#!/usr/bin/env python

from __future__ import annotations

import argparse
import math
import os
import os.path as path
import subprocess
from typing import List

import sys
sys.path.append("/global/u2/g/gaidai/SpectrumSDT_ifort/scripts")
from SpectrumSDTConfig import SpectrumSDTConfig


class SubmissionScript:
    def __init__(self, filesystem: str, qos: str, nodes: str, time: str, time_min:str, job_name: str, out_name: str, node_type: str,
            n_procs: str, cores_per_proc: str, program_location: str, program_out_file_name: str, time_file_name: str, sbcast: bool):
        self.program_name = "spectrumsdt"
        self.filesystem = filesystem
        self.qos = qos
        self.nodes = nodes
        self.time = time
        self.time_min = time_min
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
        time_min = "{:.0f}".format(args.time_min * 60)
        nodes = str(args.nodes)
        n_procs = str(args.nprocs)

        cores_per_proc = str(int(ParameterMaster.cores_per_node * args.nodes / args.nprocs) * ParameterMaster.threads_per_core)
        return cls(args.filesystem, args.qos, nodes, time, time_min, args.jobname, args.outname, ParameterMaster.nodes_type,
                n_procs, cores_per_proc, args.program_location, args.program_out_file_name, args.time_file_name, args.sbcast)


    def write(self):
        program_path = path.join(self.program_location, self.program_name)
        tmp_program_path = path.join("/tmp", self.program_name)
        call_location = tmp_program_path if self.sbcast else program_path

        filesystem_line = "#SBATCH -L SCRATCH\n" if self.filesystem == "scratch" else ""
        qos_line = "#SBATCH -q " + self.qos + "\n" if self.qos is not None else ""
        nodes_line = "#SBATCH -N " + self.nodes + "\n" if self.nodes is not None else ""
        cores_line = "#SBATCH -n " + self.n_procs + "\n" if self.qos == "shared" else ""
        time_line = "#SBATCH -t " + self.time + "\n" if self.time is not None else ""
        time_min_line = "#SBATCH --time-min " + self.time_min + "\n" if self.qos == "overrun" or self.qos == "flex" else ""
        job_line = "#SBATCH -J " + self.job_name + "\n" if self.job_name is not None else ""
        out_line = "#SBATCH -o " + self.out_name + "\n" if self.out_name is not None else ""
        node_type_line = "#SBATCH -C " + self.node_type + "\n"
        export_pmi_line = "export PMI_MMAP_SYNC_WAIT_TIME=300\n" if self.sbcast else ""
        sbcast_line = "sbcast --compress=lz4 " + program_path + " " + tmp_program_path + "\n" if self.sbcast else ""
        script = ("#!/bin/bash\n"
                  + filesystem_line
                  + qos_line
                  + nodes_line
                  + cores_line
                  + time_line
                  + time_min_line
                  + job_line
                  + out_line
                  + node_type_line
                  + "\n"
                  + "date\n"
                  + "echo $SLURM_JOB_ID\n"
                  + "rm -f " + self.time_file_name + "\n"
                  + export_pmi_line
                  + "export FORT_FMT_RECL=$((10*1024*1024))\n"
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
    grid_file_names = ["rho_info.txt", "theta_info.txt", "phi_info.txt"]
    pes_file_name = "pes_out.txt"
    config_filename = "spectrumsdt.config"
    stage_result_name = {"basis": "num_vectors_2d.fwc", "overlaps": "time.out", "eigensolve": "states.fwc", "properties": "state_properties.fwc"}

    @staticmethod
    def set_pesprint_params(config_path: str, args: argparse.Namespace):
        args.nodes = 1
        args.nprocs = ParameterMaster.compute_cores(args.nodes)

    @staticmethod
    def get_pes_path(config: SpectrumSDTConfig) -> str:
        grid_folder = config.get_grid_path()
        return path.join(grid_folder, ParameterMaster.pes_file_name)

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
    def set_basis_params(config: SpectrumSDTConfig, args: argparse.Namespace):
        pes_path = ParameterMaster.get_pes_path(config)
        # make sure pesprint worked out ok
        assert path.isfile(pes_path) and path.getsize(pes_path) > 0, "pesprint is not completed"

        # set up parameters
        args.nprocs = ParameterMaster.get_grid_points_num(config, 1)
        args.nodes = ParameterMaster.compute_nodes(args.nprocs)

    @staticmethod
    def set_overlap_params(args: argparse.Namespace):
        args.nodes = 1
        args.nprocs = ParameterMaster.compute_cores(args.nodes)

    @staticmethod
    def set_eigensolve_params(args: argparse.Namespace):
        args.nodes = 1
        args.nprocs = ParameterMaster.compute_cores(args.nodes)

    @staticmethod
    def set_properties_params(config: SpectrumSDTConfig, args: argparse.Namespace):
        # set up parameters
        args.nprocs = config.get_number_of_states() / args.states_per_proc
        args.nodes = ParameterMaster.compute_nodes(args.nprocs)

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
            ParameterMaster.set_overlap_params(args)
        elif stage == "eigensolve":
            ParameterMaster.set_eigensolve_params(args)
        elif stage == "properties":
            ParameterMaster.set_properties_params(config, args)

    @staticmethod
    def compute_nodes(cores: int, hyperthreading: bool = None) -> int:
        """ returns required number of nodes for specified number of cores """
        if hyperthreading is None:
            hyperthreading = ParameterMaster.hyperthreading
        factor = ParameterMaster.threads_per_core if hyperthreading else 1
        return int(math.ceil(cores / (ParameterMaster.cores_per_node * factor)))

    @staticmethod
    def compute_cores(nodes: int, hyperthreading: bool = None) -> int:
        """ returns number of cores in the specified number of nodes """
        if hyperthreading is None:
            hyperthreading = ParameterMaster.hyperthreading
        factor = ParameterMaster.threads_per_core if hyperthreading else 1
        return nodes * ParameterMaster.cores_per_node * factor

    @staticmethod
    def generate_job_name(config_path: str) -> str:
        config_path_parts = config_path.split(ParameterMaster.job_name_separator, 1)
        return config_path_parts[1]


def parse_command_line_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generates sbatch input file for ozone calculations")
    parser.add_argument("-q", "--qos", help="Job QoS")
    parser.add_argument("-t", "--time", type=float, help="Requested job time")
    parser.add_argument("-tm", "--time-min", type=float, help="Minimum time before interruption (for flex and overrun QOS)")
    parser.add_argument("-n", "--nodes", type=int, help="Desired number of nodes")
    parser.add_argument("-np", "--nprocs", type=int, help="Desired number of processes")
    parser.add_argument("-jn", "--jobname", help="Job name")
    parser.add_argument("-on", "--outname", help="Slurm output file name")
    parser.add_argument("-pon", "--program-out-file-name", default="prg.out", help="Name of a separate file with program output only")
    parser.add_argument("-tfn", "--time-file-name", default="time.out", help="Name of a separate file with time output only")
    parser.add_argument("-go", "--gen-only", action="store_true", help="Generate sbatch without submission")
    parser.add_argument("-ht", "--hyperthreading", dest="hyperthreading", action="store_true", help="Specify to enable hyperthreading")
    parser.add_argument("-nm", "--node-multiplier", dest="nodes_mult", type=float, help="Multiplier for implicitly computed number of nodes")
    parser.add_argument("-pm", "--procs-multiplier", dest="procs_mult", type=float, help="Multiplier for implicitly computed number of processors")
    parser.add_argument("-v", "--verbose", action="store_true", help="Makes the script print additional information")
    parser.add_argument("-pl", "--program-location", help="Explicit path to program folder")
    parser.add_argument("-hn", "--host-name", help="Explicit host name, used to determine node configuration")
    parser.add_argument("-sbc", "--sbcast", action="store_true", help="Specify to use sbcast (helps to speed up jobs with large number (1000+) of MPI tasks)")
    #  parser.add_argument("-bn", "--build-name", default="cori", help="Specifies name of the folder with build")
    parser.add_argument("-fs", "--filesystem", default="none", help="Controls filesystem requirements")
    parser.add_argument("-c", "--node-type", default="haswell", choices=["haswell", "amd"], help="Node type")
    parser.add_argument("-r", "--resubmit", type=int, default=1, help="If 0, does not submit job if job result exists.")

    # Stage-specific options
    parser.add_argument("-spp", "--states-per-proc", type=int, default=8, help="Number of states per processor for properties calculation")

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


def resolve_defaults_config(args: argparse.Namespace):
    # Coditionally determines values for some of the None values in args based on SpectrumSDT config in the working directory
    args.config = path.abspath(ParameterMaster.config_filename)

    if args.nodes is None and args.nprocs is not None:
        args.nodes = ParameterMaster.compute_nodes(args.nprocs)
    if args.nprocs is None and args.nodes is not None:
        args.nprocs = ParameterMaster.compute_cores(args.nodes)
    if args.nprocs is None and args.nodes is None:
        ParameterMaster.set_spectrumsdt_params(args.config, args)

    if args.jobname is None:
        args.jobname = ParameterMaster.generate_job_name(path.dirname(args.config))
    if args.outname is None:
        args.outname = "out.slurm"

    if args.program_location is None:
        args.program_location = path.expandvars("$mybin")
        if len(args.program_location) == 0:
            raise "Program location cannot be determined. Specify full path via -pl."

    if args.procs_mult is None and args.nodes_mult is None:
        args.procs_mult = 1
        args.nodes_mult = 1
    if args.procs_mult is None and args.nodes_mult is not None:
        args.procs_mult = 1 if args.nodes_mult > 1 else args.nodes_mult
    if args.nodes_mult is None and args.procs_mult is not None:
        args.nodes_mult = args.procs_mult if args.procs_mult > 1 else 1

    args.nprocs = math.ceil(args.nprocs * args.procs_mult)
    args.nodes = math.ceil(args.nodes * args.nodes_mult)

    if args.time is None:
        if args.qos == "flex":
            args.time = 2.01
        else:
            args.time = 0.5

    if args.time_min is None:
        if args.time is None:
            args.time_min = 2
        else:
            args.time_min = args.time

    if args.qos is None:
        if args.time <= 0.5 and args.nodes <= ParameterMaster.max_debug_nodes:
            args.qos = "debug"
        else:
            args.qos = "regular"

    return args


def main():
    args = parse_command_line_args()
    configure_parameter_master(args)
    resolve_defaults_config(args)

    if args.resubmit == 0:
        config = SpectrumSDTConfig(args.config)
        stage = config.get_stage()
        result_exists = path.isfile(ParameterMaster.stage_result_name[stage])
        if result_exists:
            if stage == 'eigensolve':
                # Also check that result file has enough states computed
                with open(ParameterMaster.stage_result_name[stage]) as result:
                    last_energy = float(result.readlines()[-1].split()[0])
                if last_energy > 1000:
                    return
            else:
                return

    script = SubmissionScript.assemble_script(args)
    script.write()

    if not args.gen_only:
        script.submit()

    if args.verbose:
        print("Program folder is " + args_base.program_location)
        print("Host name is " + ParameterMaster.host_name)
        print("Script name is " + script.script_name)


main()
