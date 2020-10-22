#!/usr/bin/env python

import argparse
import subprocess
import os
from typing import List, Tuple


class StageManager:
    def __init__(self, args: argparse.Namespace):
        # paths to job stages relative to previous stages
        self.stage_paths = [".", "../overlaps", "../diagonalization", "../properties"]
        self.submission_script_path = "/global/homes/g/gaidai/bin/o3_submit.py"
        self.chain_script_path = "/global/homes/g/gaidai/bin/chain_call_next_stage.py"
        self.stage_options = args.stage_options.split(";")  # type: List[str]
        self.stage_options += [""] * (len(self.stage_paths) + 1 - len(self.stage_options))  # pad array to match number of stages

    def chain_call_stage(self, stage: int):
        """ Generates sbatch script for next stage, modifies it with call to itself for further chain submissions if necessary
        and submits the job """
        # go into stage's directory
        os.chdir(self.stage_paths[stage - 1])
        # form submit command for the stage
        submit_command = self.generate_sbatch_creation_command(stage)
        # generate sbatch script for the stage and store its output, it contains generated file name
        script_output = subprocess.check_output(submit_command, shell=True).decode("utf-8")
        # retrieve the program loc and name from the output
        program_location, host_name, sbatch_name = StageManager.parse_generation_output(script_output)
        self.update_submission_options(program_location, host_name)

        # add a call to self if next stage is not last
        if stage < len(self.stage_paths):
            self.append_self_call(sbatch_name, stage + 1)
        # submit the generated script
        subprocess.call("sbatch " + sbatch_name, shell=True)

    def generate_sbatch_creation_command(self, stage: int) -> str:
        """ Generates submission commands for each stage """
        sbatch_generation_command = self.submission_script_path + " --gen-only --verbose"
        if self.stage_options is not None:
            # 0th item contains common args, the following items are stage specific
            sbatch_generation_command += " " + self.stage_options[0] + " " + self.stage_options[stage]
        return sbatch_generation_command

    @staticmethod
    def parse_generation_output(output: str) -> Tuple[str, str, str]:
        """ Parses script generation output to fish out important information """
        lines = output.split("\n")
        program_location = lines[0].split(" is ")[1]
        host_name = lines[1].split(" is ")[1]
        sbatch_name = lines[2].split(" is ")[1]
        return program_location, host_name, sbatch_name

    def update_submission_options(self, program_location, host_name):
        """ Appends submission options with necessary technicalities """
        if "--program-location" not in self.stage_options[0]:
            self.stage_options[0] += " --program-location " + program_location
        if "--host-name" not in self.stage_options[0]:
            self.stage_options[0] += " --host-name " + host_name

    def append_self_call(self, sbatch_path: str, next_stage: int):
        """ Appends a call to self at the end of sbatch script """
        with open(sbatch_path, "a") as f:
            # preserve original host name for subsequent calls on other machines
            write_string = "\n{0} --next-stage {1}".format(self.chain_script_path, next_stage)
            if self.stage_options is not None:
                write_string += " -so \"{0}\"".format(";".join(self.stage_options))
            f.write(write_string)


def parse_command_line_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generates and chains sbatch input files for ozone calculations")
    parser.add_argument("-ns", "--next-stage", type=int, choices={1, 2, 3, 4}, default=1, help="Next stage number")
    parser.add_argument("-so", "--stage-options", default=";;;;",
                        help="Custom submission parameters for each stage. Stages are separated by semicolon. The first field "
                             "supplies common parameters for all stages")

    args = parser.parse_args()
    return args


def main():
    args = parse_command_line_args()
    stage_manager = StageManager(args)
    stage_manager.chain_call_stage(args.next_stage)


main()
