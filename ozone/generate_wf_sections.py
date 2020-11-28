#!/usr/bin/env python3
import math
import pathlib
from typing import List, TextIO

import sys
sys.path.append("/global/u2/g/gaidai/SpectrumSDT_gfortran/scripts/")
from SpectrumSDTConfig import SpectrumSDTConfig


def get_ozone_molecule(mass: str) -> str:
    """ Returns 686 or 868 based on specified mass string """
    if mass == "O16, O18, O16":
        return "686"
    elif mass == "O18, O16, O18":
        return "868"
    else:
        raise Exception("Unknwon molecule")


def select_interpolating_Js(J: int) -> List[int]:
    """ Selects the values of *J_low* and *J_high* used to interpolate a given *J* """
    if J < 4:
        return [4, 8]
    elif J > 56:
        return [52, 56]
    else:
        J_low = int(J / 4) * 4
        J_high = J_low + 4
        return [J_low, J_high]


def get_symmetry_letter(sym_code: int):
    """ Returns symmetry letter associated with the given *sym_code* """
    if sym_code == 0:
        return "S"
    elif sym_code == 1:
        return "A"
    else:
        raise Exception("Wrong symmetry code")


def get_channels_file_path(molecule: str, J: int, K: int, sym: int) -> str:
    """ Generates path to the channels file corresponding to given *molecule*, *J*, *K*, and *symmetry* """
    script_folder_path = pathlib.Path(__file__).resolve().parent
    molecule_folder = molecule + "_full"
    J_folder = "J{:02}".format(J)
    symmetry_letter = get_symmetry_letter(sym)
    K_folder = "K{:02}".format(K) + symmetry_letter
    return script_folder_path / "script_data" / molecule_folder / J_folder / K_folder / "chrecog" / "channels.dat"


def load_lowest_barrier_info(channels_file_path: str) -> List[float]:
    """ Loads positions and energies of lowest barrier tops in channels A, B and S from the specified channels file """
    with open(channels_file_path) as channels_file:
        channel_lines = channels_file.readlines()
    barrier_positions = [0]*3
    for line in channel_lines:
        line_tokens = line.split()
        group_ind = int(line_tokens[1]) - 1
        barrier_position = float(line_tokens[2])
        if barrier_positions[group_ind] == 0:
            barrier_positions[group_ind] = barrier_position
        if all(position > 0 for position in barrier_positions):
            break
    return barrier_positions


def linear_interpolation(point1: List[float], point2: List[float], query_x: float) -> float:
    """ Uses two given points to linearly interpolate the value at *query_x* """
    return (point2[1] - point1[1]) / (point2[0] - point1[0]) * (query_x - point1[0]) + point1[1]


def interpolate_barrier_positions(molecule: str, J: int, symmetry: int) -> List[float]:
    """ Interpolates known barrier positions to estimate positions for given arguments """
    Js_interp = select_interpolating_Js(J)

    # Load interpolating Js barrier position
    barrier_positions_interp = []
    for i in range(len(Js_interp)):
        channels_file_path = get_channels_file_path(molecule, Js_interp[i], 0, symmetry)
        lowest_barriers = load_lowest_barrier_info(channels_file_path)
        barrier_positions_interp.append(lowest_barriers)

    # Interpolate barrier positions for the current J
    barrier_positions = [0]*3
    for i in range(3):
        point1 = [Js_interp[0], barrier_positions_interp[0][i]]
        point2 = [Js_interp[1], barrier_positions_interp[1][i]]
        barrier_positions[i] = linear_interpolation(point1, point2, J)

    return barrier_positions


def get_phi_barrier(molecule: str) -> float:
    """ Returns position of VdW phi barrier between symmetric and asymmetric ozone isotopomers """
    if molecule == "686":
        return 117.65
    elif molecule == "868":
        return 122.35
    else:
        raise Exception("Unknown molecule")


def write_wf_sections(file_name: str, molecule: str, barrier_positions: List[float], Ks: List[int]):
    """ Appends wave function section descriptions corresponding to given *barrier_positions* in ozone *molecule* to given *file_name*.
        Barrier positions are given in order: B, A, S """
    phi_barrier = get_phi_barrier(molecule)
    with open(file_name, "a") as file:
        file.write("\n")
        file.write("wf_sections = {\n")
        file.write("  Covalent B = {\n")
        file.write("    rho = start .. {:.15f}\n".format(barrier_positions[0]))
        file.write("    phi = 0 .. 60\n")
        file.write("  }\n")
        file.write("\n")
        file.write("  Covalent A = {\n")
        file.write("    rho = start .. {:.15f}\n".format(barrier_positions[1]))
        file.write("    phi = 60 .. {}\n".format(phi_barrier))
        file.write("  }\n")
        file.write("\n")
        file.write("  Covalent S = {\n")
        file.write("    rho = start .. {:.15f}\n".format(barrier_positions[2]))
        file.write("    phi = {} .. 180\n".format(phi_barrier))
        file.write("  }\n")
        file.write("\n")
        file.write("  VdW B = {\n")
        file.write("    rho = {:.15f} .. 11\n".format(barrier_positions[0]))
        file.write("    phi = 0 .. 60\n")
        file.write("  }\n")
        file.write("\n")
        file.write("  VdW A = {\n")
        file.write("    rho = {:.15f} .. 11\n".format(barrier_positions[1]))
        file.write("    phi = 60 .. {}\n".format(phi_barrier))
        file.write("  }\n")
        file.write("\n")
        file.write("  VdW S = {\n")
        file.write("    rho = {:.15f} .. 11\n".format(barrier_positions[2]))
        file.write("    phi = {} .. 180\n".format(phi_barrier))
        file.write("  }\n")
        file.write("\n")
        file.write("  Infinity = {\n")
        file.write("    rho = 11 .. end\n".format(barrier_positions[2]))
        file.write("  }\n")

        for K in range(Ks[0], Ks[1] + 1):
            file.write("\n")
            file.write("  K{} = {{\n".format(K))
            file.write("    K = {} .. {}\n".format(K, K))
            file.write("  }\n")
        file.write("}\n")


def main():
    """ Reads SpectrumSDT config, determines ozone wf integration boundaries for the specified parameters and adds them to the config """
    config = SpectrumSDTConfig('spectrumsdt.config')
    mass = config.get_mass_str()
    molecule = get_ozone_molecule(mass)
    J = config.get_J()
    Ks = config.get_Ks()
    symmetry = config.get_symmetry()
    barrier_positions = interpolate_barrier_positions(molecule, J, symmetry)
    write_wf_sections('spectrumsdt.config', molecule, barrier_positions, Ks)


main()
