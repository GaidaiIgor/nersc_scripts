#!/usr/bin/env python3
import numpy as np
import pathlib
from typing import List

import sys
sys.path.append("/global/u2/g/gaidai/SpectrumSDT_ifort/scripts/")
from SpectrumSDTConfig import SpectrumSDTConfig


def get_ozone_molecule(mass: str) -> str:
    """ Returns ozone molecule type based on specified mass string. """
    if mass == "O16, O16, O16":
        return "686"
    if mass == "O16, O18, O16":
        return "686"
    elif mass == "O18, O16, O18":
        return "868"
    else:
        raise Exception("Unknown molecule")


def get_vdw_barriers(molecule: str, sym: int, J_ind: int, K_ind: int) -> List[float]:
    """ Loads VdW barriers correspond to the given arguments. """
    base_load_path = pathlib.Path(__file__).resolve().parent / "script_data" / "barriers" / molecule / f"sym_{sym}"
    pathways = ["B", "A", "S"]
    barriers_pathway = [0] * len(pathways)
    for pathway_ind in range(len(pathways)):
        pathway = pathways[pathway_ind]
        load_path = base_load_path / pathway / "barriers.txt"
        barriers_JK = np.loadtxt(load_path)
        barriers_pathway[pathway_ind] = barriers_JK[K_ind, J_ind]


def get_phi_barrier(molecule: str) -> float:
    """ Returns position of VdW phi barrier between symmetric and asymmetric ozone isotopomers """
    if molecule == "666":
        return 120.0
    elif molecule == "686":
        return 117.65
    elif molecule == "868":
        return 122.35
    else:
        raise Exception("Unknown molecule")


def write_wf_sections(file_name: str, molecule: str, vdw_barrier_positions: List[float], sym_asym_barrier_position: float, Ks: List[int]):
    """ Appends wave function section descriptions corresponding to given arguments to given *file_name*.
        Barrier positions are given in order: B, A, S """
    with open(file_name, "a") as file:
        file.write("\n")
        file.write("wf_sections = (\n")
        file.write("  Covalent B = (\n")
        file.write("    rho = start .. {:.15f}\n".format(vdw_barrier_positions[0]))
        file.write("    phi = 0 .. 60\n")
        file.write("  )\n")
        file.write("\n")
        file.write("  Covalent A = (\n")
        file.write("    rho = start .. {:.15f}\n".format(vdw_barrier_positions[1]))
        file.write("    phi = 60 .. {}\n".format(phi_barrier))
        file.write("  )\n")
        file.write("\n")
        file.write("  Covalent S = (\n")
        file.write("    rho = start .. {:.15f}\n".format(vdw_barrier_positions[2]))
        file.write("    phi = {} .. 180\n".format(phi_barrier))
        file.write("  )\n")
        file.write("\n")
        file.write("  VdW B = (\n")
        file.write("    rho = {:.15f} .. 11\n".format(vdw_barrier_positions[0]))
        file.write("    phi = 0 .. 60\n")
        file.write("  )\n")
        file.write("\n")
        file.write("  VdW A = (\n")
        file.write("    rho = {:.15f} .. 11\n".format(vdw_barrier_positions[1]))
        file.write("    phi = 60 .. {}\n".format(phi_barrier))
        file.write("  )\n")
        file.write("\n")
        file.write("  VdW S = (\n")
        file.write("    rho = {:.15f} .. 11\n".format(vdw_barrier_positions[2]))
        file.write("    phi = {} .. 180\n".format(phi_barrier))
        file.write("  )\n")
        file.write("\n")
        file.write("  Infinity = (\n")
        file.write("    rho = 11 .. end\n")
        file.write("  )\n")
        file.write("\n")
        file.write("  Gamma B (cm^-1) = (\n")
        file.write("    phi = 0 .. 60\n")
        file.write("    stat = gamma\n")
        file.write("  )\n")
        file.write("\n")
        file.write("  Gamma A (cm^-1) = (\n")
        file.write("    phi = 60 .. 180\n")
        file.write("    stat = gamma\n")
        file.write("  )\n")
        file.write("\n")
        file.write("  A*hbar^2 (cm^-1) = (\n")
        file.write("    stat = A\n")
        file.write("  )\n")
        file.write("\n")
        file.write("  B*hbar^2 (cm^-1) = (\n")
        file.write("    stat = B\n")
        file.write("  )\n")
        file.write("\n")
        file.write("  C*hbar^2 (cm^-1) = (\n")
        file.write("    stat = C\n")
        file.write("  )\n")

        if Ks[0] != Ks[1]:
            for K in range(Ks[0], Ks[1] + 1):
                file.write("\n")
                file.write("  K{} = (\n".format(K))
                file.write("    K = {} .. {}\n".format(K, K))
                file.write("  )\n")

        file.write(")\n")


def main():
    """ Reads SpectrumSDT config, determines ozone wf integration boundaries for the specified parameters and adds them to the config. """
    known_Js = list(range(0, 2, 33)) + list(range(36, 4, 65))
    known_Ks = list(range(0, 2, 20))

    config = SpectrumSDTConfig("spectrumsdt.config")
    mass = config.get_mass_str()
    molecule = get_ozone_molecule(mass)
    symmetry = config.get_symmetry()
    J = config.get_J()
    J_ind = known_Js.index(J)
    Ks = config.get_Ks()
    K_ind = known_Ks.index(Ks[0])
    vdw_barrier_positions = get_vdw_barriers(molecule, symmetry, J_ind, K_ind)
    phi_barrier_position = get_phi_barrier(molecule)
    write_wf_sections("spectrumsdt.config", molecule, vdw_barrier_positions, phi_barrier_position, Ks)


main()
