#!/usr/bin/env python3
import numpy as np
import pathlib
from typing import Dict, List

import sys
sys.path.append("/global/u2/g/gaidai/SpectrumSDT_ifort/scripts/")
from SpectrumSDTConfig import SpectrumSDTConfig


def get_ozone_molecule(mass: str) -> str:
    """ Returns ozone molecule type based on specified mass string. """
    if mass == "O16, O16, O16":
        return "666"
    if mass == "O16, O18, O16":
        return "686"
    elif mass == "O18, O16, O18":
        return "868"
    else:
        raise Exception("Unknown molecule")


def get_vdw_barriers(molecule: str, sym: int, J_ind: int, K_ind: int) -> Dict[str, float]:
    """ Loads VdW barriers correspond to the given arguments. """
    base_load_path = pathlib.Path(__file__).resolve().parent / "script_data" / "barriers" / molecule / f"sym_{sym}"
    pathways = ["all"] if molecule == "666" else ["B", "A", "S"]
    vdw_barriers = {}
    for pathway in pathways:
        load_path = base_load_path / pathway / "barriers.txt"
        barriers_JK = np.loadtxt(load_path)
        vdw_barriers[pathway] = barriers_JK[K_ind, J_ind]
    return vdw_barriers


def get_phi_barriers(molecule: str) -> Dict[str, List[float]]:
    """ Returns phi-range for each pathway. """
    phi_barriers = {}
    if molecule == "666":
        phi_barriers["S"] = [0, 360]
    else:
        if molecule == "686":
            sym_asym_barrier = 117.65
        elif molecule == "868":
            sym_asym_barrier = 122.35
        else:
            raise Exception("Unknown molecule")
        phi_barriers["B"] = [0, 60]
        phi_barriers["A"] = [60, sym_asym_barrier]
        phi_barriers["S"] = [sym_asym_barrier, 180]
    return phi_barriers


def write_wf_sections(file_name: str, molecule: str, vdw_barriers: Dict[str, float], phi_barriers: Dict[str, List[float]], Ks: List[int]):
    """ Appends wave function section descriptions corresponding to given arguments to given *file_name*.
        Barrier positions are given in order: B, A, S. """
    with open(file_name, "a") as file:
        file.write("\n")
        file.write("wf_sections = (\n")
        for pathway, vdw_barrier in vdw_barriers.items():
            file.write(f"  Covalent {pathway} = (\n")
            file.write(f"    rho = start .. {vdw_barrier}\n")
            if molecule == "686" or molecule == "868":
                file.write(f"    phi = {phi_barriers[pathway][0]} .. {phi_barriers[pathway][1]}\n")
            file.write("  )\n")
            file.write("\n")
        for pathway, vdw_barrier in vdw_barriers.items():
            file.write(f"  VdW {pathway} = (\n")
            file.write(f"    rho = {vdw_barrier} .. 11\n")
            if molecule == "686" or molecule == "868":
                file.write(f"    phi = {phi_barriers[pathway][0]} .. {phi_barriers[pathway][1]}\n")
            file.write("  )\n")
            file.write("\n")
        file.write("  Infinity = (\n")
        file.write("    rho = 11 .. end\n")
        file.write("  )\n")
        file.write("\n")
        if molecule == "686" or molecule == "868":
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
    vdw_barriers = get_vdw_barriers(molecule, symmetry, J_ind, K_ind)
    phi_barriers = get_phi_barriers(molecule)
    write_wf_sections("spectrumsdt.config", molecule, vdw_barriers, phi_barriers, Ks)


main()
