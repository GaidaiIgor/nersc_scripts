#!/usr/bin/env python
import math
import numpy as np
from scipy.interpolate import griddata

from common import *

import sys
sys.path.append('/global/u2/g/gaidai/SpectrumSDT_ifort/scripts/')
from SpectrumSDTConfig import SpectrumSDTConfig


def arrange_interp_data(num_states_ref, Js, Ks):
    """ Arranges irregular data for interpolation into n x 3 list of points. """
    J_mesh, K_mesh = np.meshgrid(Js, Ks)
    J_1d = J_mesh.flatten()
    K_1d = K_mesh.flatten()
    num_states_1d = num_states_ref.flatten()
    interp_data = np.stack((J_1d, K_1d, num_states_1d), axis=1)
    interp_data = interp_data[interp_data[:, 2] != 0, :]
    return interp_data


def estimate_states(num_states_ref, Js, Ks, J, K, mult):
    """ Estimates necessary number of states for given J and K. Js and Ks are values of J and K for num_states_ref. """
    interp_data = arrange_interp_data(num_states_ref, Js, Ks)
    states = griddata(interp_data[:, 0:2], interp_data[:, 2], (J, K))
    states = int(math.ceil(states * mult))
    return states


def set_states_placeholder(states):
    """ Sets num_states placeholder in the config file in working directory to a given value. """
    with open('spectrumsdt.config', 'r+') as config:
        content = config.read()
        formatted = content.format(num_states=states)
        config.seek(0)
        config.write(formatted)
        config.truncate()


def main():
    """ Estimates necessary number of states for values J and K specified in config file and replaces num_states placeholder in config file with this number. """
    Js = list(range(0, 33, 2)) + list(range(36, 65, 4))
    Ks = list(range(0, 21, 2))
    config = SpectrumSDTConfig('spectrumsdt.config')
    mass = config.get_mass_str()
    molecule = get_ozone_molecule(mass)
    use_half_integers = config.get_half_integers()
    sym_suffix = 'H' if use_half_integers else ''
    mult = 1 if is_monoisotopomer(molecule) else 1/3
    mult *= 1.05
    load_path = f'/global/u2/g/gaidai/nersc_scripts/ozone/script_data/num_states/{molecule}/sym_1{sym_suffix}/num_states.txt'
    num_states_ref = np.loadtxt(load_path)

    J = config.get_J()
    K = config.get_Ks()[0]  # Assuming sym top rotor
    states = estimate_states(num_states_ref, Js, Ks, J, K, mult)
    set_states_placeholder(states)


if __name__ == '__main__':
    main()
