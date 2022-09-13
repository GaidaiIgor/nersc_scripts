#!/usr/bin/env python
import math
import numpy as np

from common import *

import sys
sys.path.append('/global/u2/g/gaidai/SpectrumSDT_ifort/scripts/')
from SpectrumSDTConfig import SpectrumSDTConfig


def estimate_states(Js, Ks, num_states_ref, J, K, mult):
    """ Estimates necessary number of states for given J and K. Js and Ks are values of J and K for num_states_ref. """
    states_interp = interpolate_JK(Js, Ks, num_states_ref, J, K)
    states = int(math.ceil(states_interp * mult(K)))
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
    Js = list(range(0, 33)) + list(range(36, 65, 4))
    Ks = list(range(0, 21))
    config = SpectrumSDTConfig('spectrumsdt.config')
    mass = config.get_mass_str()

    molecule = get_ozone_molecule(mass)
    molecule = '666'

    sym_name = config.get_full_symmetry_name()

    mult = 1 if is_monoisotopomer(molecule) else 1/3
    mult = lambda K: 1.15 + 0.02*K

    load_path = f'/global/u2/g/gaidai/nersc_scripts/ozone/script_data/num_states/{molecule}/sym_{sym_name}/num_states.txt'
    num_states_ref = np.loadtxt(load_path)

    J = config.get_J()
    K = config.get_Ks()[0]  # Assuming sym top rotor
    states = estimate_states(Js, Ks, num_states_ref, J, K, mult)
    set_states_placeholder(states)


if __name__ == '__main__':
    main()
