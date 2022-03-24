#!/usr/bin/env python
import numpy as np
import math

from common import *

import sys
sys.path.append('/global/u2/g/gaidai/SpectrumSDT_ifort/scripts/')
from SpectrumSDTConfig import SpectrumSDTConfig


def estimate_states(num_states_ref, Js, Ks, J, K, mult):
    """ Estimates necessary number of states for given J and K. Js and Ks are values of J and K for num_states_ref. """
    J_ind = Js.index(J)
    K_ind = Ks.index(K)
    states = int(math.ceil(num_states_ref[K_ind, J_ind] * mult))
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
    load_path = f'/global/u2/g/gaidai/nersc_scripts/ozone/script_data/num_states/{molecule}/sym_1{sym_suffix}/num_states.txt'
    mult = 1/3
    num_states_ref = np.loadtxt(load_path)

    J = config.get_J()
    K = config.get_Ks()[0]  # Assuming sym top rotor
    states = estimate_states(num_states_ref, Js, Ks, J, K, mult)
    set_states_placeholder(states)


if __name__ == '__main__':
    main()
