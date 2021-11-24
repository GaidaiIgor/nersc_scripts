#!/usr/bin/env python
import numpy as np
import sys
sys.path.append('/global/u2/g/gaidai/SpectrumSDT_ifort/scripts/')
from SpectrumSDTConfig import SpectrumSDTConfig


def estimate_states(num_states_ref, Js, Ks, J, K):
    """ Estimates necessary number of states for given J and K. Js and Ks are values of J and K for num_states_ref. """
    J_ind = Js.index(J)
    K_ind = Ks.index(K)
    states = num_states_ref[K_ind, J_ind] * 1.1
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
    num_states_ref = np.loadtxt('/global/u2/g/gaidai/nersc_scripts/ozone/script_data/num_states/686/sym_1/num_states.txt')

    config = SpectrumSDTConfig('spectrumsdt.config')
    J = config.get_J()
    K = config.get_Ks()[0]  # Assuming sym top rotor
    states = estimate_states(num_states_ref, Js, Ks, J, K)
    set_states_placeholder(states)


if __name__ == '__main__':
    main()
