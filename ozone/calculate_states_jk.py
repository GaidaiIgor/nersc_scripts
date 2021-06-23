#!/usr/bin/env python
import sys
sys.path.append("/global/u2/g/gaidai/SpectrumSDT_ifort/scripts/")
from SpectrumSDTConfig import SpectrumSDTConfig


def predict_states(molecule, J, K):
    """Predicts necessary number of states for a given molecule, J and K. Coefficients are based on external fits."""
    if molecule == "O16, O18, O16":
        states = round(2500 - 0.177526537939908 * J ** 2 - 1.1621474648769 * J - 0.867466517857143 * K ** 2 - 59.6746651785714 * K)
    else:
        raise Exception("Unrecognized molecule")

    return states


def set_states_placeholder(states):
    """Sets num_states placeholder in the config file in working directory to a given value."""
    with open("spectrumsdt.config", "r+") as config:
        content = config.read()
        formatted = content.format(num_states=states)
        config.seek(0)
        config.write(formatted)
        config.truncate()


def main():
    """Predicts necessary number of states for values J and K specified in config file and replaces num_states placeholder in config file with this number."""
    config = SpectrumSDTConfig("spectrumsdt.config")
    molecule = config.get_mass_str()
    J = config.get_J()
    K = config.get_Ks()[0]  # Assuming sym top rotor
    states = predict_states(molecule, J, K)
    set_states_placeholder(states)


if __name__ == "__main__":
    main()
