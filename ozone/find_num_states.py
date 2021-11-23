#!/usr/bin/env python
import numpy as np
import os.path as path


def main():
    root_path = '/global/cfs/cdirs/m409/gaidai/ozone/dev/emax_600/rmax_20/rstep_0.65'
    Js = list(range(0, 33, 2)) + list(range(36, 65, 4))
    Ks = list(range(0, 21, 2))
    sym = 1
    target_energy = 1000

    num_states = np.zeros((len(Ks), len(Js)))
    for J_ind in range(len(Js)):
        J = Js[J_ind]
        for K_ind in range(len(Ks)):
            K = Ks[K_ind]
            if K <= J:
                states_path = path.join(root_path, f'J_{J}', f'K_{K}', f'symmetry_{sym}', 'eigensolve', 'states.fwc')
                state_energies = np.loadtxt(states_path, skiprows=1, usecols=[0])
                target_ind = np.where(state_energies > target_energy)[0][0]
                num_states[K_ind, J_ind] = target_ind + 1

    np.savetxt(path.join('script_data', 'num_states', '686', f'sym_{sym}', 'num_states.txt'), num_states)


if __name__ == '__main__':
    main()
