#!/usr/bin/env python
import numpy as np
import os
import os.path as path


def main():
    #  root_path = '/global/cfs/cdirs/m409/gaidai/ozone/dev/686/emax_600/rmax_20/rstep_0.65'
    #  root_path = '/global/cfs/cdirs/m409/gaidai/ozone/dev/686/emax_600/rmax_20/rstep_0.65/half_integers'
    root_path = '/global/cfs/cdirs/m409/gaidai/ozone/dev/676/'
    molecule = '676'
    Js = list(range(0, 33)) + list(range(36, 65, 4))
    Ks = list(range(0, 21))
    sym = 1
    sym_suffix = ''
    target_energy = 1000

    num_states = np.zeros((len(Ks), len(Js)))
    for J_ind, J in enumerate(Js):
        for K_ind, K in enumerate(Ks):
            if J > 32 and K % 2 == 1:
                continue
            if K <= J:
                states_path = path.join(root_path, f'J_{J}', f'K_{K}', f'symmetry_{sym}', 'eigensolve', 'states.fwc')
                if path.exists(states_path) and os.stat(states_path).st_size != 0:
                    state_energies = np.loadtxt(states_path, skiprows=1, usecols=[0])
                    target_inds = np.where(state_energies > target_energy)[0]
                    if len(target_inds) == 0:
                        print(f'Insufficient number of states in {J}, {K}')
                        target_ind = -1
                    else:
                        target_ind = target_inds[0]
                    num_states[K_ind, J_ind] = target_ind + 1
                else:
                    print(f'{J}, {K} not found')
                    num_states[K_ind, J_ind] = 0

    save_dir = path.join('script_data', 'num_states', molecule, f'sym_{sym}{sym_suffix}')
    os.makedirs(save_dir, exist_ok=True)
    np.savetxt(path.join(save_dir, 'num_states.txt'), num_states)


if __name__ == '__main__':
    main()
