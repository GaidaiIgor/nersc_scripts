import numpy as np
from scipy.interpolate import griddata


def is_monoisotopomer(molecule):
    """ Returns True if all isotopes are the same. """
    return all([isotope == molecule[0] for isotope in molecule])


def get_ozone_molecule(mass: str) -> str:
    """ Returns ozone molecule type based on specified mass string. """
    return mass[2::5]


def arrange_interp_data(Js, Ks, vals):
    """ Arranges irregular data for interpolation into n x 3 list of points. """
    J_mesh, K_mesh = np.meshgrid(Js, Ks)
    J_1d = J_mesh.flatten()
    K_1d = K_mesh.flatten()
    vals_1d = vals.flatten()
    interp_data = np.stack((J_1d, K_1d, vals_1d), axis=1)
    interp_data = interp_data[interp_data[:, 2] != 0, :]
    return interp_data


def interpolate_JK(Js, Ks, vals, J, K):
    """ Estimates necessary number of states for given J and K. Js and Ks are values of J and K for vals. """
    interp_data = arrange_interp_data(Js, Ks, vals)
    val_interp = griddata(interp_data[:, 0:2], interp_data[:, 2], (J, K))
    return val_interp


