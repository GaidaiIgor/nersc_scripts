def is_monoisotopomer(molecule):
    """ Returns True if all isotopes are the same. """
    return all([isotope == molecule[0] for isotope in molecule])


def get_ozone_molecule(mass: str) -> str:
    """ Returns ozone molecule type based on specified mass string. """
    return mass[2::5]

