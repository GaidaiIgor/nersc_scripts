#!/usr/bin/env python
import shutil
from pathlib import Path


def main():
    """Copies file state_properties.fwc from the working directory to the target directory.
    Target directory path is formed by inserting 'results' before the J folder and remove stage folder name."""
    states_path = Path("state_properties.fwc").absolute()
    target_path_parts = list(states_path.parts)
    del target_path_parts[-2] # delete stage folder
    j_part_index = [part.startswith("J_") for part in target_path_parts].index(True)
    target_path_parts.insert(j_part_index, "results")
    target_path = Path(*target_path_parts)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(states_path, target_path)


if __name__ == "__main__":
    main()
