#!/usr/bin/env python
import shutil
from pathlib import Path


def main():
    states_path = Path("states.fwc").absolute()
    target_path_parts = list(states_path.parts)
    del target_path_parts[-2]
    k_folder_index = [part.startswith("K_") for part in target_path_parts].index(True)
    target_path_parts.insert(k_folder_index, "results")
    target_path_parts[-1] = "states.ssdtp"
    target_path = Path(*target_path_parts)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(states_path, target_path)


if __name__ == "__main__":
    main()
