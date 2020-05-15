import os

open_mastr_home = os.path.join(os.path.expanduser("~"), ".open-MaStR")


def create_open_mastr_dir(dir):
    os.mkdir(dir)
