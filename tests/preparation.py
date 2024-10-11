import os
from open_mastr.utils.config import get_project_home_dir


def create_credentials_file():
    """Use token and user stored in GitHub secrets for creating credentials file

    This is used to allow test workflow to access MaStR database.
    """
    credentials_file = os.path.join(get_project_home_dir(), "config", "credentials.cfg")

    token = os.getenv("MASTR_TOKEN")
    user = os.getenv("MASTR_USER")
    section_title = "[MaStR]"

    file_content = f"{section_title}\n" f"user = {user}\n" f"token = {token}\n"

    with open(credentials_file, "w") as credentials_fh:
        credentials_fh.write(file_content)
