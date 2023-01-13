import logging
from pathlib import Path

import git

import publoader.utils.logs

logger = logging.getLogger("debug")


def check_for_update(root_path: Path) -> bool:
    repo = git.Repo(root_path)
    repo.git.clean("-df")
    repo.git.reset("--hard")
    current = repo.head.commit
    changes = repo.remotes.origin.fetch()
    first_change = changes[0]
    remote_commit = first_change.commit
    logger.info(f"Current commit: {current}, remote commit: {remote_commit}")
    if current != remote_commit:
        print(f"Pulling commit {remote_commit}.")
        repo.remotes.origin.pull()
        return True
    return False


if __name__ == "__main__":
    check_for_update(Path("."))
