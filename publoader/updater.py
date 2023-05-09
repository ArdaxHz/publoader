import json
import logging
import shutil
import time
from pathlib import Path

import requests
from github import Github
from github.Commit import Commit

from publoader.utils.config import config, resources_path
from publoader.utils.utils import root_path

logger = logging.getLogger("publoader")


class PubloaderUpdater:
    def __init__(self):
        self.root_path = root_path
        self.update_path = self.root_path.joinpath("temp")
        self.update_path.mkdir(parents=True, exist_ok=True)

        self.commits_file = resources_path.joinpath(config["Paths"]["commits_path"])
        self.github = Github(config["Repo"]["github_access_token"])
        self.local_commits = self._open_commits()
        self.latest_commit_sha = self.local_commits.get("base_repo")
        self.latest_extension_sha = self.local_commits.get("extension_repo")

        self.repo_owner = config["Repo"]["repo_owner"]
        self.base_repo = config["Repo"]["base_repo_path"]
        self.extensions_repo = config["Repo"]["extensions_repo_path"]
        self.extensions_path = Path("publoader/extensions")

    def _open_commits(self):
        """Open the commits file."""
        try:
            with open(self.commits_file, "r") as login_file:
                token = json.load(login_file)
            return token
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _save_commits(self, data=None):
        """Save the commits file."""
        if data is None:
            data = {
                "base_repo": self.latest_commit_sha,
                "extension_repo": self.latest_extension_sha,
            }

        with open(self.commits_file, "w") as login_file:
            login_file.write(json.dumps(data, indent=4))

    def _get_latest_commit(self, repo):
        commits = repo.get_commits()
        latest_commit: Commit = commits[0]
        return latest_commit

    def download_file(self, root_path, content_data):
        file_name = content_data.name
        file_remote_path = content_data.path
        file_path = root_path.joinpath(file_remote_path)
        file_path_parent = file_path.parent
        file_path_parent.mkdir(parents=True, exist_ok=True)

        download_url = content_data.download_url
        logger.info(f"Downloading file {file_path}, link: {download_url}")

        response = requests.get(download_url)

        if response.status_code == 200:
            content = response.content
            file_path.write_bytes(content)
            return False
        return True

    def download_content(self, repo, root_path, current_path, failed_download=False):
        logger.info(
            f"Contents path {repo=}, {root_path=}, {current_path=}, {failed_download=}"
        )

        root_path.mkdir(parents=True, exist_ok=True)

        all_content = repo.get_contents(current_path)
        root_files = [file for file in all_content if file.type == "file"]
        directories = [direc for direc in all_content if direc.type == "dir"]

        for file in root_files:
            failed_download = self.download_file(root_path, file)
            time.sleep(2)

        for direc in directories:
            self.download_content(
                repo=repo,
                root_path=root_path,
                current_path=direc.path,
                failed_download=failed_download,
            )
            time.sleep(4)

        return failed_download

    def fetch_repo(self, repo_name, commit_sha_var, download_path):
        repo = self.github.get_repo(f"{self.repo_owner}/{repo_name}")
        logger.info(f"Checking for update in: {repo}")

        latest_remote_commit = self._get_latest_commit(repo)
        if commit_sha_var is not None and commit_sha_var == latest_remote_commit.sha:
            logger.info(
                f"No new commit, not updating. Latest commit: {latest_remote_commit.sha}"
            )
            return

        logger.info(f"Update found, downloading {latest_remote_commit.sha}")
        failed_download = self.download_content(repo, download_path, "")
        return failed_download

    def move_files(self):
        shutil.copytree(self.update_path, self.root_path, copy_function=shutil.move)

    def update(self):
        base_repo_failed = self.fetch_repo(
            self.base_repo, self.latest_commit_sha, self.root_path
        )

        time.sleep(8)

        extensions_repo_failed = self.fetch_repo(
            self.extensions_repo, self.latest_extension_sha, self.extensions_path
        )

        if base_repo_failed or extensions_repo_failed:
            logger.warning(f"Downloading new repo update failed, not updating.")
            shutil.rmtree(self.update_path, ignore_errors=True)
            return

        self.move_files()
        self._save_commits()
