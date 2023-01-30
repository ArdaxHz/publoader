import argparse
import configparser
import subprocess
import sys
import time
from datetime import time as dtTime
from datetime import timezone
from pathlib import Path

from scheduler import Scheduler

from updater import check_for_update

root_path = Path(".")
config_file_path = root_path.joinpath("config").with_suffix(".ini")

if sys.platform.startswith("linux"):
    RUNNER = "python3"
else:
    RUNNER = "python"


def open_config_file() -> configparser.RawConfigParser:
    # Open config file and read values
    if config_file_path.exists():
        config = configparser.RawConfigParser()
        config.read(config_file_path)
    else:
        raise FileNotFoundError("Config file not found.")

    return config


config = open_config_file()


def install_requirements():
    for file in root_path.rglob("requirements.txt"):
        print(f"Installing requirements from {file.resolve()}")
        try:
            successful_install = subprocess.run(f'pip install -r "{file.resolve()}"')
        except FileNotFoundError:
            continue
        print(
            f"Requirements installation completed with error code {successful_install.returncode} for file {file.resolve()}"
        )


def main(general_run=False):
    """Call the main function of the publoader bot."""
    runner = [RUNNER, "publoader.py"]
    if general_run:
        runner.append("-g")

    subprocess.call(runner)


def daily_check_run():
    check_for_update(root_path)
    install_requirements()
    print("Running the clean database function.")
    subprocess.call([RUNNER, "publoader.py"])


def clean_db():
    """Call the clean_db function of the publoader bot."""
    check_for_update(root_path)
    install_requirements()
    print("Running the clean database function.")
    subprocess.call([RUNNER, "publoader.py", "-c"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--clean",
        "-c",
        default=False,
        const=True,
        nargs="?",
        help="Clean the database.",
    )
    parser.add_argument(
        "--general",
        "-g",
        default=False,
        const=True,
        nargs="?",
        help="General run of the bot.",
    )

    vargs = vars(parser.parse_args())

    daily_run_time_daily_hour = int(
        config["User Set"]["bot_run_time_daily"].split(":")[0]
    )
    daily_run_time_daily_minute = int(
        config["User Set"]["bot_run_time_daily"].split(":")[1]
    )
    daily_run_time_checks_hour = int(
        config["User Set"]["bot_run_time_checks"].split(":")[0]
    )
    daily_run_time_checks_minute = int(
        config["User Set"]["bot_run_time_checks"].split(":")[1]
    )

    print("Initial run of bot.")

    if vargs["clean"]:
        clean_db()
    elif vargs["general"]:
        main(True)
    else:
        main()

    print("End of initial run, starting scheduler.")
    schedule = Scheduler(tzinfo=timezone.utc, max_exec=1)
    schedule.hourly(
        dtTime(
            minute=0,
            tzinfo=timezone.utc,
        ),
        main,
        weight=1,
    )
    schedule.daily(
        dtTime(
            hour=daily_run_time_checks_hour,
            minute=daily_run_time_checks_minute,
            tzinfo=timezone.utc,
        ),
        daily_check_run,
        weight=9,
    )

    while True:
        schedule.exec_jobs()
        time.sleep(1)
