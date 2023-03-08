import argparse
import configparser
import json
import os
import subprocess
import sys
import time
from datetime import time as dtTime
from datetime import timezone, timedelta
from pathlib import Path

from scheduler import Scheduler

from updater import check_for_update

root_path = Path(".")
config_file_path = root_path.joinpath("config").with_suffix(".ini")


def open_timings():
    timings_path = root_path.joinpath("components", "schedule").with_suffix(".json")
    if not timings_path.exists():
        return {}

    try:
        return json.loads(timings_path.read_bytes())
    except json.JSONDecodeError:
        return {}


def open_config_file() -> configparser.RawConfigParser:
    """Open config file and read values"""
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


def update():
    """Update local repo."""
    check_for_update(root_path)
    install_requirements()


def main(extension_names: list[str] = None, general_run=False):
    """Call the main function of the publoader bot."""
    runner = [sys.executable, "publoader.py"]
    if general_run:
        runner.append("-g")

    if extension_names is not None and extension_names:
        for name in extension_names:
            runner.append(f"-e {name}")

    subprocess.call(runner)


def daily_check_run():
    update()
    print("Running the daily checker function.")
    subprocess.call([sys.executable, "publoader.py", "-g"])


def clean_db():
    """Call the clean_db function of the publoader bot."""
    update()
    print("Running the clean database function.")
    subprocess.call([sys.executable, "publoader.py", "-c"])


def schedule_extensions():
    """Add the timings to the scheduler."""
    same = []
    timings = open_timings()

    for timing in timings:
        extension_timings = timings[timing]
        hour = extension_timings.get("hour", daily_run_time_daily_hour)
        minute = extension_timings.get("minute", daily_run_time_daily_minute)

        for in_same in same:
            if (
                hour == in_same["hour"]
                and in_same["minute"] - 3 <= minute <= in_same["minute"] + 3
                and timing not in in_same["extensions"]
            ):
                in_same["extensions"].append(timing)
                break
            else:
                same.append({"hour": hour, "minute": minute, "extensions": [timing]})
                break
        else:
            same.append({"hour": hour, "minute": minute, "extensions": [timing]})

    for fixed_timing in same:
        schedule.daily(
            dtTime(
                hour=fixed_timing["hour"],
                minute=fixed_timing["minute"],
                tzinfo=timezone.utc,
            ),
            main,
            weight=1,
            alias=", ".join(fixed_timing["extensions"]),
            tags=fixed_timing["extensions"],
            kwargs={"extension_names": list(fixed_timing["extensions"])},
        )


def restart():
    """Restart the script."""
    update()
    print(f"Restarting with args {sys.executable=} {sys.argv=}")
    os.execv(sys.executable, [sys.executable] + sys.argv)


def print_schedule():
    print(schedule)


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
    parser.add_argument(
        "--extension",
        "-e",
        action="append",
        required=False,
        help="Run a specific extension.",
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

    if vargs["extension"] is None:
        extension_to_run = None
    else:
        extension_to_run = [str(extension).strip() for extension in vargs["extension"]]

    if vargs["clean"]:
        clean_db()
    elif vargs["general"]:
        main(extension_names=extension_to_run, general_run=True)

    print("Starting scheduler.")
    schedule = Scheduler(tzinfo=timezone.utc)
    schedule.daily(
        dtTime(
            hour=0,
            minute=0,
            tzinfo=timezone.utc,
        ),
        restart,
        weight=9,
        alias="restarter",
        tags={"restarter"},
    )
    schedule.daily(
        dtTime(
            hour=daily_run_time_checks_hour,
            minute=daily_run_time_checks_minute,
            tzinfo=timezone.utc,
        ),
        daily_check_run,
        weight=8,
        alias="daily_checker",
        tags={"daily_checker"},
    )
    schedule.cyclic(
        timedelta(minutes=30),
        print_schedule,
        weight=5,
        alias="print_schedule",
        tags={"print_schedule"},
    )
    schedule_extensions()
    print(schedule)

    try:
        while True:
            schedule.exec_jobs()
            time.sleep(1)
    except KeyboardInterrupt:
        sys.exit(1)
