import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import time as dtTime, timezone
from importlib import reload

from scheduler import Scheduler

from publoader.updater import PubloaderUpdater
from publoader.utils.config import (
    daily_run_time_checks_hour,
    daily_run_time_checks_minute,
    daily_run_time_daily_hour,
    daily_run_time_daily_minute,
)
from publoader.utils.utils import get_current_datetime, root_path
from publoader.models.database import get_database_connection
from publoader.workers import worker

logger = logging.getLogger("publoader")


def main(
    database_connection,
    extension_names: list[str] = None,
    general_run=False,
    clean_db=False,
):
    """Call the main function of the publoader bot."""
    from publoader import publoader

    reload(publoader)
    publoader.open_extensions(
        database_connection,
        names=extension_names,
        general_run=general_run,
        clean_db=clean_db,
    )


def open_timings():
    """Open the timings file."""
    timings = {}

    for schedule_file in root_path.joinpath("publoader", "extensions").glob(
        "schedule*.json"
    ):
        try:
            timings.update(json.loads(schedule_file.read_bytes()))
        except json.JSONDecodeError:
            pass
    return timings


def schedule_extensions(database_connection):
    """Add the timings to the scheduler."""
    same = []
    timings = open_timings()
    now = get_current_datetime()

    for timing in timings:
        extension_timings = timings[timing]
        day = extension_timings.get("day")
        hour = extension_timings.get("hour", daily_run_time_daily_hour)
        minute = extension_timings.get("minute", daily_run_time_daily_minute)

        if day is not None and day != now.day:
            continue

        # Join extensions to run together if they are scheduled to run within seven minutes of each other
        for in_same in same:
            if (
                hour == in_same["hour"]
                and in_same["minute"] - 7 <= minute <= in_same["minute"] + 7
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
            kwargs={
                "database_connection": database_connection,
                "extension_names": list(fixed_timing["extensions"]),
            },
        )


def install_requirements():
    """Install requirements for the extensions."""
    for file in root_path.rglob("requirements.txt"):
        print(f"Installing requirements from {file.resolve()}")
        try:
            successful_install = subprocess.run(f'pip install -r "{file.resolve()}"')
        except FileNotFoundError:
            continue
        print(
            "Requirements installation completed with error code",
            f"{successful_install.returncode} for file {file.resolve()}",
        )


def restart():
    """Restart the script."""
    worker.kill()
    updater = PubloaderUpdater()
    updater.update()
    install_requirements()

    print(f"Restarting with args {sys.executable=} {sys.argv=}")
    os.execv(sys.executable, [sys.executable, sys.argv[0]])


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
        "--force",
        "-f",
        default=False,
        const=True,
        nargs="?",
        help="Force run the bot, if extensions is unspecified, run all.",
    )
    parser.add_argument(
        "--extension",
        "-e",
        action="append",
        required=False,
        help="Run a specific extension.",
    )
    parser.add_argument(
        "--update",
        "-u",
        default=False,
        const=True,
        nargs="?",
        help="Update the bot.",
    )

    vargs = vars(parser.parse_args())

    if vargs["update"]:
        restart()

    database_connection = get_database_connection()
    worker.main(database_connection)

    if vargs["extension"] is None:
        extension_to_run = None
    else:
        extension_to_run = [str(extension).strip() for extension in vargs["extension"]]

    if vargs["force"] or vargs["clean"]:
        main(
            database_connection,
            extension_names=extension_to_run,
            general_run=vargs["force"],
            clean_db=vargs["clean"],
        )

    print(
        "--------------------------------------------------Starting scheduler--------------------------------------------------"
    )
    schedule = Scheduler(tzinfo=timezone.utc, max_exec=1)
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
        main,
        weight=8,
        alias="daily_checker",
        tags={"daily_checker"},
        kwargs={
            "database_connection": database_connection,
        },
    )
    schedule_extensions(database_connection)
    print(schedule)

    try:
        while True:
            schedule.exec_jobs()
            time.sleep(1)
    except KeyboardInterrupt:
        worker.kill()
        sys.exit(1)
