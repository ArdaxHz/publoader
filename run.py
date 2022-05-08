import configparser
import subprocess
import sys
import time
from datetime import time as dtTime
from datetime import timezone
from pathlib import Path

from scheduler import Scheduler, trigger

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


def main(clean_db=False, move_chapters=False):
    """Call the main function of the mangaplus bot."""
    subprocess.call([RUNNER, "mangaplus.py"])


def clean_db():
    """Call the clean_db function of the mangaplus bot."""
    print("Running the clean database function.")
    subprocess.call([RUNNER, "mangaplus.py", "-c"])


if __name__ == "__main__":

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
    main()
    print("End of initial run, starting scheduler.")
    schedule = Scheduler(tzinfo=timezone.utc)
    schedule.weekly(
        trigger.Wednesday(
            dtTime(
                hour=daily_run_time_checks_hour,
                minute=daily_run_time_checks_minute,
                tzinfo=timezone.utc,
            ),
        ),
        clean_db,
        weight=8,
    )
    schedule.daily(
        dtTime(
            hour=daily_run_time_daily_hour,
            minute=daily_run_time_daily_minute,
            tzinfo=timezone.utc,
        ),
        main,
        weight=9,
    )
    schedule.daily(
        dtTime(
            hour=daily_run_time_checks_hour,
            minute=daily_run_time_checks_minute,
            tzinfo=timezone.utc,
        ),
        main,
        weight=1,
    )

    while True:
        schedule.exec_jobs()
        time.sleep(1)
