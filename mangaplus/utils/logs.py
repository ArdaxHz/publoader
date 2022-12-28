import logging
from datetime import date, timedelta, datetime
from pathlib import Path

from .utils import root_path
from .config import max_log_days


def format_log_dir_path(directory_name: str):
    log_folder_path = root_path.joinpath("logs").joinpath(directory_name)
    log_folder_path.mkdir(parents=True, exist_ok=True)
    return log_folder_path


current_date = date.today()
last_date_keep_logs = current_date - timedelta(days=max_log_days)

bot_logs_folder_path = format_log_dir_path("bot")
webhook_logs_folder_path = format_log_dir_path("webhook")
debug_logs_folder_path = format_log_dir_path("debug")


def setup_logs(
    logger_name: str,
    path: Path = bot_logs_folder_path,
    logger_filename: str = None,
):
    if logger_filename is None:
        logger_filename = logger_name

    filename = f"{logger_filename}_{str(current_date)}.log"

    logs_path = path.joinpath(filename)
    fileh = logging.FileHandler(logs_path, "a")
    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-8s [%(filename)s:%(funcName)s:%(lineno)d] %(message)s"
    )
    fileh.setFormatter(formatter)

    log = logging.getLogger(logger_name)  # root logger
    # for hdlr in log.handlers[:]:  # remove all old handlers
    #     if isinstance(hdlr, logging.FileHandler):
    #         log.removeHandler(hdlr)
    log.addHandler(fileh)
    log.setLevel(logging.DEBUG)


setup_logs(
    logger_name="mangaplus",
    path=bot_logs_folder_path,
    logger_filename="mplus_md_uploader",
)
setup_logs(
    logger_name="debug",
    path=debug_logs_folder_path,
    logger_filename="mplus_md_uploader_debug",
)
setup_logs(
    logger_name="webhook",
    path=webhook_logs_folder_path,
    logger_filename="webhook",
)

_logger = logging.getLogger("mangaplus")


def clear_old_logs(folder_path: Path):
    for log_file in folder_path.glob("*.log"):
        file_date = datetime.fromtimestamp(log_file.stat().st_mtime).date()
        if file_date < last_date_keep_logs:
            _logger.debug(f"{log_file.name} is over {max_log_days} days old, deleting.")
            log_file.unlink()


clear_old_logs(bot_logs_folder_path)
clear_old_logs(webhook_logs_folder_path)
clear_old_logs(debug_logs_folder_path)
