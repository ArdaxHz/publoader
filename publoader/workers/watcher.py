import importlib.util
import logging
import queue
import sys
import threading
import traceback

import pymongo

from publoader.models.database import (database_connection, )
from publoader.models.http import http_client
from publoader.utils.utils import root_path
from publoader.webhook import PubloaderQueueWebhook

logger = logging.getLogger("publoader")

worker_paths = root_path.joinpath("publoader", "workers")
worker_paths.mkdir(parents=True, exist_ok=True)
queue = queue.Queue()


def worker(worker_type: str, worker_module, http_client, queue_webhook, **kwargs):
    """Run the worker."""
    while True:
        item = queue.get()

        try:
            print(f"----{worker_type.title()}: Working on {item['_id']}----")
            logger.debug(f"----{worker_type.title()}: Working on {item['_id']}----")
            worker_module.run(item, http_client, queue_webhook)
        except Exception as e:
            traceback.print_exc()
            logger.exception(f"{worker_type.title()} raised an error.")

        queue.task_done()
        if queue.qsize() == 0:
            queue_webhook.send_queue_finished()


def setup_thread(worker_type, queue_webhook, worker_module, *args, **kwargs):
    """Start the worker thread."""
    with queue.mutex:
        queue.queue.clear()

    chapters = worker_module.fetch_data_from_database()
    for chapter in chapters:
        queue.put(chapter)

    thread = threading.Thread(
        target=worker,
        daemon=True,
        args=(worker_type, worker_module, http_client, queue_webhook),
        kwargs=kwargs,
    )
    thread.start()
    return thread


def open_worker_module(worker_type):
    """Load the runner file."""
    spec = importlib.util.spec_from_file_location(
        worker_type, worker_paths.joinpath(worker_type).with_suffix(".py")
    )
    foo = importlib.util.module_from_spec(spec)
    sys.modules[worker_type] = foo
    spec.loader.exec_module(foo)
    return foo


def main(worker_type: str, table_name: str, webhook_colour: str):
    """Start the watcher."""
    queue_webhook = PubloaderQueueWebhook(worker_type=worker_type, colour=webhook_colour)
    worker_module = open_worker_module(worker_type)

    # Turn-on the worker thread.
    thread = setup_thread(
        worker_type=worker_type,
        queue_webhook=queue_webhook,
        worker_module=worker_module,
    )
    print(f"Starting {worker_type.title()} watcher.")
    logger.info(f"Starting {worker_type.title()} watcher.")

    while True:
        try:
            with database_connection[table_name].watch(
                [{"$match": {"operationType": "insert"}}]
            ) as stream:
                for change in stream:
                    queue.put(change["fullDocument"])

                if not thread.is_alive():
                    print(f"Restarting {worker_type.title()} Thread")
                    thread = setup_thread(queue_webhook=queue_webhook)
        except pymongo.errors.PyMongoError as e:
            print(e)

    # Block until all tasks are done.
    queue.join()
    print("All work completed")
