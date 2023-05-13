import multiprocessing

from publoader.workers import watcher


def main(restart_threads=True):
    """Initialise watcher processes."""
    try:
        watchers = [
            {"name": "uploader", "table": "to_upload", "colour": "26D454"},
            {"name": "deleter", "table": "to_delete", "colour": "C43542"},
            {"name": "editor", "table": "to_edit", "colour": "FFF71C"},
        ]
        for worker in watchers:
            process = multiprocessing.Process(
                target=watcher.main,
                kwargs={
                    "worker_type": worker["name"],
                    "table_name": worker["table"],
                    "webhook_colour": worker["colour"],
                    "restart_threads": restart_threads,
                },
            )
            process.start()
    except KeyboardInterrupt:
        kill()


def kill():
    """Kill the sub-processes."""
    print("Killing watcher processes.")

    for process in multiprocessing.active_children():
        process.terminate()
