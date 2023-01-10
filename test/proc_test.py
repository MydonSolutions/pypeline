import time, logging
from datetime import datetime

NAME = "proc_test"

STATE_data = None
STATE_context = None


def setup(hostname, instance, logger = None):
    if logger is None:
        logger = logging.getLogger(NAME)

    global STATE_data, STATE_context

    STATE_context = {"setup_timestamp": datetime.now(), "runs_left": 2}

    STATE_data = [
        hostname,
        instance,
    ]
    logger.info("Test process-stage setup:", STATE_data)


def dehydrate():
    return (STATE_data, STATE_context)


def rehydrate(dehydration_tuple):
    global STATE_data, STATE_context
    STATE_data = dehydration_tuple[0]
    STATE_context = dehydration_tuple[1]


def run(logger = None):
    if logger is None:
        logger = logging.getLogger(NAME)

    global STATE_data, STATE_context
    if STATE_context["runs_left"] == 0:
        return False

    for i in range(3, 0, -1):
        logger.info("Test process-stage run:", i)
        time.sleep(1)

    STATE_context["runs_left"] -= 1

    return STATE_data


def setupstage(stage, logger = None):
    if logger is None:
        logger = logging.getLogger(NAME)

    global STATE_context
    if hasattr(stage, "CONTEXT"):
        stage.CONTEXT = STATE_context


if __name__ == "__main__":
    import socket

    setup(socket.gethostname(), 1)
    print(run())
