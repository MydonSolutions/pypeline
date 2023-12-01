import time, logging
from datetime import datetime

from Pypeline import ProcessNote

NAME = "test"

STATE_data = None
STATE_env = None
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
    global STATE_data, STATE_context
    return (STATE_data, STATE_context)


def rehydrate(dehydration_tuple):
    global STATE_data, STATE_context
    STATE_data = dehydration_tuple[0]
    STATE_context = dehydration_tuple[1]


def run(env = None, logger = None):
    if logger is None:
        logger = logging.getLogger(NAME)

    global STATE_data, STATE_context
    if STATE_context["runs_left"] == 0:
        return False

    for i in range(3, 0, -1):
        logger.info(f"Test process-stage run: count down {i} (ENV={env})")
        time.sleep(1)

    STATE_context["runs_left"] -= 1

    return STATE_data


def setupstage(stage, logger = None):
    if logger is None:
        logger = logging.getLogger(NAME)

    global STATE_context
    if hasattr(stage, "STAGE_CONTEXT"):
        stage.STAGE_CONTEXT = STATE_context


def note(processnote: ProcessNote, **kwargs):
    if kwargs["logger"] is None:
        kwargs["logger"] = logging.getLogger(NAME)
    
    kwargs["logger"].info(f"{processnote.value}: kwargs={kwargs}")


if __name__ == "__main__":
    import socket

    setup(socket.gethostname(), 1)
    print(run())
