import time
from datetime import datetime

STATE_data = None
STATE_context = None


def setup(hostname, instance):
    global STATE_data, STATE_context

    STATE_context = {"setup_timestamp": datetime.now(), "runs_left": 2}

    STATE_data = [
        hostname,
        instance,
    ]
    print("Test process-stage setup:", STATE_data)


def run():
    if STATE_context["runs_left"] == 0:
        return False

    global STATE_data

    for i in range(3, 0, -1):
        print("Test process-stage run:", i)
        time.sleep(1)

    STATE_context["runs_left"] -= 1

    return STATE_data


def setupstage(stage):
    global STATE_context
    if hasattr(stage, "PROC_CONTEXT"):
        stage.PROC_CONTEXT = STATE_context


if __name__ == "__main__":
    import socket

    setup(socket.gethostname(), 1)
    print(run())
