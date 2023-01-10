import time
import logging

ENV_KEY = None
ARG_KEY = "TESTARG"
INP_KEY = "TESTINP"
NAME = "teststage"

CONTEXT = None

def run(argstr, inputs, env, logger = None):
    if logger is None:
        logger = logging.getLogger(NAME)

    logger.info(f"argstr: {argstr}")
    logger.info(f"inputs: {inputs}")
    logger.info(f"env: {env}")
    logger.info(f"context: {CONTEXT}")
    logger.info(f"sleeping for 5 seconds...")
    time.sleep(5)

    return [f"Test input: {input_val}" for input_val in inputs]
