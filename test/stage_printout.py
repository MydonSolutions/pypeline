import errno
import time
import logging
import argparse

ENV_KEY = None
ARG_KEY = "PRINTOUT_ARG"
INP_KEY = "PRINTOUT_INP"
NAME = "printout"

STAGE_CONTEXT = None


def run(argstr, inputs, env, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)

    parser = argparse.ArgumentParser(
        description="A stage that prints-out its arguments and optionally sleeps.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--sleep",
        default=0,
        type=int,
        help="How long to sleep after printing arguments.",
    )
    args = parser.parse_args(argstr.split(" "))

    message = "\n".join(
        [
            f"argstr: {argstr}",
            f"inputs: {inputs}",
            f"env: {env}",
            f"stage_context: {STAGE_CONTEXT}",
        ]
    )
    if args.sleep > 0:
        message += f"\n\nsleeping for {args.sleep} seconds..."
    logger.info(message)

    if (
        STAGE_CONTEXT is not None
        and STAGE_CONTEXT.get("runs_left", -1) == 1
        and STAGE_CONTEXT.get("raise_exception", True)
    ):
        raise OSError(errno.EAGAIN, "Testing the retry method")

    if args.sleep > 0:
        time.sleep(args.sleep)

    return [f"Test input: {input_val}" for input_val in inputs]
