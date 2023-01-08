import argparse
import time
import socket
import logging
from datetime import datetime

import redis

from Pypeline import import_stage, get_redis_keys_in_use, process
from Pypeline.identifier import Identifier
from Pypeline.redis_interface import RedisInterface
from Pypeline.log_formatter import LogFormatter


def main():
    """ Entrypoint for a Pypeline"""

    parser = argparse.ArgumentParser(
        description="A python-pipeline executable, with a Redis interface.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("instance", type=int, help="The instance ID of the pypeline.")
    parser.add_argument("procstage", type=str, help="The name of process stage.")
    parser.add_argument(
        "-kv",
        type=str,
        nargs="*",
        default=["#STAGES=skip"],
        help="key=value strings to set in the pypeline's Redis Hash.",
    )
    parser.add_argument(
        "--redis-hostname",
        type=str,
        default="redishost",
        help="The hostname of the Redis server.",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="The port of the Redis server.",
    )
    args = parser.parse_args()

    initial_stage_dict = {}
    initial_stage_name = args.procstage
    assert import_stage(initial_stage_name, stagePrefix="proc", definition_dict=initial_stage_dict)
    initial_stage = initial_stage_dict.pop(initial_stage_name)

    instance_hostname = socket.gethostname()
    instance_id = args.instance
    initial_stage.setup(
        instance_hostname,
        instance_id,
    )

    redis_interface = RedisInterface(
        instance_hostname,
        instance_id,
        redis.Redis(
            host=args.redis_hostname,
            port=args.redis_port,
            decode_responses=True
        ),
    )

    redis_interface.set("#PRIMARY", initial_stage_name)

    for kvstr in args.kv:
        delim_idx = kvstr.index("=")
        redis_interface.set(kvstr[0:delim_idx], kvstr[delim_idx + 1 :])

    previous_stage_list = redis_interface.get("#STAGES")
    cleanup_stability_factor = 2
    process_changed_count = 0

    while True:
        stage_list = redis_interface.get("#STAGES")
        redis_interface.set("PULSE", "%s" % (datetime.now().strftime("%Y/%m/%d %H:%M:%S")))

        redis_interface.get_broadcast_messages(0.1)
        
        process_changed = (
            stage_list is not None
            and stage_list != previous_stage_list
        )
        if process_changed:
            process_changed_count += 1

        if process_changed_count == cleanup_stability_factor:
            # clear unused redis-hash keys
            process_changed_count = 0
            previous_stage_list = stage_list

            exclusion_list = get_redis_keys_in_use(
                stage_list.split(' ')
            )
            logger.info("Clear all except:", exclusion_list)
            redis_interface.clear(exclusion_list)

        new_initial_stage_name = redis_interface.get("#PRIMARY")
        if new_initial_stage_name != initial_stage_name:
            if not import_stage(new_initial_stage_name, stagePrefix="proc", definition_dict=initial_stage_dict):
                logger.warn(f"Could not load new Primary Stage: `{new_initial_stage_name}`. Maintaining current Primary Stage: `{initial_stage_name}`.")
                redis_interface.set("#PRIMARY", initial_stage_name)
            else:
                initial_stage_dict.pop(initial_stage_name)
                initial_stage_name = new_initial_stage_name
                initial_stage = initial_stage_dict.pop(initial_stage_name)
                initial_stage.setup(
                    instance_hostname,
                    instance_id,
                )

        # Wait until the process-stage returns outputs
        try:
            proc_outputs = initial_stage.run()
        except KeyboardInterrupt:
            exit(0)

        if proc_outputs is None:
            continue
        if proc_outputs is False:
            exit(0)
        if len(proc_outputs) == 0:
            print("No captured data found for post-processing.")
            continue

        redis_cache = redis_interface.get_all()
        postproc_str = redis_cache.get("#STAGES", None)
        if postproc_str is None:
            print("#STAGES key is not found. Not post-processing.")
            continue
        if "skip" in postproc_str[0:4]:
            print("#STAGES key begins with skip, not post-processing.")
            continue

        process_identifier = Identifier(instance_hostname, instance_id, None)
        logger = logging.getLogger(str(process_identifier))
        if not logger.hasHandlers():
            ch = logging.StreamHandler()
            ch.setFormatter(LogFormatter())
            logger.addHandler(ch)
            logger.setLevel(logging.INFO)

        process(
            process_identifier,
            redis_cache,
            {initial_stage_name: initial_stage},
            {initial_stage_name: proc_outputs},
            redis_hostname = args.redis_hostname,
            redis_port = args.redis_port,
        )

