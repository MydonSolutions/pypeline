import os
import argparse
import time
import socket
import logging
from datetime import datetime
import multiprocessing as mp

import redis

from . import import_stage, get_redis_keys_in_use, process as PypelineProcess
from .identifier import Identifier
from .redis_interface import RedisInterface
from .log_formatter import LogFormatter


def main():
    """ Entrypoint for a Pypeline"""

    parser = argparse.ArgumentParser(
        description="A python-pipeline executable, with a Redis interface.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("instance", type=int, help="The instance ID of the pypeline.")
    parser.add_argument("procstage", type=str, help="The name of process stage.")
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=4,
        help="The number of parallel processes to pool.",
    )
    parser.add_argument(
        "--log-directory",
        type=str,
        default=None,
        help="The directory in which to log.",
    )
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
    process_asyncobj_jobs = {
        Identifier(instance_hostname, instance_id, process_index): None
        for process_index in range(args.workers)
    }
    process_queue = []

    logger = logging.getLogger(f"{instance_hostname}:{instance_id}")
    ch = logging.StreamHandler()
    ch.setFormatter(LogFormatter())
    logger.addHandler(ch)
    if args.log_directory is not None:
        fh = logging.FileHandler(os.path.join(args.log_directory, f"pypeline_{instance_hostname}_{instance_id}.log"))
        fh.setFormatter(LogFormatter())
        logger.addHandler(fh)
    logger.setLevel(logging.INFO)

    with mp.Pool(processes=args.workers) as pool:
    
        while True:
            for process_id, process_async_obj in process_asyncobj_jobs.items():
                if process_async_obj is not None and process_async_obj.ready():
                    logger.info(f"Process #{process_id} is complete.")
                    logger.info(f"\tSuccessfully: {process_asyncobj_jobs[process_id].successful()}.")
                    logger.info(f"\tReturning: {process_asyncobj_jobs[process_id].get()}.")
                    process_asyncobj_jobs[process_id] = None
                if process_asyncobj_jobs[process_id] is None and len(process_queue) > 0:
                    process_args = process_queue.pop(0)
                    logger.info(f"#Spawning Process #{process_id}:\n\t#STAGES={process_args[0].get('#STAGES', None)}\n\t{process_args[2]}")
                    process_asyncobj_jobs[process_id] = pool.apply_async(
                        PypelineProcess,
                        (
                            process_id,
                            *process_args
                        )
                    )
                
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
                logger.info("Keyboard Interrupt")
                exit(0)

            if proc_outputs is None:
                continue
            if proc_outputs is False:
                logger.info(f"{initial_stage_name}.run() returned False. Awaiting processes...")
                while True:
                    processing = False
                    for process_id, process_async_obj in process_asyncobj_jobs.items():
                        if process_async_obj is None:
                            continue
                        if process_async_obj.ready():
                            logger.info(f"Process #{process_id} is complete.")
                            logger.info(f"\tSuccessfully: {process_asyncobj_jobs[process_id].successful()}.")
                            logger.info(f"\tReturning: {process_asyncobj_jobs[process_id].get()}.")
                            process_asyncobj_jobs[process_id] = None
                        else:
                            processing = True
                    if not processing:
                        break
                exit(0)
            if len(proc_outputs) == 0:
                logger.info("No captured data found for post-processing.")
                continue

            redis_cache = redis_interface.get_all()
            postproc_str = redis_cache.get("#STAGES", None)
            if postproc_str is None:
                logger.warn("#STAGES key is not found. Not post-processing.")
                continue
            if "skip" in postproc_str[0:4]:
                logger.info("#STAGES key begins with skip, not post-processing.")
                continue

            process_queue.append(
                (
                    redis_cache,
                    {initial_stage_name: proc_outputs},
                    initial_stage.dehydrate(),
                    args.redis_hostname,
                    args.redis_port,
                )
            )

