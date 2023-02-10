import os
import argparse
import time
import socket
import json
import logging
import traceback
from datetime import datetime
import multiprocessing as mp

import redis

from . import REDIS_KEYS, import_module, get_stage_keys, process as PypelineProcess, ProcessNote, ProcessParameters
from .identifier import Identifier
from .redis_interface import RedisInterface
from .log_formatter import LogFormatter


def _isolated_context_rehydration(process_parameters: ProcessParameters, logger: logging.Logger):
    context_name = list(process_parameters.stage_outputs.keys())[0]
    tmp_stage_dict = {}
    import_module(context_name, modulePrefix="context", definition_dict=tmp_stage_dict, logger=logger)
    context = tmp_stage_dict[context_name]
    context.rehydrate(process_parameters.dehydrated_context)
    return context


def main():
    """ Entrypoint for a Pypeline"""

    parser = argparse.ArgumentParser(
        description="A python-pipeline executable, with a Redis interface.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("instance", type=int, help="The instance ID of the pypeline.")
    parser.add_argument("context", type=str, help="The name of the context.")
    parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=4,
        help="The number of parallel processes to pool.",
    )
    parser.add_argument(
        "--queue-limit",
        "-q",
        type=int,
        default=10,
        help="The limit of the process queue.",
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

    mp.set_start_method("fork")

    context_dict = {}
    context_name = args.context
    assert import_module(context_name, modulePrefix="context", definition_dict=context_dict)
    context = context_dict.pop(context_name)

    instance_hostname = socket.gethostname()
    instance_id = args.instance
    context.setup(
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

    redis_interface.set("#CONTEXT", context_name)

    for kvstr in args.kv:
        delim_idx = kvstr.index("=")
        redis_interface.set(kvstr[0:delim_idx], kvstr[delim_idx + 1 :])

    previous_stage_list = None
    cleanup_stability_factor = 5
    process_changed_count = 0
    process_asyncobj_jobs = {
        Identifier(instance_hostname, instance_id, process_index): None
        for process_index in range(args.workers)
    }
    process_state_last_timestamps = {
        str(process_id): {
            "Start": None,
            "Finish": None,
            "Error": None,
        }
        for process_id in process_asyncobj_jobs.keys()
    }
    process_busy_parameters = {
        str(process_id): None
        for process_id in process_asyncobj_jobs.keys()
    }
    process_occupancy = 0
    process_queue = []

    logger = logging.getLogger(f"{instance_hostname}:{instance_id}")
    ch = logging.StreamHandler()
    ch.setFormatter(LogFormatter())
    logger.addHandler(ch)
    if args.log_directory is not None:
        fh = logging.FileHandler(os.path.join(args.log_directory, f"pypeline_{instance_hostname}_{instance_id}.log"))
        fh.setFormatter(LogFormatter())
        logger.addHandler(fh)
    logger.setLevel(logging.DEBUG)

    context_outputs = None
    with mp.Pool(processes=args.workers) as pool:
    
        while True:
            for process_id, process_async_obj in process_asyncobj_jobs.items():
                if process_async_obj is not None and process_async_obj.ready():
                    logger.info(f"Process #{process_id} is complete.")
                    logger.info(f"\tSuccessfully: {process_async_obj.successful()}.")

                    # rehydrate the initial stage to note completion
                    try:
                        logger.info(f"\tReturning: {process_async_obj.get()}.")
                        process_state_last_timestamps[str(process_id)]["Finish"] = time.time()

                    except BaseException as err:
                        logger.error(f"\tTraceback: {traceback.format_exc()}.")
                        process_state_last_timestamps[str(process_id)]["Error"] = time.time()

                        process_context = _isolated_context_rehydration(
                            process_busy_parameters[process_id],
                            logger=logger
                        )
                        if hasattr(process_context, "note"):
                            process_context.note(
                                ProcessNote.Error,
                                process_id = process_id,
                                logger = logger,
                                error = err,
                            )

                    process_asyncobj_jobs[process_id] = None
                    process_busy_parameters[process_id] = None
                    process_occupancy -= 1

                if process_asyncobj_jobs[process_id] is None and len(process_queue) > 0:
                    process_parameters = process_queue.pop(0)
                    logger.info(f"Spawning Process #{process_id}:\n\t#STAGES={process_parameters.redis_kvcache.get('#STAGES', None)}\n\t{process_parameters.dehydrated_context}")
                    process_asyncobj_jobs[process_id] = pool.apply_async(
                        PypelineProcess,
                        (
                            process_id,
                            process_parameters
                        )
                    )
                    process_busy_parameters[process_id] = process_parameters
                    process_state_last_timestamps[str(process_id)]["Start"] = time.time()
                    process_occupancy += 1
                
            stage_list = redis_interface.get("#STAGES")
            redis_interface.set("PULSE", "%s" % (datetime.now().strftime("%Y/%m/%d %H:%M:%S")))
            redis_interface.set("STATUS", f"{process_occupancy}/{args.workers} ({len(process_queue)} queued)")
            redis_interface.set("PROCESSES", json.dumps(process_state_last_timestamps))

            if context_outputs is False:
                if process_occupancy > 0:
                    # continue to wait on processes
                    continue
                logger.info(f"{context_name}.run() returned False. Exiting")
                exit(0)

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

                exclusion_list = get_stage_keys(
                    stage_list.split(' ')
                )
                exclusion_list.extend(REDIS_KEYS)
                exclusion_list.extend([f"STATUS:{process_index}" for process_index in range(args.workers)])
                logger.info(f"Clear all except: {exclusion_list}")
                redis_interface.clear(exclusion_list)

            new_context_name = redis_interface.get("#CONTEXT")
            if new_context_name != context_name:
                try:
                    import_module(new_context_name, modulePrefix="context", definition_dict=context_dict)

                    context_name = new_context_name
                    context = context_dict.pop(context_name)
                    context.setup(
                        instance_hostname,
                        instance_id,
                        logger=logger
                    )
                except:
                    logger.warn(f"Could not load new Context: `{new_context_name}`. Maintaining current Context: `{context_name}`.")
                    redis_interface.set("#CONTEXT", context_name)

            # Wait until the process-stage returns outputs
            try:
                context_outputs = context.run(
                    env=redis_interface.get("#CONTEXTENV"),
                    logger=logger
                )
            except KeyboardInterrupt:
                logger.info("Keyboard Interrupt. Awaiting processes...")
                context_outputs = False
                continue

            if context_outputs is None:
                continue
            if context_outputs is False:
                logger.info(f"{context_name}.run() returned False. Awaiting processes: {process_occupancy}/{args.workers} ({len(process_queue)} queued)")
                continue
                
            if len(context_outputs) == 0:
                logger.info("No captured data found for post-processing.")
                continue

            redis_kvcache = redis_interface.get_all()
            postproc_str = redis_kvcache.get("#STAGES", None)
            for key in REDIS_KEYS + [f"STATUS:{process_index}" for process_index in range(args.workers)]:
                if key in redis_kvcache:
                    redis_kvcache.pop(key)

            if postproc_str is None:
                logger.warn("#STAGES key is not found. Not post-processing.")
                continue
            if "skip" in postproc_str[0:4]:
                logger.info("#STAGES key begins with skip, not post-processing.")
                continue

            if len(process_queue) == args.queue_limit:
                context.note(
                    ProcessNote.Error,
                    process_id = None,
                    logger = logger,
                    error = RuntimeError(f"Queue limit of {args.queue_limit} reached."),
                )
                continue

            process_queue.append(
                ProcessParameters(
                    redis_kvcache,
                    {context_name: context_outputs},
                    postproc_str.split(" "),
                    context.dehydrate(),
                    args.redis_hostname,
                    args.redis_port,
                )
            )

