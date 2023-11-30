import os
import argparse
import time
import socket
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import sys, traceback, atexit
from datetime import datetime
from datetime import time as datetime_time
import multiprocessing as mp
from typing import List, Optional

from . import import_module, get_stage_keys, process as PypelineProcess, ProcessNote, ProcessParameters
from .identifier import ServiceIdentifier
from .redis_interface import RedisServiceInterface, ServiceStatus, ProcessStateTimestamps
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
        "--log-backup-days",
        type=int,
        default=7,
        help="The number of day-logs to keep in arrears.",
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
    parser.add_argument(
        "-v",
        "--verbosity",
        action="count",
        default=0,
        help="Increase the verbosity of the logs (0=Error, 1=Warn, 2=Info, 3=Debug)."
    )
    args = parser.parse_args()
    mp.set_start_method("fork")
    
    service_id = ServiceIdentifier(
        socket.gethostname(),
        args.instance
    )

    logger = logging.getLogger(str(service_id))
    logger_level = [
        logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG
    ][args.verbosity]
    
    if args.log_directory is not None:
        for ext_level_tuple in [
            ("log", logging.INFO),
            ("err", logging.WARNING)
        ]:
            log_ext, log_level = ext_level_tuple
            if log_level < logger_level:
                continue

            fh = TimedRotatingFileHandler(
                os.path.join(args.log_directory, f"pypeline_{service_id.hostname}_{service_id.enumeration}.{log_ext}"),
                when='midnight',
                utc=True,
                backupCount=args.log_backup_days
            )
            fh.setFormatter(LogFormatter())
            fh.setLevel(log_level)
            logger.addHandler(fh)
    else:
        ch = logging.StreamHandler()
        ch.setFormatter(LogFormatter())
        logger.addHandler(ch)

    logger.setLevel(logger_level)
    logger.warning("Start up.")

    context_dict = {}
    context_name = args.context
    assert import_module(context_name, modulePrefix="context", definition_dict=context_dict, logger=logger)
    context = context_dict.pop(context_name)

    context.setup(
        service_id.hostname,
        service_id.enumeration,
    )

    redis_interface = RedisServiceInterface(
        service_id,
        host=args.redis_hostname,
        port=args.redis_port,
    )

    redis_interface.context = context_name

    for kvstr in args.kv:
        delim_idx = kvstr.index("=")
        redis_interface.set(kvstr[0:delim_idx], kvstr[delim_idx + 1 :])

    previous_stage_list = None
    cleanup_stability_factor = 5
    process_changed_count = 0
    process_asyncobj_jobs: List[Optional[mp.pool.ApplyResult]] = [None]*args.workers
    process_state_last_timestamps = [
        ProcessStateTimestamps(
            start=None,
            finish=None,
            error=None,
        )
    ]*args.workers
    process_busy_parameters = [None]*args.workers
    status = ServiceStatus(
        workers_busy_count=0,
        workers_total_count=args.workers,
        job_queue=[]
    )

    sys.excepthook = lambda *args: logger.error("".join(traceback.format_exception(*args)))
    # this happens after exception_hook even in the event of an exception
    atexit.register(lambda: logger.warning("Exiting."))

    context_outputs = None
    with mp.Pool(processes=args.workers) as pool:
        while True:
            for process_id, process_async_obj in enumerate(process_asyncobj_jobs):
                if process_async_obj is not None and process_async_obj.ready():
                    logger.info(f"Process #{process_id} is complete.")
                    logger.info(f"\tSuccessfully: {process_async_obj.successful()}.")

                    # rehydrate the initial stage to note completion
                    try:
                        logger.info(f"\tReturning: {process_async_obj.get()}.")
                        process_state_last_timestamps[process_id].finish = time.time()

                    except BaseException as err:
                        logger.error(f"\tTraceback: {traceback.format_exc()}.")
                        process_state_last_timestamps[process_id].error = time.time()

                        process_context = _isolated_context_rehydration(
                            process_busy_parameters[process_id],
                            logger=logger
                        )
                        if hasattr(process_context, "note"):
                            process_context.note(
                                ProcessNote.Error,
                                process_id = service_id.process_identifier(process_id),
                                logger = logger,
                                error = err,
                            )

                    process_asyncobj_jobs[process_id] = None
                    process_busy_parameters[process_id] = None
                    status.workers_busy_count -= 1

                if process_asyncobj_jobs[process_id] is None and len(status.job_queue) > 0:
                    process_parameters = status.job_queue.pop(0)
                    logger.info(f"Spawning Process #{process_id}:\n\t#STAGES={process_parameters.redis_kvcache.get('#STAGES', None)}\n\t{process_parameters.dehydrated_context}")
                    process_asyncobj_jobs[process_id] = pool.apply_async(
                        PypelineProcess,
                        (
                            service_id.process_identifier(process_id),
                            process_parameters
                        )
                    )
                    process_busy_parameters[process_id] = process_parameters
                    process_state_last_timestamps[process_id].start = time.time()
                    status.workers_busy_count += 1

            redis_interface.process_broadcast_set_messages(0.1)
            stage_list = redis_interface.stages
            redis_interface.pulse = datetime.now()
            redis_interface.status = status
            redis_interface.processes = process_state_last_timestamps

            if context_outputs is False:
                if status.workers_busy_count > 0:
                    # continue to wait on processes
                    continue
                logger.info(f"{context_name}.run() returned False. Exiting")
                break

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
                exclusion_list.extend(redis_interface.REDIS_HASH_KEYS)
                logger.info(f"Clear all except: {exclusion_list}")
                redis_interface.clear(exclusion_list)

            new_context_name = redis_interface.context
            if new_context_name != context_name:
                try:
                    import_module(new_context_name, modulePrefix="context", definition_dict=context_dict, logger=logger)

                    context_name = new_context_name
                    context = context_dict.pop(context_name)
                    context.setup(
                        service_id.hostname,
                        service_id.enumeration,
                        logger=logger
                    )
                except:
                    logger.warn(f"Could not load new Context: `{new_context_name}`. Maintaining current Context: `{context_name}`.")
                    redis_interface.context = context_name

            # Wait until the process-stage returns outputs
            try:
                context_outputs = context.run(
                    env=redis_interface.context_environment,
                    logger=logger
                )
            except KeyboardInterrupt:
                logger.info("Keyboard Interrupt. Awaiting processes...")
                context_outputs = False
                continue

            if context_outputs is None:
                continue
            if context_outputs is False:
                logger.info(f"{context_name}.run() returned False. Awaiting processes: {status})")
                continue
                
            if len(context_outputs) == 0:
                logger.info("No captured data found for post-processing.")
                continue

            redis_kvcache = redis_interface.get_all()
            postproc_str = redis_kvcache.get("#STAGES", None)

            if postproc_str is None:
                logger.warn("#STAGES key is not found. Not post-processing.")
                continue
            if "skip" in postproc_str[0:4]:
                logger.info("#STAGES key begins with skip, not post-processing.")
                continue
            
            for key in redis_interface.REDIS_HASH_KEYS:
                redis_kvcache.pop(key, None)

            if len(status.job_queue) == args.queue_limit:
                context.note(
                    ProcessNote.Error,
                    process_id = None,
                    logger = logger,
                    error = RuntimeError(f"Queue limit of {args.queue_limit} reached."),
                )
                continue

            status.job_queue.append(
                ProcessParameters(
                    redis_kvcache,
                    {context_name: context_outputs},
                    postproc_str.split(" "),
                    context.dehydrate(),
                    args.redis_hostname,
                    args.redis_port,
                )
            )

