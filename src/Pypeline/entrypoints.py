import os
import argparse
import socket
import logging
from logging.handlers import TimedRotatingFileHandler
import sys, traceback, atexit
from datetime import datetime
import multiprocessing as mp
from typing import List, Optional

from . import import_module, get_stage_keys, process as PypelineProcess
from .redis_interface import RedisServiceInterface
from .dataclasses import ServiceIdentifier, ServiceStatus, ProcessState, JobEvent, JobEventMessage, ProcessNote, JobParameters
from .log_formatter import LogFormatter


def main_cli():
    """ CLI Entrypoint for a Pypeline"""

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
    parser.add_argument(
        "--multiprocessing-start-method",
        choices=["spawn", "fork"],
        default="fork",
        help="Set the process start method."
    )
    args = parser.parse_args()

    mp.set_start_method(args.multiprocessing_start_method)
    main(
        args.instance,
        args.context,
        kv = args.kv,
        redis_hostname = args.redis_hostname,
        redis_port = args.redis_port,
        workers = args.workers,
        queue_limit = args.queue_limit,
        verbosity = args.verbosity,
        log_directory = args.log_directory,
        log_backup_days = args.log_backup_days,
    )

def main(
    instance: int,
    context: str,
    kv: List[str],
    multiprocessing_start_method: str = "fork",
    redis_hostname: str = "redishost",
    redis_port: int = 6379,
    workers: int = 4,
    queue_limit: int = 10,
    verbosity: int = 0,
    log_directory: Optional[str] = None,
    log_backup_days: int = 7,
):
    """ Entrypoint for a Pypeline"""
    service_id = ServiceIdentifier(
        socket.gethostname(),
        instance
    )

    logger = logging.getLogger(str(service_id))
    logger_level = [
        logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG
    ][verbosity]
    
    if log_directory is not None:
        for ext_level_tuple in [
            ("log", min(logging.INFO, logger_level)), # INFO and finer
            ("err", max(logging.WARNING, logger_level)) # WARNING and coarser
        ]:
            log_ext, log_level = ext_level_tuple
            if log_level < logger_level:
                continue

            fh = TimedRotatingFileHandler(
                os.path.join(log_directory, f"pypeline_{service_id.hostname}_{service_id.enumeration}.{log_ext}"),
                when='midnight',
                utc=True,
                backupCount=log_backup_days
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
    pool = mp.Pool(processes=workers)

    context_dict = {}
    context_name = context
    assert import_module(context_name, modulePrefix="context", definition_dict=context_dict, logger=logger)
    context = context_dict.pop(context_name)

    context.setup(
        service_id.hostname,
        service_id.enumeration,
    )

    redis_interface = RedisServiceInterface(
        service_id,
        host=redis_hostname,
        port=redis_port,
    )

    redis_interface.context = context_name

    for kvstr in kv:
        delim_idx = kvstr.index("=")
        redis_interface.set(kvstr[0:delim_idx], kvstr[delim_idx + 1 :])

    previous_stage_list = None
    cleanup_stability_factor = 5
    process_changed_count = 0
    process_asyncobj_jobs: List[Optional[mp.pool.ApplyResult]] = [None]*workers
    process_states = [ProcessState.Idle]*workers
    status = ServiceStatus(
        workers_busy_count=0,
        workers_total_count=workers,
        process_job_queue=[]
    )
    job_id = 1

    sys.excepthook = lambda *args: logger.error("".join(traceback.format_exception(*args)))
    # this happens after exception_hook even in the event of an exception
    atexit.register(lambda: logger.warning("Exiting."))

    context_outputs = None
    with pool:
        while True:
            for process_id in range(len(process_asyncobj_jobs)):
                process_async_obj = process_asyncobj_jobs[process_id]
                if process_async_obj is not None and process_async_obj.ready():
                    successful = process_async_obj.get() # process is safely wrapped
                    logger.info(f"Process #{process_id} has {'completed' if successful else 'failed'}.")

                    process_states[process_id] = ProcessState.Finished
                    if not successful:
                        process_states[process_id] = ProcessState.Errored

                    process_asyncobj_jobs[process_id] = None
                    status.workers_busy_count -= 1

                if process_asyncobj_jobs[process_id] is None and len(status.process_job_queue) > 0:
                    job_parameters = status.process_job_queue.pop(0)
                    logger.info(f"Spawning Process #{process_id}")
                    process_asyncobj_jobs[process_id] = pool.apply_async(
                        PypelineProcess,
                        (
                            service_id.process_identifier(process_id),
                            job_parameters,
                            redis_hostname,
                            redis_port
                        )
                    )
                    process_states[process_id] = ProcessState.Busy
                    status.workers_busy_count += 1

            redis_interface.process_broadcast_set_messages(0.1)
            stage_list = redis_interface.stages
            redis_interface.pulse = datetime.now()
            redis_interface.status = status
            redis_interface.processes = process_states

            if context_outputs is False:
                if status.workers_busy_count > 0:
                    # continue to wait on processes
                    continue
                logger.warning(f"{context_name}.run() returned False. Exiting")
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
                    stage_list,
                    logger=logger
                )
                exclusion_list.extend(redis_interface.REDIS_HASH_KEYS)
                logger.debug(f"Clear all except: {exclusion_list}")
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
                    logger.warning(f"Could not load new Context: `{new_context_name}`. Maintaining current Context: `{context_name}`.")
                    redis_interface.context = context_name

            # Wait until the process-stage returns outputs
            context_environment = redis_interface.context_environment
            try:
                context_outputs = context.run(
                    env=context_environment,
                    logger=logger
                )
            except KeyboardInterrupt:
                logger.info("Keyboard Interrupt. Awaiting processes...")
                context_outputs = False
                continue

            if context_outputs is None:
                # logger.debug(f"{context_name}.run() returned None.")
                continue
            if context_outputs is False:
                logger.warning(f"{context_name}.run() returned False. Awaiting processes: {status})")
                continue

            redis_kvcache = redis_interface.get_all()
            stages_keyvalue = redis_kvcache.get("#STAGES", None)
            
            for key in redis_interface.REDIS_HASH_KEYS:
                redis_kvcache.pop(key, None)

            params = JobParameters(
                job_id=job_id,
                redis_kvcache=redis_kvcache,
                context_name=context_name,
                context_output=context_outputs,
                context_dehydrated=context.dehydrate(),
                stage_list=stages_keyvalue.split(" ") if stages_keyvalue is not None else []
            )
            job_id += 1
            event = JobEvent.Queue

            if stages_keyvalue is None or "skip" in stages_keyvalue[0:4]:
                logger.info(f"#STAGES key begins with 'skip' or is missing. Not processing. ('{stages_keyvalue}')")
                event=JobEvent.Skip

            elif len(status.process_job_queue) == queue_limit:
                message = f"Queue limit of {queue_limit} reached."
                logger.warning(message)

                event=JobEvent.Drop
                if hasattr(context, "note"):
                    context.note( # TODO change to service note
                        ProcessNote.Error,
                        process_id = None,
                        logger = logger,
                        error = RuntimeError(message),
                    )

            job_event_message = JobEventMessage(
                event=event,
                job_parameters=params,
                context_environment=context_environment
            )
            logger.debug(f"job_event_message: {job_event_message}")
            redis_interface.job_event_message = job_event_message
            if event != JobEvent.Queue:
                continue

            status.process_job_queue.append(params)
    
    atexit.unregister(lambda: logger.warning("Exiting."))
    pool.close()
    logger.warning("Finished.")
    pool.join()
    if hasattr(context, "reset"):
        context.reset()
    logger.handlers.clear()
