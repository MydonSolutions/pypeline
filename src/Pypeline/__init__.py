import time
import importlib
import logging
import sys
import traceback

from .redis_interface import RedisServiceInterface
from .dataclasses import (
    JobProgress,
    ProcessIdentifier,
    ServiceIdentifier,
    JobParameters,
    ProcessNote,
    ProcessStatus,
    ProcessNoteMessage,
    StageTimestamp,
)


class StageException(Exception):
    """Exception raised for errors in that occur during a stage.

    Attributes:
        stage_name
        exception
    """

    def __init__(self, stage_name, exception):
        self.stage_name = stage_name
        self.exception = exception
        super().__init__(
            f"`{self.stage_name}` failed, due to error: {repr(self.exception)}"
        )


def import_module(
    stagename, modulePrefix="stage", definition_dict=globals(), logger=None
):
    log_debug_func = print if logger is None else logger.debug
    log_error_func = print if logger is None else logger.error
    if stagename not in definition_dict:  # stagename not in sys.modules:
        try:
            definition_dict[stagename] = importlib.import_module(
                f"{modulePrefix}_{stagename}"
            )
        except ModuleNotFoundError as error:
            log_error_func(
                f"Could not find {modulePrefix}_{stagename}.py:\n\t"
                + "\n\t".join(sys.path)
            )
            raise error
        except BaseException as error:
            log_error_func(
                f"Could not import {modulePrefix}_{stagename}.py:\n\t{traceback.format_exc()}"
            )
            raise error

        log_debug_func(f"Imported {modulePrefix}_{stagename}.")

        return True

    try:
        definition_dict[stagename] = importlib.reload(definition_dict[stagename])
    except ModuleNotFoundError:
        log_error_func(
            f"Could not find the {modulePrefix}_{stagename}.py module to reload, keeping existing version:\n\t"
            + "\n\t".join(sys.path)
        )
        return False
    except:
        log_error_func(
            f"Could not reload the {modulePrefix}_{stagename}.py module, keeping existing version:\n\t{traceback.format_exc()}"
        )
        return False

    log_debug_func(f"Reloaded {modulePrefix}_{stagename}.")
    return True


def parse_input_template(
    input_template, pypeline_outputs, pypeline_lastinput, logger=None
):
    log_debug_func = print if logger is None else logger.debug
    log_error_func = print if logger is None else logger.error

    if input_template is None:
        return [[]]
    ret = []
    input_values = {}
    value_indices = {}

    # Gather values for each
    input_template_symbols = input_template.split(" ")
    for symbol in input_template_symbols:
        # TODO flatten this out (extract flagchar and process uniformly)
        if symbol in pypeline_outputs:
            if len(pypeline_outputs[symbol]) == 0:
                log_debug_func("Stage {} produced zero outputs.".format(symbol))
                return False
            input_values[symbol] = pypeline_outputs[symbol]
        elif symbol[0] == "&" or (
            len(symbol) > 1
            and symbol[0] in ["^", "*"]
            and symbol[1:] in pypeline_outputs
        ):
            if symbol[0] == "^":
                input_values[symbol] = pypeline_lastinput[symbol[1:]]
            elif symbol[0] == "&":
                log_debug_func('Detected verbatim input "{}"'.format(symbol))
                input_values[symbol] = [symbol[1:]]
            elif symbol[0] == "*":
                if len([symbol[1:]]) == 0:
                    log_debug_func("Stage {} produced zero outputs.".format(symbol))
                    return False
                log_debug_func('Detected exhaustive input "{}"'.format(symbol))
                input_values[symbol] = [
                    pypeline_outputs[symbol[1:]]
                ]  # wrap in list to treat as single value
        else:
            log_error_func(
                "No replacement for {} within input template (that stage has not been run yet).".format(
                    symbol
                )
            )
            return False

        value_indices[symbol] = 0

    input_template_symbols_rev = input_template_symbols[::-1]

    fully_permutated = False

    while not fully_permutated:
        inp = []
        for symbol in input_template_symbols:
            if symbol[0] == "*":
                inp.extend(input_values[symbol][value_indices[symbol]])
            else:
                inp.append(input_values[symbol][value_indices[symbol]])
        ret.append(inp)

        for i, symbol in enumerate(input_template_symbols_rev):
            value_indices[symbol] += 1
            if value_indices[symbol] == len(input_values[symbol]):
                value_indices[symbol] = 0
                if i + 1 == len(input_template_symbols_rev):
                    fully_permutated = True
            else:
                break

    return ret


def replace_keywords(keyword_dict, string, keyword_opener="$", keyword_closer="$"):
    for keyword, keyvalue in keyword_dict.items():
        keyword_value = (
            " ".join(map(str, keyvalue))
            if isinstance(keyvalue, list)
            else str(keyvalue)
        )
        string = string.replace(
            f"{keyword_opener}{keyword}{keyword_closer}", keyword_value
        )
    return string


def get_proc_dict_progress_str(
    proc, inp_tmpl_dict, inp_tmplidx_dict, inp_dict, inpidx_dict, arg_dict, argidx_dict
):
    return "{}: input_templateindex {}/{}, inputindex {}/{}, argindex {}/{}".format(
        proc,
        inp_tmplidx_dict[proc] + 1,
        len(inp_tmpl_dict[proc]),
        inpidx_dict[proc] + 1,
        len(inp_dict[proc]),
        argidx_dict[proc],
        len(arg_dict[proc]),
    )


def get_stage_keys(stage_list_in_use, definition_dict=None, logger=None):
    if definition_dict is None:
        definition_dict = {}

    # clear unused redis-hash keys
    rediskeys_in_use = []
    for stage_name in stage_list_in_use:

        if stage_name == "skip":
            break
        if stage_name not in definition_dict:
            import_module(stage_name, definition_dict=definition_dict, logger=logger)
        stage = definition_dict[stage_name]

        for proc_key in ["ENV_KEY", "INP_KEY", "ARG_KEY"]:
            if not hasattr(stage, proc_key):
                continue
            attr = getattr(stage, proc_key)
            if attr is not None:
                rediskeys_in_use.append(attr)

    return rediskeys_in_use


def process(
    identifier: ProcessIdentifier,
    job_parameters: JobParameters,
    redis_hostname: str,
    redis_port: int,
):
    """
    This function wraps `process_unsafe` in a try-except block, handling any exceptions.

    Params:
        identifier: Identifier,
            The identifier of the process operation (pypeline)
        job_parameters: JobParameters
            The dataclass containing all the arguments required for a process
        redis_hostname: str
        redis_port: int

        Logs with `logging.getLogger(str(identifier))`

    Returns:
        bool
            Whether an exception was raised or not.
    """
    logger = logging.getLogger(str(identifier))

    redis_interface = RedisServiceInterface(
        ServiceIdentifier(identifier.hostname, identifier.enumeration),
        host=redis_hostname,
        port=redis_port,
    )

    stage_dict = {}
    import_module(
        job_parameters.context_name,
        modulePrefix="context",
        definition_dict=stage_dict,
        logger=logger,
    )
    job_progress = JobProgress(job_parameters)
    while True:
        try:
            process_unsafe(
                identifier,
                job_parameters,
                redis_interface,
                logger,
                stage_dict,
                job_progress=job_progress,
            )
            return True
        except StageException as err:
            process_context = stage_dict[job_parameters.context_name]
            if hasattr(process_context, "exceptstage"):
                if process_context.exceptstage(
                    err.exception,
                    logger=logger,
                    stage_name=err.stage_name,
                ):
                    continue

            logger.error(f"StageException: {err}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
            redis_interface.process_note_message = ProcessNoteMessage(
                job_id=job_parameters.job_id,
                process_note=ProcessNote.StageError,
                process_id=identifier.process_enumeration,
                stage_name=err.stage_name,
                error_message=f"{err.exception}",
            )
            break

        except BaseException as err:
            logger.error(f"{err}")
            logger.debug(f"Traceback: {traceback.format_exc()}")

            process_context = stage_dict[job_parameters.context_name]
            if hasattr(process_context, "note"):
                process_context.note(
                    ProcessNote.Error,
                    process_id=identifier,
                    logger=logger,
                    error=err,
                )

            redis_interface.process_note_message = ProcessNoteMessage(
                job_id=job_parameters.job_id,
                process_note=ProcessNote.Error,
                process_id=identifier.process_enumeration,
                stage_name=None,
                error_message=traceback.format_exc(),
            )
            break
    return False


def process_unsafe(
    identifier: ProcessIdentifier,
    job_parameters: JobParameters,
    redis_interface: RedisServiceInterface,
    logger: logging.Logger,
    stage_dict: dict,
    job_progress: JobProgress = None,
):
    """
    This function raises errors as necessary

    Params:
        identifier: Identifier,
            The identifier of the process operation (pypeline)
        job_parameters: JobParameters
            The dataclass containing all the arguments required for a process
        redis_interface: RedisProcessInterface
        logger: logging.Logger
        stage_dict:
            The dictionary of modules, holding only the context stage.
    """
    logger.debug(f"{identifier} starting: {job_parameters}")
    status = ProcessStatus(
        job_id=job_parameters.job_id,
        process_id=identifier.process_enumeration,
        stage_timestamps=[],
    )

    redis_interface.process_note_message = ProcessNoteMessage(
        job_id=job_parameters.job_id,
        process_note=ProcessNote.Start,
        process_id=identifier.process_enumeration,
        stage_name=None,
        error_message=None,
    )
    redis_interface.process_status = status

    context = stage_dict[job_parameters.context_name]
    context.rehydrate(job_parameters.context_dehydrated)

    if hasattr(context, "note"):
        context.note(
            ProcessNote.Start,
            process_id=identifier,
            redis_kvcache=job_parameters.redis_kvcache,
            logger=logger,
        )

    keywords = {
        "hnme": identifier.hostname,
        "inst": identifier.enumeration,
        "beg": time.time(),
        "times": [],
        "stages": [],
    }

    if job_progress is None:
        job_progress = JobProgress(job_parameters)

    if not job_progress.imported_modules:
        for stage_name in job_parameters.stage_list:
            try:
                import_module(stage_name, definition_dict=stage_dict, logger=logger)
            except BaseException as error:
                raise RuntimeError(f"Could not load stage: {stage_name}") from error

            envkey = stage_dict[stage_name].ENV_KEY
            inpkey = stage_dict[stage_name].INP_KEY
            argkey = stage_dict[stage_name].ARG_KEY

            # Load INP, ARG and ENV key's value for the process if applicable
            ## INP
            if inpkey is not None:
                job_progress.stage_map_input_templates[stage_name] = (
                    job_parameters.redis_kvcache[inpkey].split(";")
                )
            else:
                job_progress.stage_map_input_templates[stage_name] = [None]
            job_progress.stage_map_input_templateindices[stage_name] = 0

            ## ARG
            if argkey is not None:
                job_progress.stage_map_args[stage_name] = job_parameters.redis_kvcache[
                    argkey
                ].split(";")
            else:
                job_progress.stage_map_args[stage_name] = [None]
            job_progress.stage_map_argindices[stage_name] = 0

            ## ENV
            if envkey is not None:
                job_progress.stage_map_envvar[stage_name] = (
                    job_parameters.redis_kvcache[envkey]
                )
            else:
                job_progress.stage_map_envvar[stage_name] = None

            job_progress.stage_map_inputindices[stage_name] = 0
    job_progress.imported_modules = True

    while True:
        stage_name = job_progress.stage_name()

        # wait on any previous POPENED
        # TODO consider removing detached stage capabilities...
        if stage_name[-1] == "&" and stage_name in job_progress.status_map_popened:
            redis_interface.process_status = status
            for popenIdx, popen in enumerate(
                job_progress.status_map_popened[stage_name]
            ):
                poll_count = 0
                while popen.poll() is None:
                    if poll_count == 0:
                        logger.debug(
                            "Polling previous %s stage's detached process for completion (#%d)"
                            % (stage_name, popenIdx)
                        )
                    poll_count = (poll_count + 1) % 10
                    time.sleep(1)
            logger.debug(
                "%s's %d process(es) are complete."
                % (stage_name, len(job_progress.status_map_popened[stage_name]))
            )

        context.setupstage(
            stage_dict[stage_name], logger=logger
        )  # TODO let the context do this in the note function

        if job_progress.stage_map_inputindices[stage_name] == 0:
            # Parse the input_template and create the input list from permutations
            proc_input_template = job_progress.stage_map_input_templates[stage_name][
                job_progress.stage_map_input_templateindices[stage_name]
            ]
            job_progress.stage_map_inputs[stage_name] = parse_input_template(
                proc_input_template,
                job_progress.stage_map_outputs,
                job_progress.stage_map_lastinput,
                logger=logger,
            )
            logger.debug(
                f"{stage_name} inputs: {job_progress.stage_map_inputs[stage_name]}"
            )
            assert job_progress.stage_map_inputs[stage_name]

        inp = job_progress.stage_map_inputs[stage_name][
            job_progress.stage_map_inputindices[stage_name]
        ]
        job_progress.stage_map_lastinput[stage_name] = inp

        arg = job_progress.stage_map_args[stage_name][
            job_progress.stage_map_argindices[stage_name]
        ]
        if arg is not None:
            arg = replace_keywords(keywords, arg)
        logger.debug(f"{stage_name} arg: {arg}")

        env = job_progress.stage_map_envvar[stage_name]
        if env is not None:
            env = replace_keywords(keywords, env)

        # Run the process
        if hasattr(context, "note"):
            context.note(
                ProcessNote.StageStart,
                process_id=identifier,
                redis_kvcache=job_parameters.redis_kvcache,
                logger=logger,
                stage=stage_dict[stage_name],
                argvalue=arg,
                inpvalue=inp,
                envvalue=env,
            )
        redis_interface.process_note_message = ProcessNoteMessage(
            job_id=job_parameters.job_id,
            process_note=ProcessNote.StageStart,
            process_id=identifier.process_enumeration,
            stage_name=stage_name,
            error_message=None,
        )
        status.stage_timestamps.append(
            StageTimestamp(name=stage_name, start=time.time(), end=None)
        )
        redis_interface.process_status = status

        stage_logger = logging.getLogger(f"{identifier}.{stage_name}")

        checkpoint_time = time.time()
        try:
            job_progress.stage_map_outputs[stage_name] = stage_dict[stage_name].run(
                arg, inp, env, logger=stage_logger
            )
        except BaseException as err:
            if hasattr(context, "note"):
                context.note(
                    ProcessNote.StageError,
                    process_id=identifier,
                    redis_kvcache=job_parameters.redis_kvcache,
                    logger=logger,
                    stage=stage_dict[stage_name],
                    error=err,
                )
            raise StageException(
                stage_name, err
            )  # TODO ensure this is the neatest error stack possible

        keywords["times"].append(time.time() - checkpoint_time)
        keywords["stages"].append(stage_name)
        status.stage_timestamps[-1].end = time.time()
        redis_interface.process_status = status

        if stage_name[-1] == "&":
            job_progress.status_map_popened[stage_name] = stage_dict[stage_name].POPENED
            logger.info(
                "Captured %s's %d detached processes."
                % (stage_name, len(stage_dict[stage_name].POPENED))
            )

        if hasattr(context, "note"):
            context.note(
                ProcessNote.StageFinish,
                process_id=identifier,
                redis_kvcache=job_parameters.redis_kvcache,
                logger=logger,
                stage=stage_dict[stage_name],
                output=job_progress.stage_map_outputs[stage_name],
            )
        redis_interface.process_note_message = ProcessNoteMessage(
            job_id=job_parameters.job_id,
            process_note=ProcessNote.StageFinish,
            process_id=identifier.process_enumeration,
            stage_name=stage_name,
            error_message=None,
        )
        redis_interface.process_status = status

        try:
            job_progress.increment(logger)
        except StopIteration:
            logger.debug("Processing Done!")
            redis_interface.process_status = status
            break

        progress_str = job_progress.get_progress_str()
        logger.debug(progress_str)

        logger.debug(f"Rewound to {job_progress.stage_name()}")

    if hasattr(context, "note"):
        context.note(
            ProcessNote.Finish,
            process_id=identifier,
            redis_kvcache=job_parameters.redis_kvcache,
            logger=logger,
        )

    redis_interface.process_note_message = ProcessNoteMessage(
        job_id=job_parameters.job_id,
        process_note=ProcessNote.Finish,
        process_id=identifier.process_enumeration,
        stage_name=stage_name,
        error_message=None,
    )
