import time
import importlib
import logging
import sys
import traceback
from dataclasses import dataclass

from .redis_interface import RedisProcessInterface, ProcessStatus
from .identifier import ProcessIdentifier

@dataclass
class ProcessParameters:
    redis_kvcache: dict # {key::string: value::string}
    stage_outputs: dict # {stage_name::string: output::list}
    stage_list: list
    dehydrated_context: tuple # context.dehydrate()
    redis_hostname: str
    redis_port: int

class ProcessNote:
    Start = 0
    StageStart = 1
    StageFinish = 2
    StageError = 3
    Finish = 4
    Error = 5

    @staticmethod
    def string(note):
        if note == ProcessNote.Start:
            return "Start"
        if note == ProcessNote.StageStart:
            return "Stage Start"
        if note == ProcessNote.StageFinish:
            return "Stage Finish"
        if note == ProcessNote.StageError:
            return "Stage Error"
        if note == ProcessNote.Finish:
            return "Finish"
        if note == ProcessNote.Error:
            return "Error"
        raise ValueError(f"{note} is not a recognised ProcessNote value.")


def import_module(
    stagename,
    modulePrefix="stage",
    definition_dict=globals(),
    logger=None
):
    log_info_func = print if logger is None else logger.info
    log_error_func = print if logger is None else logger.error
    if stagename not in definition_dict:  # stagename not in sys.modules:
        try:
            definition_dict[stagename] = importlib.import_module(f"{modulePrefix}_{stagename}")
        except ModuleNotFoundError as error:
            log_error_func(
                f"Could not find {modulePrefix}_{stagename}.py:\n\t" +
                    "\n\t".join(sys.path)
            )
            raise error
        except BaseException as error:
            log_error_func(
                f"Could not import {modulePrefix}_{stagename}.py:\n\t{traceback.format_exc()}"
            )
            raise error

        log_info_func(f"Imported {modulePrefix}_{stagename}.")

        return True

    try:
        definition_dict[stagename] = importlib.reload(definition_dict[stagename])
    except ModuleNotFoundError:
        log_error_func(
            f"Could not find the {modulePrefix}_{stagename}.py module to reload, keeping existing version:\n\t" +
                "\n\t".join(sys.path)
        )
        return False
    except:
        log_error_func(
            f"Could not reload the {modulePrefix}_{stagename}.py module, keeping existing version:\n\t{traceback.format_exc()}"
        )
        return False

    log_info_func(f"Reloaded {modulePrefix}_{stagename}.")
    return True


def parse_input_template(
    input_template,
    pypeline_outputs,
    pypeline_lastinput,
    logger=None
):
    log_info_func = print if logger is None else logger.info
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
                log_info_func("Stage {} produced zero outputs.".format(symbol))
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
                log_info_func('Detected verbatim input "{}"'.format(symbol))
                input_values[symbol] = [symbol[1:]]
            elif symbol[0] == "*":
                if len([symbol[1:]]) == 0:
                    log_info_func("Stage {} produced zero outputs.".format(symbol))
                    return False
                log_info_func('Detected exhaustive input "{}"'.format(symbol))
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


def replace_keywords(keyword_dict, string, keyword_opener='$', keyword_closer='$'):
    for keyword, keyvalue in keyword_dict.items():
        keyword_value = (
            " ".join(map(str, keyvalue))
            if isinstance(keyvalue, list)
            else str(keyvalue)
        )
        string = string.replace(f"{keyword_opener}{keyword}{keyword_closer}", keyword_value)
    return string


def get_proc_dict_progress_str(proc,
    inp_tmpl_dict, inp_tmplidx_dict,
    inp_dict, inpidx_dict,
    arg_dict, argidx_dict
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


def get_stage_keys(
    stage_list_in_use,
    definition_dict = None,
    logger=None
):
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

        for proc_key in [
            "ENV_KEY",
            "INP_KEY",
            "ARG_KEY"
        ]:
            if not hasattr(stage, proc_key):
                continue
            attr = getattr(stage, proc_key)
            if attr is not None:
                rediskeys_in_use.append(attr)

    return rediskeys_in_use


def process(
    identifier: ProcessIdentifier,
    parameters: ProcessParameters
):
    '''
    Params:
        identifier: Identifier,
            The identifier of the process operation (pypeline)
        parameters: ProcessParameters
            The dataclass containing all the arguments required for a process
    
        Logs with `logging.getLogger(str(identifier))`
    '''
    logger = logging.getLogger(str(identifier))
    logger.info(f"{identifier} started.")

    context_name = list(parameters.stage_outputs.keys())[0]
    stage_dict = {}
    import_module(context_name, modulePrefix="context", definition_dict=stage_dict, logger=logger)
    context = stage_dict[context_name]
    context.rehydrate(parameters.dehydrated_context)

    if hasattr(context, "note"):
        context.note(
            ProcessNote.Start,
            process_id = identifier,
            redis_kvcache = parameters.redis_kvcache,
            logger = logger,
        )

    redis_interface = RedisProcessInterface(
        identifier,
        host=parameters.redis_hostname,
        port=parameters.redis_port,
    )
    redis_interface.status = ProcessStatus(
        timestamp_last_stage=time.time(),
        last_stage="START"
    )

    keywords = {
        "hnme": identifier.hostname,
        "inst": identifier.enumeration,
        "beg": time.time(),
        "times": [],
        "stages": [],
    }

    pypeline_envvar = {}
    pypeline_input_templates = {}
    pypeline_input_templateindices = {}
    pypeline_inputs = {}
    pypeline_inputindices = {}
    pypeline_lastinput = {}
    pypeline_args = {}
    pypeline_argindices = {}
    pypeline_stage_popened = {}

    stage_index = 0

    for stage_name in parameters.stage_list:
        try:
            import_module(
                stage_name,
                definition_dict=stage_dict,
                logger=logger
            )
        except BaseException as error:
            raise RuntimeError(f"Could not load stage: {stage_name}") from error

        envkey = stage_dict[stage_name].ENV_KEY
        inpkey = stage_dict[stage_name].INP_KEY
        argkey = stage_dict[stage_name].ARG_KEY

        # Load INP, ARG and ENV key's value for the process if applicable
        ## INP
        if inpkey is not None:
            pypeline_input_templates[stage_name] = parameters.redis_kvcache[inpkey].split(';')
        else:
            pypeline_input_templates[stage_name] = [None]
        pypeline_input_templateindices[stage_name] = 0
        
        ## ARG
        if argkey is not None:
            pypeline_args[stage_name] = parameters.redis_kvcache[argkey].split(';')
        else:
            pypeline_args[stage_name] = [None]
        pypeline_argindices[stage_name] = 0

        ## ENV
        if envkey is not None:
            pypeline_envvar[stage_name] = parameters.redis_kvcache[envkey]
        else:
            pypeline_envvar[stage_name] = None
        
        pypeline_inputindices[stage_name] = 0
    
    while True:
        stage_name = parameters.stage_list[stage_index]

        # wait on any previous POPENED
        if stage_name[-1] == "&" and stage_name in pypeline_stage_popened:
            redis_interface.status = ProcessStatus(
                timestamp_last_stage=time.time(),
                last_stage=f"Poll detached {stage_name}"
            )
            for popenIdx, popen in enumerate(pypeline_stage_popened[stage_name]):
                poll_count = 0
                while popen.poll() is None:
                    if poll_count == 0:
                        logger.info(
                            "Polling previous %s stage's detached process for completion (#%d)"
                            % (stage_name, popenIdx)
                        )
                    poll_count = (poll_count + 1) % 10
                    time.sleep(1)
            logger.info(
                "%s's %d process(es) are complete."
                % (stage_name, len(pypeline_stage_popened[stage_name]))
            )

        context.setupstage(stage_dict[stage_name], logger=logger)

        if pypeline_inputindices[stage_name] == 0:
            # Parse the input_template and create the input list from permutations
            proc_input_template = pypeline_input_templates[stage_name][
                pypeline_input_templateindices[stage_name]
            ]
            pypeline_inputs[stage_name] = parse_input_template(
                proc_input_template, parameters.stage_outputs, pypeline_lastinput,
                logger = logger
            )
            logger.debug(f"{stage_name} inputs: {pypeline_inputs[stage_name]}")
            assert pypeline_inputs[stage_name]


        redis_interface.status = ProcessStatus(
            timestamp_last_stage=time.time(),
            last_stage=stage_name
        )

        inp = pypeline_inputs[stage_name][pypeline_inputindices[stage_name]]
        pypeline_lastinput[stage_name] = inp

        arg = pypeline_args[stage_name][pypeline_argindices[stage_name]]
        if arg is not None:
            arg = replace_keywords(keywords, arg)
        logger.debug(f"{stage_name} arg: {arg}")

        env = pypeline_envvar[stage_name]
        if env is not None:
            env = replace_keywords(keywords, env)

        # Run the process        
        if hasattr(context, "note"):
            context.note(
                ProcessNote.StageStart,
                process_id = identifier,
                redis_kvcache = parameters.redis_kvcache,
                logger = logger,
                stage = stage_dict[stage_name],
                argvalue = arg,
                inpvalue = inp,
                envvalue = env,
            )

        stage_logger = logging.getLogger(f"{identifier}.{stage_name}")

        checkpoint_time = time.time()
        try:
            parameters.stage_outputs[stage_name] = stage_dict[stage_name].run(arg, inp, env, logger=stage_logger)
        except BaseException as err:
            if hasattr(context, "note"):
                context.note(
                    ProcessNote.StageError,
                    process_id = identifier,
                    redis_kvcache = parameters.redis_kvcache,
                    logger = logger,
                    stage = stage_dict[stage_name],
                    error = err
                )

            message = f"{repr(err)} ({stage_name})"
            logger.error(message)
            redis_interface.status = ProcessStatus(
                timestamp_last_stage=time.time(),
                last_stage=f"ERROR: {message}"
            )
            raise RuntimeError(message) from err

        keywords["times"].append(time.time() - checkpoint_time)
        keywords["stages"].append(stage_name)

        if stage_name[-1] == "&":
            pypeline_stage_popened[stage_name] = stage_dict[stage_name].POPENED
            logger.info(
                "Captured %s's %d detached processes."
                % (stage_name, len(stage_dict[stage_name].POPENED))
            )

        if hasattr(context, "note"):
            context.note(
                ProcessNote.StageFinish,
                process_id = identifier,
                redis_kvcache = parameters.redis_kvcache,
                logger = logger,
                stage = stage_dict[stage_name],
                output = parameters.stage_outputs[stage_name]
            )

        # Increment through inputs, overflow increment through arguments
        pypeline_inputindices[stage_name] += 1
        if pypeline_inputindices[stage_name] >= len(pypeline_inputs[stage_name]):
            pypeline_inputindices[stage_name] = 0
            pypeline_input_templateindices[stage_name] += 1
            if pypeline_input_templateindices[stage_name] >= len(
                pypeline_input_templates[stage_name]
            ):
                pypeline_input_templateindices[stage_name] = 0
                pypeline_argindices[stage_name] += 1

        # Proceed to next process or...
        if stage_index + 1 < len(parameters.stage_list):
            logger.info("Next process")

            stage_index += 1
            stage_name = parameters.stage_list[stage_index]
            if stage_name[0] == "*":
                stage_name = stage_name[1:]
            pypeline_input_templateindices[stage_name] = 0
            pypeline_inputindices[stage_name] = 0
            pypeline_argindices[stage_name] = 0
        else:  # ... rewind to the closest next novel process (argumentindices indicate exhausted permutations)
            logger.info(f"Rewinding after {stage_name}")
            while stage_index >= 0 and pypeline_argindices[stage_name] >= len(
                pypeline_args[stage_name]
            ):
                progress_str = get_proc_dict_progress_str(
                    stage_name,
                    pypeline_input_templates,
                    pypeline_input_templateindices,
                    pypeline_inputs,
                    pypeline_inputindices,
                    pypeline_args,
                    pypeline_argindices,
                )
                logger.info(progress_str)

                stage_index -= 1
                stage_name = parameters.stage_list[stage_index]
                if stage_name[0] == "*":
                    stage_name = stage_name[1:]

            # Break if there are no novel process argument-input permutations
            if stage_index < 0:
                logger.info("Post Processing Done!")
                redis_interface.status = ProcessStatus(
                    timestamp_last_stage=time.time(),
                    last_stage="DONE"
                )
                break
            progress_str = get_proc_dict_progress_str(
                stage_name,
                pypeline_input_templates,
                pypeline_input_templateindices,
                pypeline_inputs,
                pypeline_inputindices,
                pypeline_args,
                pypeline_argindices,
            )
            logger.info(progress_str)

            logger.info(f"Rewound to {parameters.stage_list[stage_index]}")

    if hasattr(context, "note"):
        context.note(
            ProcessNote.Finish,
            process_id = identifier,
            redis_kvcache = parameters.redis_kvcache,
            logger = logger,
        )