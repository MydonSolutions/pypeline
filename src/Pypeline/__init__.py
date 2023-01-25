import time
import importlib
import logging
import os, sys
import redis
import traceback

from .redis_interface import RedisInterface
from .identifier import Identifier
from .log_formatter import LogFormatter

def import_stage(
    stagename,
    stagePrefix="postproc",
    definition_dict=globals(),
    logger=None
):
    log_info_func = print if logger is None else logger.info
    log_error_func = print if logger is None else logger.error
    if stagename not in definition_dict:  # stagename not in sys.modules:
        try:
            definition_dict[stagename] = importlib.import_module(f"{stagePrefix}_{stagename}")
        except ModuleNotFoundError:
            log_error_func(
                "Could not find the {}.py stage:\n\t{}".format(
                    f"{stagePrefix}_{stagename}",
                    "\n\t".join(sys.path)
                )
            )
            return False
        except:
            log_error_func(
                "Could not import the {}.py stage:\n\t{}".format(
                    f"{stagePrefix}_{stagename}",
                    traceback.format_exc()
                )
            )
            return False

        log_info_func("Imported the {} stage!".format(stagename))

        return True

    try:
        definition_dict[stagename] = importlib.reload(definition_dict[stagename])
    except ModuleNotFoundError:
        log_error_func(
            "Could not find the {}.py stage to reload, keeping existing version:\n\t{}".format(
                f"{stagePrefix}_{stagename}",
                "\n\t".join(sys.path)
            )
        )
        return True
    except:
        log_error_func(
            "Could not reload the {}.py stage, keeping existing version:\n\t{}".format(
                f"{stagePrefix}_{stagename}",
                traceback.format_exc()
            )
        )
        return True

    log_info_func("Reloaded the {} stage!".format(stagename))
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


def replace_keywords(keyword_dict, string):
    for keyword, keyvalue in keyword_dict.items():
        keyword_value = (
            " ".join(map(str, keyvalue))
            if isinstance(keyvalue, list)
            else str(keyvalue)
        )
        string = string.replace(f"${keyword}$", keyword_value)
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


def get_redis_keys_in_use(
    stage_list_in_use,
    definition_dict = None
):
    if definition_dict is None:
        definition_dict = {}

    # clear unused redis-hash keys
    rediskeys_in_use = ["#PRIMARY", "#STAGES", "STATUS", "PULSE"]
    for stage_name in stage_list_in_use:

        if stage_name == "skip":
            break
        if stage_name not in definition_dict:
            if not import_stage(stage_name, definition_dict=definition_dict):
                raise RuntimeError(f"Could not import {stage_name}")
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
    identifier: Identifier,
    keyvalues: dict,
    stage_output_dict: dict,
    initial_stage_dehydrated: tuple,
    redis_hostname: str,
    redis_port: int,
):
    '''
    Params:
        identifier: Identifier,
            The identifier of the process operation (pypeline)
        keyvalues: dict
            Holds '#STAGES' and all the keys appropriate to each stage listed in '#STAGES'
        stage_output_dict: dict
            A dictionary holding only the output of the initial-stage
    
        Logs with `logging.getLogger(str(identifier))`
    '''
    logger = logging.getLogger(str(identifier))
    logger.info(f"{identifier} started.")

    initialstage_name = list(stage_output_dict.keys())[0]
    stage_dict = {}
    import_stage(initialstage_name, stagePrefix="proc", definition_dict=stage_dict, logger=logger)
    stage_dict[initialstage_name].rehydrate(initial_stage_dehydrated)

    redis_interface = RedisInterface(
        identifier.hostname,
        identifier.enumeration,
        redis.Redis(
            host=redis_hostname,
            port=redis_port,
            decode_responses=True
        ),
        sub_instance_id = identifier.process_enumeration
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

    stage_names = keyvalues["#STAGES"].split(' ')
    for stage_name in stage_names:
        assert import_stage(
            stage_name,
            definition_dict=stage_dict,
            logger=logger
        )

        envkey = stage_dict[stage_name].ENV_KEY
        inpkey = stage_dict[stage_name].INP_KEY
        argkey = stage_dict[stage_name].ARG_KEY

        # Load INP, ARG and ENV key's value for the process if applicable
        ## INP
        if inpkey is not None:
            pypeline_input_templates[stage_name] = keyvalues[inpkey].split(',')
        else:
            pypeline_input_templates[stage_name] = [None]
        pypeline_input_templateindices[stage_name] = 0
        
        ## ARG
        if argkey is not None:
            pypeline_args[stage_name] = keyvalues[argkey].split(',')
        else:
            pypeline_args[stage_name] = [None]
        pypeline_argindices[stage_name] = 0

        ## ENV
        if envkey is not None:
            pypeline_envvar[stage_name] = keyvalues[envkey]
        else:
            pypeline_envvar[stage_name] = None
        
        pypeline_inputindices[stage_name] = 0
    
    while True:
        stage_name = stage_names[stage_index]

        # wait on any previous POPENED
        if stage_name[-1] == "&" and stage_name in pypeline_stage_popened:
            redis_interface.set_status(f"POLL LAST DETACHED {stage_name}")
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

        stage_dict[initialstage_name].setupstage(stage_dict[stage_name])

        if pypeline_inputindices[stage_name] == 0:
            # Parse the input_template and create the input list from permutations
            proc_input_template = pypeline_input_templates[stage_name][
                pypeline_input_templateindices[stage_name]
            ]
            pypeline_inputs[stage_name] = parse_input_template(
                proc_input_template, stage_output_dict, pypeline_lastinput,
                logger = logger
            )
            logger.debug(f"{stage_name} inputs:{ pypeline_inputs[stage_name]}")
            assert pypeline_inputs[stage_name]

        redis_interface.set_status(stage_name)

        inp = pypeline_inputs[stage_name][pypeline_inputindices[stage_name]]
        assert inp
        pypeline_lastinput[stage_name] = inp

        arg = pypeline_args[stage_name][pypeline_argindices[stage_name]]
        if arg is not None:
            arg = replace_keywords(keywords, arg)

        env = pypeline_envvar[stage_name]
        if env is not None:
            env = replace_keywords(keywords, env)

        # Run the process
        logger.info(
            "----------------- {:^12s} -----------------".format(
                stage_name
            )
        )
        checkpoint_time = time.time()

        stage_logger = logging.getLogger(f"{identifier}.{stage_name}")
        try:
            stage_output_dict[stage_name] = stage_dict[stage_name].run(arg, inp, env, logger=stage_logger)
        except BaseException as err:
            logger.error(f"{stage_name}: {repr(err)}")
            redis_interface.set_status(f"ERROR: {stage_name}-{repr(err)}")
            raise err

        logger.info(
            "^^^^^^^^^^^^^^^^^ {:^12s} ^^^^^^^^^^^^^^^^^".format(
                stage_name
            )
        )

        keywords["times"].append(time.time() - checkpoint_time)
        keywords["stages"].append(stage_name)

        if stage_name[-1] == "&":
            pypeline_stage_popened[stage_name] = stage_dict[stage_name].POPENED
            logger.info(
                "Captured %s's %d detached processes."
                % (stage_name, len(stage_dict[stage_name].POPENED))
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
        if stage_index + 1 < len(stage_names):
            logger.info("Next process")

            stage_index += 1
            stage_name = stage_names[stage_index]
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
                stage_name = stage_names[stage_index]
                if stage_name[0] == "*":
                    stage_name = stage_name[1:]

            # Break if there are no novel process argument-input permutations
            if stage_index < 0:
                logger.info("Post Processing Done!")
                redis_interface.set_status("Done")
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

            logger.info(f"Rewound to {stage_names[stage_index]}")
