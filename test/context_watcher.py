import json, logging

from Pypeline import ProcessNote, RedisClientInterface, ServiceIdentifier, JobEventMessage

NAME = "watcher"

STATE_redis_client = None
STATE_env = None

def setup(hostname, instance, logger = None):
    pass


def dehydrate():
    global STATE_env
    return (STATE_env, )


def rehydrate(dehydration_tuple):
    global STATE_env
    STATE_env = dehydration_tuple[0]


def run(env = None, logger = None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_redis_client, STATE_env

    if STATE_redis_client is None:
        assert env is not None
        STATE_env = {
            env_str[0:env_split_index]: env_str[env_split_index+1:] 
            for env_str, env_split_index in map(lambda s: (s, s.index("=")), env.split(" "))
        }
        STATE_redis_client = RedisClientInterface(
            ServiceIdentifier(
                hostname=STATE_env["TARGET_HOSTNAME"],
                enumeration=STATE_env["TARGET_ENUMERATION"],
            ),
            host=STATE_env["REDIS_HOSTNAME"],
            port=STATE_env["REDIS_PORT"],
            timeout_s=None
        )

    logger.info(f"Blocking on job event message from {STATE_redis_client.id}")
    job_event_message = STATE_redis_client.job_event_message
    if job_event_message is not None:
        job_event_message = JobEventMessage(**json.loads(job_event_message["data"]))
    
    logger.info(f"Event: {job_event_message}")
    if job_event_message is not None:
        return [
            job_event_message
        ]
    return job_event_message


def setupstage(stage, logger = None):
    pass


def note(processnote: ProcessNote, **kwargs):
    pass
