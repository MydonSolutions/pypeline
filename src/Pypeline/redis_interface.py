import time
from string import Template

class RedisInterface(object):
    POSTPROCHASH = Template("pypeline://${host}/${inst}/status")

    def __init__(self, hostname, instance_id, redis_obj, sub_instance_id=None):
        self.hostname = hostname
        self.instance_id = instance_id
        self.sub_instance_id = sub_instance_id
        self.redis_obj = redis_obj

        self.redis_pubsub = self.redis_obj.pubsub(
            ignore_subscribe_messages = True
        )
        self.redis_pubsub.subscribe("pypeline:///set")

        self.postproc_hash = self.POSTPROCHASH.substitute(
            host=hostname, inst=instance_id
        )

    def __del__(self):
        try:
            self.redis_pubsub.unsubscribe("pypeline:///set")
        except:
            pass

    def set(self, key, value):
        self.redis_obj.hset(self.postproc_hash, key, value)

    def get(self, keystr):
        return self.redis_obj.hget(self.postproc_hash, keystr)

    def get_all(self):
        return self.redis_obj.hgetall(self.postproc_hash)

    def clear(self, exclusion_list=[]):
        all_keys = self.redis_obj.hkeys(self.postproc_hash)
        keys_to_clear = [key for key in all_keys if key not in exclusion_list]
        if len(keys_to_clear) > 0:
            self.redis_obj.hdel(self.postproc_hash, *keys_to_clear)
    
    def get_broadcast_messages(self, timeout_s):
        message = self.redis_pubsub.get_message(timeout=timeout_s)
        if message is None:
            return False

        if isinstance(message.get("data"), bytes):
            message["data"]  = message["data"].decode()
        # TODO rather implement redis_obj.hset(, mapping={})
        for keyvaluestr in message["data"].split("\n"):
            parts = keyvaluestr.split("=")
            self.set(parts[0], '='.join(parts[1:]))
        return True

    def set_status(self, status):
        key = "STATUS"
        if self.sub_instance_id is not None:
            key = f"STATUS:{self.sub_instance_id}"

        self.set(key, status)