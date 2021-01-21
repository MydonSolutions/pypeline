from string import Template
import redis

class RedisHash:
	def __init__(self, hostname, instance, redishost='redishost'):
		self.redis_obj = redis.Redis(redishost)

		REDISSETGW = Template('hashpipe://${host}/${inst}/set')
		self.set_chan = REDISSETGW.substitute(host=hostname, inst=instance)

		REDISGETGW = Template('hashpipe://${host}/${inst}/status')
		self.get_chan = REDISGETGW.substitute(host=hostname, inst=instance)
		print(self.set_chan)
		print(self.get_chan)

	def setkey(self, keyvaluestr):
		self.redis_obj.publish(self.set_chan, keyvaluestr)

	def getkey(self, keystr):
		ret = self.redis_obj.hget(self.get_chan, keystr)
		if ret is None:
			return None
		else:
			return ret.decode()