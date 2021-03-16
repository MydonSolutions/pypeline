#!/usr/bin/env python
import subprocess
import argparse
import time
import os
import glob
import redis
import socket
from string import Template
import argparse
import importlib

from HpguppiMon import hashpipe_aux

#####################################################################
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
#####################################################################

import sys
sys.path.insert(0, '/home/sonata/src/observing_campaign/pypeline/')

# sys.path.insert(0, '/home/sonata/src/hpguppi_daq')
# import hashpipe_aux

def import_postproc_module(modulename):
	if modulename not in globals(): #modulename not in sys.modules:
		globals()[modulename] = importlib.import_module('postproc_'+modulename)
		print('Imported the {} module!'.format(modulename))
		return True
	return False

def block_until_obsinfo_valid(instance=0):
	while('INVALID' in hashpipe_aux.get_hashpipe_key_value_str('OBSINFO', instance)):
		print('OBSINFO is INVALID, will await VALID...', end='\r')
		time.sleep(1)

def replace_instance_keywords(keyword_dict, string):
	for keyword in keyword_dict.keys():
		string = string.replace('${}$'.format(keyword), keyword_dict[keyword])
	return string

def parse_input_keywords(input_keywords, postproc_outputs, postproc_lastinput):
	ret = []
	for keyword in input_keywords.split(' '):
		if keyword in postproc_outputs:
			if len(postproc_outputs[keyword]) != 1:
				print('The number of outputs for {} is {} != 1.'.format(keyword, len(postproc_outputs[keyword])))
				return False
			ret.append(*postproc_outputs[keyword])
		elif keyword[0] in ['^', '&']:
			if keyword[0] == '^' and keyword[1:] in postproc_lastinput:
				ret.append(*postproc_lastinput[keyword[1:]])
			elif keyword[0] == '&':
				print('Detected verbatim input \"{}\"'.format(keyword[1:]))
				ret.append(keyword[1:])
		else:
			print('No output for {}, probably it has not been run yet.'.format(keyword))
			return False
	
	return ret

def fetch_proc_key_value(key, proc, value_dict, index_dict, redishash, value_delimiter):
	if (proc not in value_dict) and (key is not None):
		value_dict[proc] = redishash.getkey(key)
		if value_dict[proc] is None:
			print('Post-Process {}: missing key \'{}\', bailing.'.format(proc, key))
			return False
		if value_delimiter is not None:
			value_dict[proc] = value_dict[proc].split(value_delimiter)
		if index_dict is not None:
			index_dict[proc] = 0
	elif key is None:
		value_dict[proc] = [None] if value_delimiter is not None else None
		if index_dict is not None:
			index_dict[proc] = 0
	return True

def print_proc_dict_progress(proc, inp_dict, inpidx_dict, arg_dict, argidx_dict):
	print('{}: inputindex {}/{}, argindex {}/{}\n'.format(proc, inpidx_dict[proc], len(inp_dict[proc]), argidx_dict[proc], len(arg_dict[proc])))

parser = argparse.ArgumentParser(description='Monitors the observations of an Hpguppi_daq instance '
                                             'starting rawspec and then turbo_seti after each observation.',
             formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('instance', type=int,
                    help='The instance ID of the hashpipe.')
args = parser.parse_args()

redishash = RedisHash(socket.gethostname(), args.instance)

instance = args.instance
print('\n######Assuming Hashpipe Redis Gateway#####\n')

instance_keywords = {}
instance_keywords['inst'] = str(instance)
instance_keywords['hnme'] = socket.gethostname()

time.sleep(1)

while(True):
	# Wait until a recording starts
	redishash.setkey('PPSTATUS=WAITING')
	print('\nWaiting while DAQSTATE != recording')
	while(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance) != 'recording'):
			# print(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance), end='\r')
			time.sleep(0.25)
	# Wait until the recording ends
	print('\nWaiting while DAQSTATE == recording')
	while(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance) == 'recording'):
			# print(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance), end='\r')
			time.sleep(1)

	postproc_str = redishash.getkey('POSTPROC')
	if 'skip' in postproc_str[0:4]:
		continue
	postprocs = redishash.getkey('POSTPROC').split(' ')
	print('Post Processes:\n\t', postprocs)

	# Reset dictionaries for the post-process run
	postproc_envvar = {}
	postproc_inputs = {}
	postproc_inputindices = {}
	postproc_lastinput = {}
	postproc_args = {}
	postproc_argindices = {}
	postproc_outputs = {}
	postproc_outputs['hpguppi'] = [hashpipe_aux.get_latest_raw_stem_in_dir(hashpipe_aux.get_hashpipe_capture_dir(instance))]
	
	instance_keywords['stem'] = os.path.basename(postproc_outputs['hpguppi'][0])

	procindex = 0

	while True:
		proc = postprocs[procindex]
		import_postproc_module(proc)

		envkey = globals()[proc].PROC_ENV_KEY
		inpkey = globals()[proc].PROC_INP_KEY
		argkey = globals()[proc].PROC_ARG_KEY

		# Load INP, ARG and ENV key's value for the process if applicable
		if not fetch_proc_key_value(inpkey, proc, postproc_inputs, postproc_inputindices, redishash, ','):
			break
		if not fetch_proc_key_value(argkey, proc, postproc_args, postproc_argindices, redishash, ','):
			break
		if not fetch_proc_key_value(envkey, proc, postproc_envvar, None, redishash, None):
			break

		# Set status
		redishash.setkey('PPSTATUS='+globals()[proc].PROC_NAME)

		# Parse input's keywords afresh each time
		postproc_inputkeywords = postproc_inputs[proc][postproc_inputindices[proc]]
		postproc_lastinput[proc] = parse_input_keywords(postproc_inputkeywords, postproc_outputs, postproc_lastinput)
		if postproc_lastinput[proc] is False:
			print('Bailing...')
			break

		inp = postproc_lastinput[proc]

		arg = postproc_args[proc][postproc_argindices[proc]]
		arg = replace_instance_keywords(instance_keywords, arg) if arg is not None else None

		env = postproc_envvar[proc]
		env = replace_instance_keywords(instance_keywords, env) if env is not None else None

		# Run the process
		# TODO wrap in try..except and remove module on exception so it is reloaded
		postproc_outputs[proc] = globals()[proc].run(
																								arg,
																								inp,
																								env
																								)

		# Increment through inputs, overflow increment through arguments
		postproc_inputindices[proc] += 1
		if postproc_inputindices[proc] >= len(postproc_inputs[proc]):
			postproc_inputindices[proc] = 0
			postproc_argindices[proc] += 1

		# Proceed to next process or...
		if procindex+1 < len(postprocs):
			print('\nNext process')

			procindex += 1
			proc = postprocs[procindex]
			postproc_inputindices[proc] = 0
			postproc_argindices[proc] = 0
		else: # ... rewind to the closest next novel process (argumentindices indicate exhausted permutations)
			print('\nRewinding after '+proc)
			while (procindex >= 0
					and postproc_argindices[proc] >= len(postproc_args[proc]) ):
				print_proc_dict_progress(proc, postproc_inputs, postproc_inputindices, postproc_args, postproc_argindices)
				procindex -= 1
				proc = postprocs[procindex]

			# Break if there are no novel process argument-input permutations
			if procindex < 0:
				print('\nPost Processing Done!')
				break
			print_proc_dict_progress(proc, postproc_inputs, postproc_inputindices, postproc_args, postproc_argindices)
			print('\nRewound to {}\n'.format(postprocs[procindex]))
