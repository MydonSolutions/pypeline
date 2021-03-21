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

reloadFlagDict = {}

def import_postproc_module(modulename):
	if modulename not in globals(): #modulename not in sys.modules:
		globals()[modulename] = importlib.import_module('postproc_'+modulename)
		print('Imported the {} module!'.format(modulename))
		reloadFlagDict[modulename] = False
		return True
	elif reloadFlagDict[modulename]:
		globals()[modulename] = importlib.reload(globals()[modulename])
		print('Reloaded the {} module!'.format(modulename))
		reloadFlagDict[modulename] = False
		return True
	return False

def block_until_obsinfo_valid(instance=0):
	while('INVALID' in hashpipe_aux.get_hashpipe_key_value_str('OBSINFO', instance)):
		print('OBSINFO is INVALID, will await VALID...', end='\r')
		time.sleep(1)

def replace_instance_keywords(keyword_dict, string):
	for keyword in keyword_dict.keys():
		string = string.replace('${}$'.format(keyword), str(keyword_dict[keyword]))
	return string

def parse_input_template(input_template, postproc_outputs, postproc_lastinput):
	if input_template is None:
		return [[]]
	ret = []
	input_values = {}
	value_indices = {}

	# Gather values for each
	input_template_symbols = input_template.split(' ')
	for symbol in input_template_symbols:
		if symbol in postproc_outputs:
			if len(postproc_outputs[symbol]) == 0:
				print('Module {} produced zero outputs.'.format(symbol))
				return False
			input_values[symbol] = postproc_outputs[symbol]
		elif symbol[0] in ['^', '&', '*']:
			if symbol[0] == '^' and symbol[1:] in postproc_lastinput:
				input_values[symbol] = postproc_lastinput[symbol[1:]]
			elif symbol[0] == '&':
				print('Detected verbatim input \"{}\"'.format(symbol))
				input_values[symbol] = [symbol[1:]]
			elif symbol[0] == '*':
				if len([symbol[1:]]) == 0:
					print('Module {} produced zero outputs.'.format(symbol))
					return False
				print('Detected exhausitve input \"{}\"'.format(symbol))
				input_values[symbol] = [postproc_outputs[symbol[1:]]] # wrap in list to treat as single value
		else:
			print('No replacement for {} within INP key, probably it has not been run yet.'.format(keyword))
			return False
		
		value_indices[symbol] = 0

	input_template_symbols_rev = input_template_symbols[::-1]

	fully_permutated = False

	while not fully_permutated:
		inp = []
		for symbol in input_template_symbols:
			if symbol[0] == '*':
				inp.extend(input_values[symbol][value_indices[symbol]])
			else:
				inp.append(input_values[symbol][value_indices[symbol]])
		ret.append(inp)

		for i, symbol in enumerate(input_template_symbols_rev):
			value_indices[symbol] += 1
			if value_indices[symbol] == len(input_values[symbol]):
				value_indices[symbol] = 0
				if i+1 == len(input_template_symbols_rev):
					fully_permutated = True
			else:
				break

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

def print_proc_dict_progress(proc, inp_tmp_dict, inp_tmpidx_dict, inp_dict, inpidx_dict, arg_dict, argidx_dict):
	print('{}: input_templateindex {}/{}, inputindex {}/{}, argindex {}/{}\n'.format(
		proc,
		inp_tmpidx_dict[proc],
		len(inp_tmp_dict[proc]),
		inpidx_dict[proc],
		len(inp_dict[proc]),
		argidx_dict[proc],
		len(arg_dict[proc])
		)
	)

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
instance_keywords['inst'] = instance
instance_keywords['hnme'] = socket.gethostname()
instance_keywords['stem'] = None # repopulated after each recording
instance_keywords['beg'] 	= time.time() # repopulated as each recording begins
instance_keywords['end'] 	= time.time() # repopulated as each recording ends

time.sleep(1)

while(True):
	# Wait until a recording starts
	redishash.setkey('PPSTATUS=WAITING')
	print('\nWaiting while DAQSTATE != recording')
	while(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance) != 'recording'):
			# print(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance), end='\r')
			time.sleep(0.25)
	instance_keywords['beg'] = time.time()
	# Wait until the recording ends
	print('\nWaiting while DAQSTATE == recording')
	while(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance) == 'recording'):
			# print(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance), end='\r')
			time.sleep(0.25)
	instance_keywords['end'] = time.time()

	postproc_str = redishash.getkey('POSTPROC')
	if 'skip' in postproc_str[0:4]:
		continue
	postprocs = redishash.getkey('POSTPROC').split(' ')
	print('Post Processes:\n\t', postprocs)

	# Reset dictionaries for the post-process run
	postproc_envvar = {}
	postproc_input_templates = {}
	postproc_input_templateindices = {}
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
		if not fetch_proc_key_value(inpkey, proc, postproc_input_templates, postproc_input_templateindices, redishash, ','):
			break
		if not fetch_proc_key_value(argkey, proc, postproc_args, postproc_argindices, redishash, ','):
			break
		if not fetch_proc_key_value(envkey, proc, postproc_envvar, None, redishash, None):
			break

		if proc not in postproc_inputindices:
			postproc_inputindices[proc] = 0

		if postproc_inputindices[proc] == 0:
			# Parse the input_template and create the input list from permutations
			proc_input_template = postproc_input_templates[proc][postproc_input_templateindices[proc]]
			postproc_inputs[proc] = parse_input_template(proc_input_template, postproc_outputs, postproc_lastinput)
			# print(proc, 'inputs:', postproc_inputs[proc])

		# Set status
		redishash.setkey('PPSTATUS='+globals()[proc].PROC_NAME)

		inp = postproc_inputs[proc][postproc_inputindices[proc]]
		if inp is False:
			print('Bailing on post-processing...')
			break

		postproc_lastinput[proc] = inp

		arg = postproc_args[proc][postproc_argindices[proc]]
		arg = replace_instance_keywords(instance_keywords, arg) if arg is not None else None

		env = postproc_envvar[proc]
		env = replace_instance_keywords(instance_keywords, env) if env is not None else None

		# Run the process
		try:
			postproc_outputs[proc] = globals()[proc].run(
																									arg,
																									inp,
																									env
																									)
		except:
			reloadFlagDict[proc] = True
			print('%s.run() failed. Bailing on post-processing...'%proc)
			break

		# Increment through inputs, overflow increment through arguments
		postproc_inputindices[proc] += 1
		if postproc_inputindices[proc] >= len(postproc_inputs[proc]):
			postproc_inputindices[proc] = 0
			postproc_input_templateindices[proc] += 1
			if postproc_input_templateindices[proc] >= len(postproc_input_templates[proc]):
				postproc_input_templateindices[proc] = 0
				postproc_argindices[proc] += 1

		# Proceed to next process or...
		if procindex+1 < len(postprocs):
			print('\nNext process')

			procindex += 1
			proc = postprocs[procindex]
			postproc_input_templateindices[proc] = 0
			postproc_inputindices[proc] = 0
			postproc_argindices[proc] = 0
		else: # ... rewind to the closest next novel process (argumentindices indicate exhausted permutations)
			print('\nRewinding after '+proc)
			while (procindex >= 0
					and postproc_argindices[proc] >= len(postproc_args[proc]) ):
				print_proc_dict_progress(proc,
					postproc_input_templates, postproc_input_templateindices,
					postproc_inputs, postproc_inputindices,
					postproc_args, postproc_argindices
				)
				procindex -= 1
				proc = postprocs[procindex]

			# Break if there are no novel process argument-input permutations
			if procindex < 0:
				print('\nPost Processing Done!')
				break
			print_proc_dict_progress(proc,
				postproc_input_templates, postproc_input_templateindices,
				postproc_inputs, postproc_inputindices,
				postproc_args, postproc_argindices
			)
			print('\nRewound to {}\n'.format(postprocs[procindex]))
	# End of while post_proc step

	for proc in reloadFlagDict.keys():
		reloadFlagDict[proc] = True

# End of main while(True)