#!/usr/bin/env python
import subprocess
import argparse
import time
from datetime import datetime
import os
import glob
import redis
import socket
from string import Template
import argparse
import importlib
import threading

from SNAPobs.snap_hpguppi import hpguppi_monitor

#####################################################################
from string import Template
import redis

class RedisHash:
	def __init__(self, hostname, instance, redishost='redishost'):
		self.redis_obj = redis.Redis(redishost)

		REDISSETGW = Template('hashpipe://${host}/${inst}/set')
		self.set_hashpipe_chan = REDISSETGW.substitute(host=hostname, inst=instance)

		REDISGETGW = Template('hashpipe://${host}/${inst}/status')
		self.get_hashpipe_chan = REDISGETGW.substitute(host=hostname, inst=instance)
		print(self.set_hashpipe_chan)
		print(self.get_hashpipe_chan)

		REDISHASH = Template('postprocpype://${host}/${inst}/status')
		self.postproc_hash = REDISHASH.substitute(host=hostname, inst=instance)
		existing_keys = list(self.redis_obj.hgetall(self.postproc_hash).keys())
		if len(existing_keys) > 0:
			self.redis_obj.hdel(self.postproc_hash, *existing_keys) # empty the hash
		print(self.postproc_hash)

		self.postproc_chan = 'postprocpype:///set'
		self.redis_pubsub = self.redis_obj.pubsub()
		self.redis_pubsub.subscribe(self.postproc_chan)

	def setkey(self, keyvaluestr):
		self.redis_obj.publish(self.set_hashpipe_chan, keyvaluestr)

	def getkey(self, keystr, retry_count=5, retry_period_ms=50):
		ret = None
		while ret is None and retry_count > 0:
			try:
				ret = self.redis_obj.hget(self.get_hashpipe_chan, keystr).decode()
			except:
				time.sleep(retry_period_ms/1000)
				pass
			retry_count -= 1

		return ret

	def setpostprockey(self, key, value):
		self.redis_obj.hset(self.postproc_hash, key, value)

	def getpostprockey(self, keystr, retry_count=5, retry_period_ms=50):
		ret = None
		while ret is None and retry_count > 0:
			try:
				ret = self.redis_obj.hget(self.postproc_hash, keystr).decode()
			except:
				time.sleep(retry_period_ms/1000)
				pass
			retry_count -= 1

		return ret
#####################################################################

import sys
# insert the `hpguppi_pypeline.py` script's directory, so the adjacent
# postproc_stages can be imported
script_dir, _ = os.path.split(os.path.realpath(__file__))
print('postproc_stages.py taken to be in', script_dir)
sys.path.insert(0, script_dir)


STATUS_STR = "INITIALISING"
def publish_status_thr(redishash, sleep_interval):
		global STATUS_STR
		ellipsis_count = 0
		while(1):
			# time.sleep(sleep_interval)
			while(1):
				message = redishash.redis_pubsub.get_message(timeout=sleep_interval)
				if message is None or not isinstance(message.get('data'), bytes):
					break
				for keyvaluestr in message.get('data').decode().split('\n'):
					redishash.setpostprockey(*(keyvaluestr.split('=')))
			redishash.setpostprockey('STATUS', '%s'%(STATUS_STR+'.'*int(ellipsis_count)))
			redishash.setpostprockey('PULSE', '%s'%(datetime.now().strftime('%a %b %d %H:%M:%S %Y')))
			ellipsis_count = (ellipsis_count+1)%4

reloadFlagDict = {}

def import_postproc_stage(stagename):
	if stagename not in globals(): #stagename not in sys.modules:
		try:
			globals()[stagename] = importlib.import_module('postproc_'+stagename)
		except ModuleNotFoundError:
			print('Could not find {}.py stage!'.format(os.path.join(script_dir, 'postproc_'+stagename)))
			return False
			
		print('Imported the {} stage!'.format(stagename))
		reloadFlagDict[stagename] = False
		return True
	elif reloadFlagDict[stagename]:
		try:
			globals()[stagename] = importlib.reload(globals()[stagename])
		except ModuleNotFoundError:
			print('Could not find {}.py stage to reload, keeping old load!'.format(os.path.join(script_dir, 'postproc_'+stagename)))
			return True
		
		print('Reloaded the {} stage!'.format(stagename))
		reloadFlagDict[stagename] = False
		return True
	return None

def block_until_obsinfo_valid(instance=0):
	while('INVALID' in hpguppi_monitor.get_hashpipe_key_value_str('OBSINFO', instance)):
		print('OBSINFO is INVALID, will await VALID...', end='\r')
		time.sleep(1)

def replace_instance_keywords(keyword_dict, string):
	for keyword in keyword_dict.keys():
		keyword_value = str(keyword_dict[keyword]) if not isinstance(keyword_dict[keyword], list) else ' '.join(map(str, keyword_dict[keyword]))
		string = string.replace('${}$'.format(keyword), keyword_value)
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
				print('Stage {} produced zero outputs.'.format(symbol))
				return False
			input_values[symbol] = postproc_outputs[symbol]
		elif symbol[0] == '&' or (len(symbol) > 1 and symbol[0] in ['^', '*'] and symbol[1:] in postproc_outputs):
			if symbol[0] == '^':
				input_values[symbol] = postproc_lastinput[symbol[1:]]
			elif symbol[0] == '&':
				print('Detected verbatim input \"{}\"'.format(symbol))
				input_values[symbol] = [symbol[1:]]
			elif symbol[0] == '*':
				if len([symbol[1:]]) == 0:
					print('Stage {} produced zero outputs.'.format(symbol))
					return False
				print('Detected exhaustive input \"{}\"'.format(symbol))
				input_values[symbol] = [postproc_outputs[symbol[1:]]] # wrap in list to treat as single value
		else:
			print('No replacement for {} within INP key, probably it has not been run yet.'.format(symbol))
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
		value_dict[proc] = redishash.getpostprockey(key)
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
	print('{}: input_templateindex {}/{}, inputindex {}/{}, argindex {}/{}'.format(
		proc,
		inp_tmpidx_dict[proc]+1,
		len(inp_tmp_dict[proc]),
		inpidx_dict[proc]+1,
		len(inp_dict[proc]),
		argidx_dict[proc],
		len(arg_dict[proc])
		)
	)

def fetch_fill_key_dict(keys_dict):
	'''
		Fetch the values for the keys of a dictionary from the 
		hashpipe://.../status Redis Hash.
	'''
	for key in keys_dict.keys():
		keys_dict[key] = redishash.getkey(key)

parser = argparse.ArgumentParser(description='Monitors the observations of an Hpguppi_daq instance '
                                             'starting rawspec and then turbo_seti after each observation.',
             formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('instance', type=int,
                    help='The instance ID of the hashpipe.')
args = parser.parse_args()

redishash = RedisHash(socket.gethostname(), args.instance)

status_thread = threading.Thread(target=publish_status_thr, args=(redishash, 1.5), daemon=True)
status_thread.start()

instance = args.instance
print('\n######Assuming Hashpipe Redis Gateway#####\n')

instance_keywords = {}
instance_keywords['inst'] = instance
instance_keywords['hnme'] = socket.gethostname()
instance_keywords['stem'] = None # repopulated after each recording
instance_keywords['beg'] 	= time.time() # repopulated as each recording begins
instance_keywords['end'] 	= time.time() # repopulated as each recording ends
instance_keywords['time'] = [] # repopulated throughout each observation
instance_keywords['proc'] = [] # repopulated throughout each observation

instance_stage_popened = {}

time.sleep(1)
postproc_str = redishash.setpostprockey('#MODULES', 'skip')

while(True):
	STATUS_STR = "WAITING"
	# Wait until a recording starts
	print('\nWaiting while DAQSTATE != recording')
	for check_idx, check in enumerate([lambda x, y: x != y, lambda x, y: x == y]):
			while(check(hpguppi_monitor.get_hashpipe_key_value_str('DAQSTATE', instance), 'recording')):
					# print(hpguppi_monitor.get_hashpipe_key_value_str('DAQSTATE', instance), end='\r')
					time.sleep(0.25)
			
			if check_idx == 0:
					instance_keywords['beg'] = time.time()
					# Wait until the recording ends
					print('\nWaiting while DAQSTATE == recording')
	instance_keywords['end'] = time.time()

	postproc_str = redishash.getpostprockey('#MODULES')
	if postproc_str is None:
		print('#MODULES key is not found, ensure the Redis Gateway for instance %d is running. Not post-processing.'% instance )
		continue
	if 'skip' in postproc_str[0:4]:
		print('#MODULES key begins with skip, not post-processing.')
		continue
	postprocs = postproc_str.split(' ')
	print('Post Processes:\n\t', postprocs)
	
	instance_keywords['time'] = [] # repopulated throughout each observation
	instance_keywords['proc'] = [] # repopulated throughout each observation

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
	postproc_outputs['hpguppi'] = [None]
	attempt = 0
	while not postproc_outputs['hpguppi'][0] and attempt < 2:
		postproc_outputs['hpguppi'] = [hpguppi_monitor.get_latest_stem_in_dir(hpguppi_monitor.get_hashpipe_capture_dir(instance))]
		attempt += 1
		time.sleep(1)

	if not postproc_outputs['hpguppi'][0]:
		print('No captured RAW data found for post-processing.')
		continue

	instance_keywords['stem'] = os.path.basename(postproc_outputs['hpguppi'][0])

	procindex = 0

	while True:
		proc = postprocs[procindex]
		proc_critical = True
		if proc[0] == '*':
			proc_critical = False
			proc = proc[1:]

		# Before reimporting, wait on all previous POPENED
		if proc[-1] == '&' and proc in instance_stage_popened:
			STATUS_STR = "POLL LAST DETACHED " + globals()[proc].PROC_NAME
			for popenIdx, popen in enumerate(instance_stage_popened[proc]):
				poll_count = 0
				while popen.poll() is None:
					if poll_count == 0:
						print('Polling previous %s stage\'s detached process for completion (#%d)' % (proc, popenIdx))
					poll_count = (poll_count + 1)%10
					time.sleep(1)
			print('%s\'s %d process(es) are complete.' % (proc, len(instance_stage_popened[proc])))
			print()

		if import_postproc_stage(proc) is False:
			print('Skipping post-processing on account of missing stage.')
			break

		STATUS_STR = "SETUP " + globals()[proc].PROC_NAME
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
			if postproc_inputs[proc] is False:
				print('Bailing on post-processing...')
				break

		# Set status (the publish_status_thread reads from this global)
		STATUS_STR = globals()[proc].PROC_NAME

		inp = postproc_inputs[proc][postproc_inputindices[proc]]
		if inp is False:
			print('Bailing on post-processing...')
			break

		postproc_lastinput[proc] = inp

		arg = postproc_args[proc][postproc_argindices[proc]]
		arg = replace_instance_keywords(instance_keywords, arg) if arg is not None else None

		env = postproc_envvar[proc]
		env = replace_instance_keywords(instance_keywords, env) if env is not None else None

		if hasattr(globals()[proc], 'PROC_STATUS_KEYS'):
			fetch_fill_key_dict(globals()[proc].PROC_STATUS_KEYS)
		# Run the process
		print('\n----------------- {:^12s} -----------------'.format(globals()[proc].PROC_NAME))
		checkpoint_time = time.time()
		try:
			postproc_outputs[proc] = globals()[proc].run(
																									arg,
																									inp,
																									env
																									)
		except:
			reloadFlagDict[proc] = True
			print('%s.run() failed.'%proc)
			if proc_critical:
				print('Bailing on post-processing...')
				break
		
		print('\n^^^^^^^^^^^^^^^^^ {:^12s} ^^^^^^^^^^^^^^^^^'.format(globals()[proc].PROC_NAME))

		instance_keywords['time'].append(time.time() - checkpoint_time) 
		instance_keywords['proc'].append(globals()[proc].PROC_NAME)
		STATUS_STR = "FINISHED " + STATUS_STR

		if proc[-1] == '&':
			instance_stage_popened[proc] = globals()[proc].POPENED
			print('Captured %s\'s %d detached processes.' % (proc, len(globals()[proc].POPENED)))

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
			if proc[0] == '*':
				proc = proc[1:]
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
				if proc[0] == '*':
					proc = proc[1:]

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