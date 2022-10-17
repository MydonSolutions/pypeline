#!/usr/bin/env python
import subprocess
import argparse
import time
from datetime import datetime
import os
import re
import redis
import glob
import socket
from string import Template
import argparse
import importlib
import threading

#####################################################################

class PostProcKeyValues(object):
    POSTPROCHASH = Template('postprocpype://${host}/${inst}/status')

    def __init__(self, hostname, instance_id, redis_obj):
        self.hostname = hostname
        self.instance_id = instance_id
        self.redis_obj = redis_obj

        self.redis_pubsub = self.redis_obj.pubsub()
        self.redis_pubsub.subscribe('postprocpype:///set')

        self.postproc_hash = self.POSTPROCHASH.substitute(host=hostname, inst=instance_id)
        self.clearpostprockeys()
    
    def __del__(self):
        self.redis_pubsub.unsubscribe('postprocpype:///set')

    def setpostprockey(self, key, value):
        self.redis_obj.hset(self.postproc_hash, key, value)

    def getpostprockey(self, keystr, retry_count=5, retry_period_ms=50):
        ret = None
        while ret is None and retry_count > 0:
            try:
                ret = self.redis_obj.hget(self.postproc_hash, keystr)
            except:
                time.sleep(retry_period_ms/1000)
                pass
            retry_count -= 1

        return ret

    def clearpostprockeys(self, exclusion_list=[]):
        all_keys = self.redis_obj.hgetall(self.postproc_hash).keys()
        keys_to_clear = [key for key in all_keys if key not in exclusion_list]
        if len(keys_to_clear) > 0:
            self.redis_obj.hdel(self.postproc_hash, *keys_to_clear)
						
#####################################################################

import sys
# insert the `hpguppi_pypeline.py` script's directory, so the adjacent
# postproc_stages can be imported
script_dir, _ = os.path.split(os.path.realpath(__file__))
print('postproc_stages.py taken to be in', script_dir)
sys.path.insert(0, script_dir)


STATUS_STR = "INITIALISING"
def publish_status_thr(ppkv, sleep_interval, reloadFlagDict = {}):
		global STATUS_STR
		previous_stage_list = ppkv.getpostprockey("#MODULES")
		ellipsis_count = 0
		while(1):
			# time.sleep(sleep_interval)
			while(1):
				message = ppkv.redis_pubsub.get_message(timeout=sleep_interval)
				if message is None or not isinstance(message.get('data'), bytes):
					break
				for keyvaluestr in message.get('data').decode().split('\n'):
					ppkv.setpostprockey(*(keyvaluestr.split('=')))
			
			stage_list = ppkv.getpostprockey("#MODULES")
			if (stage_list is not None and 
				stage_list != previous_stage_list and STATUS_STR == "WAITING"
			):
				# clear unused redis-hash keys
				previous_stage_list = stage_list
				rediskeys_in_use = ['#MODULES', 'STATUS', 'PULSE']
				for proc in stage_list.split(' '):
					if proc != 'skip' and proc in globals():
						proc_pymod = globals()[proc]
						key = proc_pymod.PROC_ENV_KEY if hasattr(proc_pymod, 'PROC_ENV_KEY') else None
						if key is not None:
							rediskeys_in_use.append(key)
						key = proc_pymod.PROC_INP_KEY if hasattr(proc_pymod, 'PROC_INP_KEY') else None
						if key is not None:
							rediskeys_in_use.append(key)
						key = proc_pymod.PROC_ARG_KEY if hasattr(proc_pymod, 'PROC_ARG_KEY') else None
						if key is not None:
							rediskeys_in_use.append(key)
				ppkv.clearpostprockeys(exclusion_list = rediskeys_in_use)

			ppkv.setpostprockey('STATUS', '%s'%(STATUS_STR+'.'*int(ellipsis_count)))
			ppkv.setpostprockey('PULSE', '%s'%(datetime.now().strftime('%a %b %d %H:%M:%S %Y')))
			ellipsis_count = (ellipsis_count+1)%4

def import_stage(stagename, reloadFlagDict, stagePrefix='postproc'):
	if stagename not in globals(): #stagename not in sys.modules:
		try:
			globals()[stagename] = importlib.import_module(f'{stagePrefix}_{stagename}')
		except ModuleNotFoundError:
			print('Could not find {}.py stage!'.format(os.path.join(script_dir, f'{stagePrefix}_{stagename}')))
			return False
			
		print('Imported the {} stage!'.format(stagename))
		reloadFlagDict[stagename] = False
		return True
	elif reloadFlagDict[stagename]:
		try:
			globals()[stagename] = importlib.reload(globals()[stagename])
		except ModuleNotFoundError:
			print('Could not find {}.py stage to reload, keeping old load!'.format(os.path.join(script_dir, f'{stagePrefix}_{stagename}')))
			return True
		
		print('Reloaded the {} stage!'.format(stagename))
		reloadFlagDict[stagename] = False
		return True
	return None

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

def fetch_proc_key_value(key, proc, value_dict, index_dict, ppkv, value_delimiter):
	if (proc not in value_dict) and (key is not None):
		value_dict[proc] = ppkv.getpostprockey(key)
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

reloadFlagDict = {}

parser = argparse.ArgumentParser(description='Monitors the observations of an Hpguppi_daq instance.',
             formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('instance', type=int,
                    help='The instance ID of the hashpipe.')
parser.add_argument('procstage', type=str,
                    help='The name of process stage.')
parser.add_argument('-kv', type=str, nargs='*', default=['#MODULES=skip'],
                    help='key=value strings to set in the pypeline Redis Hash.')
args = parser.parse_args()

print('\n######Assuming Hashpipe Redis Gateway#####\n')

assert import_stage(args.procstage, reloadFlagDict, stagePrefix="proc")

instance_keywords = {}
instance_keywords['inst'] = args.instance
instance_keywords['hnme'] = socket.gethostname()
instance_keywords['beg'] 	= time.time() # repopulated as each recording begins
instance_keywords['end'] 	= time.time() # repopulated as each recording ends
instance_keywords['time'] = [] # repopulated throughout each observation
instance_keywords['proc'] = [] # repopulated throughout each observation

instance_stage_popened = {}

ppkv = PostProcKeyValues(
	instance_keywords['hnme'],
	instance_keywords['inst'],
	redis.Redis('redishost', decode_responses=True)
)

ppkv.setpostprockey('#PROCESS', args.procstage)

globals()[args.procstage].setup(
	instance_keywords['hnme'],
	instance_keywords['inst'],
)

for kvstr in args.kv:
	delim_idx = kvstr.index('=')
	ppkv.setpostprockey(kvstr[0:delim_idx], kvstr[delim_idx+1:])

status_thread = threading.Thread(target=publish_status_thr, args=(ppkv, 1.5), daemon=False)
status_thread.start()

time.sleep(1)

while(True):
	STATUS_STR = "WAITING"
	# Wait until the process-stage returns outputs
	try:
		proc_outputs = globals()[args.procstage].run()
	except KeyboardInterrupt:
		status_thread.join()
		exit(0)

	if len(proc_outputs) == 0:
		print('No captured data found for post-processing.')
		continue

	postproc_str = ppkv.getpostprockey('#MODULES')
	if postproc_str is None:
		print('#MODULES key is not found. Not post-processing.')
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
	postproc_outputs = {
		args.procstage: proc_outputs
	}
	attempt = 0

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

		if import_stage(proc, reloadFlagDict) is False:
			print('Skipping post-processing on account of missing stage.')
			break

		STATUS_STR = "SETUP " + globals()[proc].PROC_NAME
		envkey = globals()[proc].PROC_ENV_KEY
		inpkey = globals()[proc].PROC_INP_KEY
		argkey = globals()[proc].PROC_ARG_KEY

		# Load INP, ARG and ENV key's value for the process if applicable
		if not fetch_proc_key_value(inpkey, proc, postproc_input_templates, postproc_input_templateindices, ppkv, ','):
			break
		if not fetch_proc_key_value(argkey, proc, postproc_args, postproc_argindices, ppkv, ','):
			break
		if not fetch_proc_key_value(envkey, proc, postproc_envvar, None, ppkv, None):
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

		# Run the process
		print('\n----------------- {:^12s} -----------------'.format(globals()[proc].PROC_NAME))
		checkpoint_time = time.time()
		try:
			postproc_outputs[proc] = globals()[proc].run(arg,inp,env)
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