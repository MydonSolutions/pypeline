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
import redishash

#from Hashpipe import hashpipe_aux
import sys
sys.path.insert(0, '/home/sonata/src/hpguppi_daq')
import hashpipe_aux

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

def parse_input_keywords(input_keywords, postproc_outputs, postproc_lastinput):
	ret = []
	for keyword in input_keywords.split(' '):
		if keyword in postproc_outputs:
			ret.append(*postproc_outputs[keyword])
		elif keyword[0] == '^':
			if keyword[1:] in postproc_lastinput:
				ret.append(*postproc_lastinput[keyword[1:]])
			else:
				print('No lastinput for {}, probably it has not been run yet. Bailing.'.format(keyword[1:]))
				return False
		else:
			print('No outut for {}, probably it has not been run yet. Bailing.'.format(keyword))
			return False
	
	return ret

parser = argparse.ArgumentParser(description='Monitors the observations of an Hpguppi_daq instance '
                                             'starting rawspec and then turbo_seti after each observation.',
             formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('instance', type=int,
                    help='The instance ID of the hashpipe.')
args = parser.parse_args()

redishash = redishash.RedisHash(socket.gethostname(), args.instance)

instance = args.instance
print('\n######Assuming Hashpipe Redis Gateway#####\n')

time.sleep(1)

while(True):
	# Wait until a recording starts
	redishash.setkey('PPSTATUS=WAITING')
	print('\nWaiting while DAQSTATE != recording')
	while(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance) != 'recording'):
			print(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance), end='\r')
			time.sleep(0.25)
	# Wait until the recording ends
	print('\nWaiting while DAQSTATE == recording')
	while(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance) == 'recording'):
			print(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance), end='\r')
			time.sleep(1)

	postprocs = redishash.getkey('POSTPROC').split(',')
	print('Post Processes:\n\t', postprocs)

	# Reset dictionaries for the post-process run
	postproc_inputs = {}
	postproc_lastinput = {}
	postproc_inputindices = {}
	postproc_args = {}
	postproc_argindices = {}
	postproc_outputs = {}
	postproc_outputs['hpguppi'] = [hashpipe_aux.get_latest_raw_stem_in_dir(hashpipe_aux.get_hashpipe_capture_dir())]

	procindex = 0

	while True:
		proc = postprocs[procindex]
		import_postproc_module(proc)

		inpkey = globals()[proc].PROC_INP_KEY
		argkey = globals()[proc].PROC_ARG_KEY

		# Load INP key's value for the process if applicable
		if (proc not in postproc_inputs) and (inpkey is not None):
			postproc_inputs[proc] = redishash.getkey(inpkey) if inpkey is not None else None
			if postproc_inputs[proc] is None:
				print('Post-Process {}: missing input key \'{}\', bailing.'.format(proc, PROC_INP_KEY))
				break
			postproc_inputs[proc] = postproc_inputs[proc].split(',')
			postproc_inputindices[proc] = 0
		elif inpkey is None:
			postproc_inputs[proc] = [None]
			postproc_inputindices[proc] = 0

		# Load ARG key's value for the process if applicable
		if (proc not in postproc_args) and (argkey is not None):
			postproc_args[proc] = redishash.getkey(argkey)
			postproc_argindices[proc] = 0
			if postproc_args[proc] is None:
				print('Post-Process {}: no args key found \'{}\'.'.format(proc, PROC_ARG_KEY))
				postproc_args[proc] = [None]
			else:
				postproc_args[proc] = postproc_args[proc].split(',')
		elif argkey is None:
			postproc_args[proc] = [None]
			postproc_argindices[proc] = 0

		# Set status
		redishash.setkey('PPSTATUS='+globals()[proc].PROC_NAME)

		# Parse input's keywords afresh each time
		postproc_inputkeywords = postproc_inputs[proc][postproc_inputindices[proc]]
		postproc_lastinput[proc] = parse_input_keywords(postproc_inputkeywords, postproc_outputs, postproc_lastinput)
		if postproc_lastinput[proc] is False:
			break

		# Run the process
		# TODO wrap in try..except and remove module on exception so it is reloaded
		postproc_outputs[proc] = globals()[proc].run(
																								postproc_args[proc][postproc_argindices[proc]],
																								postproc_lastinput[proc]
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
				print('{}: inputindex {}/{}, argindex {}/{}\n'.format(proc, postproc_inputindices[proc], len(postproc_inputs[proc]), postproc_argindices[proc], len(postproc_args[proc])))
				procindex -= 1
				proc = postprocs[procindex]

			# Break if there are no novel process argument-input permutations
			if procindex < 0:
				print('\nPost Processing Done!')
				break
			print('{}: inputindex {}/{}, argindex {}/{}'.format(proc, postproc_inputindices[proc], len(postproc_inputs[proc]), postproc_argindices[proc], len(postproc_args[proc])))
			print('\nRewound to {}\n'.format(postprocs[procindex]))
