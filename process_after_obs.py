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
import postproc_aux

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

# def check_and_revive_hashpipe():
# 	if not block_until_pulse_change(5, silent=True):
# 		print('Hashpipe seems dead. Restarting.')

# 		obs_broke += 1
# 		kill_hashpipe_relevant()
# 		clear_shared_memory()
# 		time.sleep(1)
# 		start_hashpipe('ens6d1')
# 		if not block_until_pulse_change(20):
# 				print('Failed to restart Hashpipe. Exiting.')
# 				obs_fail = True
# 				break
# 		hashpipe_start = time.time()
# 		time.sleep(5)
# 		block_until_obsinfo_valid()

parser = argparse.ArgumentParser(description='Monitors the observations of an Hpguppi_daq instance '
                                             'starting rawspec and then turbo_seti after each observation.',
             formatter_class=argparse.ArgumentDefaultsHelpFormatter)

parser.add_argument('instance', type=int,
                    help='The instance ID of the hashpipe.')
args = parser.parse_args()

redishash = postproc_aux.HashpipeRedis(socket.gethostname(), args.instance)

# redishash.setkey('PPRWSARG=-f 1 -t 1\nPPTBSARG=-g y -c XX -p YY')
# print(redishash.getkey('PPRWSARG'))
# print(redishash.getkey('PPTBSARG'))

instance = args.instance
print('\n######Assuming Hashpipe Redis Gateway#####\n')
# print('\n######Ensuring Hashpipe Redis Gateway#####\n')
# start_redis_gateway(instance)
time.sleep(1)


# block_until_obsinfo_valid(instance)
# print('OBSINFO is %s.                                    '%(hashpipe_aux.get_hashpipe_key_value_str('OBSINFO')))

# subprocess.run(['/home/sonata/src/observing_campaign/start_record_in_x.py', '-H', instance, '-i', '5', '-n', '10'])

while(True):
	redishash.setkey('PPSTATUS=WAITING')
	print('\nWaiting while DAQSTATE != recording')
	while(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance) != 'recording'):
			print(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance), end='\r')
			time.sleep(0.25)
	print('\nWaiting while DAQSTATE == recording')
	while(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance) == 'recording'):
			print(hashpipe_aux.get_hashpipe_key_value_str('DAQSTATE', instance), end='\r')
			time.sleep(1)

	postprocs = redishash.getkey('POSTPROC').split(',')
	print(postprocs)

	postproc_inputs = {}
	postproc_inputindex = {}
	postproc_args = {}
	postproc_argindex = {}
	postproc_outputs = {}
	postproc_outputs['hpguppi'] = [hashpipe_aux.get_latest_raw_stem_in_dir(hashpipe_aux.get_hashpipe_capture_dir())]

	procindex = 0

	while True:
		proc = postprocs[procindex]
		if import_postproc_module(proc):
			postproc_inputs[proc] = redishash.getkey(globals()[proc].PROC_INP_KEY).split(',')
			postproc_inputindex[proc] = 0
			postproc_args[proc] = redishash.getkey(globals()[proc].PROC_ARG_KEY).split(',')
			postproc_argindex[proc] = 0

		redishash.setkey('PPSTATUS='+globals()[proc].PROC_NAME)

		procinput = postproc_outputs[postproc_inputs[proc][postproc_inputindex[proc]]]

		postproc_outputs[proc] = globals()[proc].run(
																								postproc_args[proc][postproc_argindex[proc]],
																								procinput
																								)

		postproc_inputindex[proc] += 1
		if postproc_inputindex[proc] >= len(postproc_inputs[proc]):
			postproc_inputindex[proc] = 0
			postproc_argindex[proc] += 1

		if procindex+1 < len(postprocs):
			print('\nNext process')

			procindex += 1
			proc = postprocs[procindex]
			postproc_inputindex[proc] = 0
			postproc_argindex[proc] = 0
		else:
			print('\nRewinding after '+proc)
			while (procindex >= 0
					and postproc_argindex[proc] >= len(postproc_args[proc]) ):
				print('{}: inputindex {}/{}, argindex {}/{}\n'.format(proc, postproc_inputindex[proc], len(postproc_inputs[proc]), postproc_argindex[proc], len(postproc_args[proc])))
				procindex -= 1
				proc = postprocs[procindex]


			if procindex < 0:
				print('\ndone')
				break
			print('{}: inputindex {}/{}, argindex {}/{}\n'.format(proc, postproc_inputindex[proc], len(postproc_inputs[proc]), postproc_argindex[proc], len(postproc_args[proc])))
			print('\nRewound to '+postprocs[procindex])
			
# redishash.setkey('PPSTATUS=PNGOUTPUT')
# pngcmd = ['python', '/home/sonata/src/observing_campaign/pipeline/run_find_plot_events.py', turbo_output, rawspec_outputstem]
# print(pngcmd)
# subprocess.run(pngcmd)

# rawfiles = glob.glob(rawstempath+'*.raw')
# redishash.setkey('PPSTATUS=REMOVERAW')
# for rawfile in rawfiles:
# 	removecmd = ['rm', rawfile]
# 	print(removecmd)
# 	os.remove(rawfile)
# 	# subprocess.run(removecmd)

