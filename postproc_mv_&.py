import subprocess
import argparse
import os
import glob
import re

PROC_ENV_KEY = None
PROC_ARG_KEY = 'PPMV&ARG'
PROC_INP_KEY = 'PPMV&INP'
PROC_NAME = 'mv'
POPENED = []

def run(argstr, inputs, env):
	global POPENED
	if len(inputs) == 0:
		print('mv requires a single path, and optionally filters for extensions.')
		return []

	patterns = [inputs[0]]
	if len(inputs) > 1:
		path_head, path_tail = os.path.split(os.path.normpath(inputs[0]))
		# print(path_head)
		# print(path_tail)

		# try take stem as preceding \..\d+\..*
		filestem = re.match(r'(?P<stem>^.*)\.\d+\..+$', path_tail)
		if filestem is not None:
			filestem = filestem.group('stem')
		else:
			filestem = path_tail
			try:
				last_point = filestem.rindex(".")
				filestem = filestem[0:last_point]
			except:
				pass

		patterns = [os.path.join(path_head, filestem+extension) for extension in inputs[1:]]

	parser = argparse.ArgumentParser(description='Move files in a detached process')
	parser.add_argument('destination', type=str, help='The destination directory for the move.')
	parser.add_argument('-i', type=int, required=False,
			help='The instance ID (use $inst$) used as the numa-memory to bind to')
	parser.add_argument('-c', type=int, required=False,
			help='The cpu-core to numa-bind to')
	parser.add_argument('-m', type=int, required=False,
			help='The memory-node to numa-bind to')

	args = parser.parse_args(argstr.split(' '))

	cmd = ['mkdir', '-p', args.destination]
	print(' '.join(cmd))
	subprocess.run(cmd)

	numactl = []
	if args.i is not None and args.c is not None:
		numactl = ['numactl', '-C', str(args.c), '-m', str(args.m)]


	POPENED = []
			cmd = numactl + ['mv', mfile, args.destination]
		print(pattern)

		matchedfiles = glob.glob(pattern)
		for mfile in matchedfiles:
			cmd = numactl + ['mv', mfile, args.destination]
			print(' '.join(cmd))
			
			# the move command is detached
			POPENED.append(subprocess.Popen(cmd))
			
	return patterns