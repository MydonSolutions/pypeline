import subprocess
import argparse
import os
import glob
import re
import time

PROC_ENV_KEY = None
PROC_ARG_KEY = 'MOVE&ARG'
PROC_INP_KEY = 'MOVE&INP'
PROC_NAME = 'mv'
POPENED = []

POPENED_TIMES = []
PROC_STATUS_KEYS = {'PROJID': None}
PROC_LOCAL_KEYWORD_STATUS_KEY_MAP = {'proj': 'PROJID'}

def run(argstr, inputs, env):
	global POPENED, POPENED_TIMES
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
			help='The instance ID (use $inst$) used as the numa-node to bind to')
	parser.add_argument('-c', type=int, required=False,
			help='The cpu-core to numa-bind to')
	parser.add_argument('-m', type=int, required=False,
			help='The memory-node to numa-bind to')

	args = parser.parse_args(argstr.split(' '))

	for (key, status_key) in PROC_LOCAL_KEYWORD_STATUS_KEY_MAP.items():
		args.destination = args.destination.replace('${}$'.format(key), PROC_STATUS_KEYS[status_key])

	destination_normed = os.path.normpath(args.destination)
	if(args.destination[-1] == '/'): # normpath removes trailing '/' which split needs to identify path as a directory
		destination_normed += '/'
	dest_dir, dest_filename = os.path.split(destination_normed)

	cmd = ['mkdir', '-p', dest_dir]
	print(' '.join(cmd))
	subprocess.run(cmd)

	numactl = []
	if args.i is not None:
		numactl = ['numactl', '-i', str(args.i)]
	elif args.c is not None and args.m is not None:
		numactl = ['numactl', '-C', str(args.c), '-m', str(args.m)]


	POPENED = []
	# for pattern in patterns:
	# 	cmd = ['mv', pattern, argstr]
	# 	print(' '.join(cmd))
		
	# 	# the move command is detached
	# 	POPENED.append(subprocess.Popen(cmd))
	# 	# POPENED_TIMES.append(time.time())
	for pattern in patterns:
		print("pattern:", pattern)

		matchedfiles = glob.glob(pattern)
		for mfile in matchedfiles:
			cmd = numactl + ['mv', mfile, destination_normed]
			print('\t', ' '.join(cmd))
			
			# the move command is detached
			POPENED.append(subprocess.Popen(cmd))
			
	return patterns

if __name__ == '__main__':
    run(
			"-i 0 /home/sonata/corr_data/bogus_stem/bogus_hostname/bogus_stem_0.uvh5",
			['bogus_stem'],
			None
		)