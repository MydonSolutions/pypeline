import subprocess
import os
import glob
import re

PROC_ENV_KEY = None
PROC_ARG_KEY = None
PROC_INP_KEY = 'PPRMINP'
PROC_NAME = 'rm'

def run(argstr, inputs, env):
	if len(inputs) == 0:
		print('rm requires a single path, and optionally filters for extensions.')
		return []
	# elif len(inputs) > 2:
	# 	print('rm requires a single path, and optionally filters for extensions. Ignoring {}'.format(inputs[2:]))

	patterns = [inputs[0]]
	if len(inputs) > 1:
		inputprefix = inputs[0]
		print(inputprefix)

		# try take prefix as preceding \..\d+\..*
		inputprefix = re.match(r'(?P<prefix>^.*)\.\d+\..+$', inputprefix)
		if inputprefix is not None:
			inputprefix = inputprefix.group('prefix')
		else:
			inputprefix = inputs[0]
			inputprefix = inputprefix[0:inputprefix.find(".")]

		patterns = [inputprefix+extension for extension in inputs[1:]]

	for pattern in patterns:
		print(pattern)

		matchedfiles = glob.glob(pattern)
		for rawfile in matchedfiles:
			cmd = ['sudo', '/opt/mnt/bin/delete_stems.sh', rawfile]
			print(' '.join(cmd))
			subprocess.run(cmd)#, env=env)
	
	return patterns