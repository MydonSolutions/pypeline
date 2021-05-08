import subprocess
import os
import glob
import re

PROC_ENV_KEY = None
PROC_ARG_KEY = 'PPCPARG'
PROC_INP_KEY = 'PPCPINP'
PROC_NAME = 'cp'

def run(argstr, inputs, env):
	if len(inputs) == 0:
		print('cp requires a single path, and optionally filters for extensions.')
		return []

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

	cmd = ['mkdir', '-p', argstr]
	print(' '.join(cmd))
	subprocess.run(cmd)

	for pattern in patterns:
		print(pattern)

		matchedfiles = glob.glob(pattern)
		for mfile in matchedfiles:
			cmd = ['cp', mfile, argstr]
			print(' '.join(cmd))
			subprocess.run(cmd)#, env=env)
	
	return patterns