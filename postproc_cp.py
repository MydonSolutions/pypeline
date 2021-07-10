import subprocess
import os
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
		path_head, path_tail = os.path.split(os.path.normpath(inputs[0]))
		print(path_head)
		print(path_tail)

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

	cmd = ['mkdir', '-p', argstr]
	print(' '.join(cmd))
	subprocess.run(cmd)

	for pattern in patterns:
		cmd = ['cp', pattern, argstr]
		print(' '.join(cmd))
		subprocess.run(cmd)#, env=env)
	
	return patterns