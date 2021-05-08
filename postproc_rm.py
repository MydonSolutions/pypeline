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
		path_head, path_tail = os.path.split(inputs[0])
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

	for pattern in patterns:
		print(pattern)

		matchedfiles = glob.glob(pattern)
		for rawfile in matchedfiles:
			cmd = ['sudo', '/opt/mnt/bin/delete_stems.sh', rawfile]
			print(' '.join(cmd))
			subprocess.run(cmd)#, env=env)
	
	return patterns