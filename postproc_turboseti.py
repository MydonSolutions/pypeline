import subprocess
import os

PROC_ENV_KEY = 'PPTBSENV'
PROC_ARG_KEY = 'PPTBSARG'
PROC_INP_KEY = 'PPTBSINP'
PROC_NAME = 'turboSETI'

def run(argstr, inputs, envvar, instanceid):
	if len(inputs) == 0:
		print('Rawspec requires a single input path.')
		return []
	elif len(inputs) > 1:
		print('Rawspec requires a single input path. Ignoring {}'.format(inputs[1:]))

	inputpath = inputs[0]

	turboargs = argstr.split(' ')
	cmd = ['/home/sonata/miniconda3/bin/turboSETI', *turboargs, inputpath]

	env = os.environ.copy()
	if envvar is not None:
		for variablevalues in envvar.split(' '):
			print(variablevalues)
			if '=' in variablevalues:
				pair = variablevalues.split('=')
				env[pair[0]] = pair[1]
	
	print(' '.join(cmd))
	subprocess.run(cmd, env=env)
	
	turbo_output = inputpath
	if '-o' in turboargs:
		turbo_output = os.path.join(turboargs[turboargs.index('-o')+1], os.path.basename(inputpath))
	turbo_output = turbo_output.replace('fil', 'dat')
	return [turbo_output]