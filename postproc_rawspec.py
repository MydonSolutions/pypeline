import subprocess
import os

PROC_ENV_KEY = 'PPRWSENV'
PROC_ARG_KEY = 'PPRWSARG'
PROC_INP_KEY = 'PPRWSINP'
PROC_NAME = 'RAWSPEC'

def run(argstr, inputs, envvar):
	if len(inputs) == 0:
		print('Rawspec requires a single input path.')
		return []
	elif len(inputs) > 1:
		print('Rawspec requires a single input path. Ignoring {}'.format(inputs[1:]))
	
	inputpath = inputs[0]
	
	rawargs = argstr.split(' ')
	cmd = ['/opt/mnt/bin/rawspec', *rawargs, inputpath]

	env = os.environ.copy()
	if envvar is not None:
		for variablevalues in envvar.split(' '):
			print(variablevalues)
			if '=' in variablevalues:
				pair = variablevalues.split('=')
				env[pair[0]] = pair[1]
	
	print(' '.join(cmd))
	subprocess.run(cmd, env=env)

	rawspec_outputstem = inputpath
	if '-d' in rawargs:
		rawspec_outputstem = os.path.join(rawargs[rawargs.index('-d')+1], os.path.basename(inputpath))
	rawspec_outputs = [rawspec_outputstem + '-ant000.rawspec.0000.fil']

	return rawspec_outputs