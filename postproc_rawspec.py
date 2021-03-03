import subprocess
import os
import glob

PROC_ENV_KEY = 'PPRWSENV'
PROC_ARG_KEY = 'PPRWSARG'
PROC_INP_KEY = 'PPRWSINP'
PROC_NAME = 'RAWSPEC'

def run(argstr, inputs, envvar, instance_keywords):
	if len(inputs) == 0:
		print('Rawspec requires a single input path.')
		return []
	elif len(inputs) > 1:
		print('Rawspec requires a single input path. Ignoring {}'.format(inputs[1:]))
	
	inputpath = inputs[0]
	
	rawargs = argstr.split(' ')
	rawargs.append('-d')
	rawargs.append('/mnt/buf{}/rawspec/{}/'.format(instance_keywords['instance'], inputpath.split('/')[-1]))
	
	cmd = ['mkdir', '-p', rawargs[-1]]
	print(' '.join(cmd))
	subprocess.run(cmd)

	cmd = ['/opt/mnt/bin/rawspec', *rawargs, inputpath]

	env = os.environ.copy()
	if envvar is not None:
		for variablevalues in envvar.split(' '):
			print(variablevalues)
			if ':' in variablevalues:
				pair = variablevalues.split(':')
				print('Environment variable-value pair:', pair)
				env[pair[0]] = pair[1]
	
	print(' '.join(cmd))
	subprocess.run(cmd, env=env)

	rawspec_outputstem = inputpath
	if '-d' in rawargs:
		rawspec_outputstem = os.path.join(rawargs[rawargs.index('-d')+1], os.path.basename(inputpath))
	
	rawspec_outputs = glob.glob(rawspec_outputstem+'*.fil')
	print('rawspec_outputs:', rawspec_outputs)

	return rawspec_outputs