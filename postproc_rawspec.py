import subprocess
import os

PROC_ARG_KEY = 'PPRWSARG'
PROC_INP_KEY = 'PPRWSINP'
PROC_NAME = 'RAWSPEC'

def run(argstr, inputs):
	if len(inputs) == 0:
		print('Rawspec requires a single input path.')
		return []
	elif len(inputs) > 1:
		print('Rawspec requires a single input path. Ignoring {}'.format(inputs[1:]))
	
	inputpath = inputs[0]
	
	rawargs = argstr.split(' ')
	rawspeccmd = ['/opt/mnt/bin/rawspec', *rawargs, inputpath]
	print(' '.join(rawspeccmd))
	subprocess.run(rawspeccmd)

	rawspec_outputstem = inputpath
	if '-d' in rawargs:
		rawspec_outputstem = os.path.join(rawargs[rawargs.index('-d')+1], os.path.basename(inputpath))
	rawspec_outputs = [rawspec_outputstem + '-ant000.rawspec.0000.fil']

	return rawspec_outputs