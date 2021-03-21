import subprocess
import os
import glob

PROC_ENV_KEY = None
PROC_ARG_KEY = None
PROC_INP_KEY = 'PPPLTINP'
PROC_NAME = 'plotter'

def run(argstr, inputs, envvar):
	if len(inputs) < 2:
		print('Plotter requires a two input paths, the turbo seti output and rawspec\'s output.')
		return []
	elif len(inputs) > 2:
		print('Plotter requires a two input paths, the turbo seti output and rawspec\'s output. Ignoring {}'.format(inputs[2:]))

	inputpaths = inputs[0:2]

	cmd = ['python', '/home/sonata/src/observing_campaign/pipeline/run_find_plot_events.py', *inputpaths]
	
	env = os.environ.copy()
	if envvar is not None:
		for variablevalues in envvar.split(' '):
			print(variablevalues)
			if ':' in variablevalues:
				pair = variablevalues.split(':')
				env[pair[0]] = pair[1]
	
	print(' '.join(cmd))
	subprocess.run(cmd, env=env)

	rawspec_output = inputpaths[0] if '.fil' in inputpaths[0] else inputpaths[1]
	
	plotter_output = glob.glob(os.path.dirname(rawspec_output)+'/*.png')

	return plotter_output if plotter_output is not None else []