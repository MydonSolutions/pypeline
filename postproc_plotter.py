import subprocess

PROC_ARG_KEY = None
PROC_INP_KEY = 'PPPLTINP'
PROC_NAME = 'plotter'

def run(argstr, inputs):
	if len(inputs) < 2:
		print('Plotter requires a two input paths, the turbo seti output and rawspec\'s output.')
		return []
	elif len(inputs) > 2:
		print('Plotter requires a two input paths, the turbo seti output and rawspec\'s output. Ignoring {}'.format(inputs[2:]))

	inputpaths = inputs[0:2]

	cmd = ['python', '/home/sonata/src/observing_campaign/pipeline/run_find_plot_events.py', *inputpaths]
	print(' '.join(cmd))
	subprocess.run(cmd)
	
	return []