PROC_ENV_KEY = None
PROC_ARG_KEY = 'TESTARG'
PROC_INP_KEY = 'TESTINP'
PROC_NAME = 'teststage'

def run(argstr, inputs, env):
	
	print('argstr:', argstr)
	print('inputs:', inputs)
	print('env:', env)
	
	return [
		f'Test input: {input_val}'
		for input_val in inputs
	]