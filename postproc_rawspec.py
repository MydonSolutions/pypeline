import subprocess
import os
import glob
import re

PROC_ENV_KEY = 'RAWSPENV'
PROC_ARG_KEY = 'RAWSPARG'
PROC_INP_KEY = 'RAWSPINP'
PROC_NAME = 'RAWSPEC'

def run(argstr, inputs, envvar):
	if len(inputs) == 0:
		print('Rawspec requires a single input path.')
		return []
	elif len(inputs) > 1:
		print('Rawspec requires a single input path. Ignoring {}'.format(inputs[1:]))
	
	inputpath = inputs[0]
	
	rawargs = argstr.split(' ')
	if '-d' in rawargs:
		rawargs[rawargs.index('-d')+1]
		cmd = ['mkdir', '-p', rawargs[-1]]
		print(' '.join(cmd))
		subprocess.run(cmd)

	cmd = ['/opt/mnt/bin/rawspec', *rawargs, inputpath]

	env = os.environ.copy()
	if envvar is not None:
		envvar = envvar.split(' ')
		if '--numa' in envvar:
			numa_argindex = envvar.index('--numa')
			try:
				cpubind = envvar[numa_argindex+1]
				membind = int(envvar[numa_argindex+2]) # safe to assume that this is node number
			except:
				print('Error parsing the --numa args:', envvar[numa_argindex:])
				cpubind = None

			if cpubind[0] in ['-', '+']:
				# relative CPU binding
				cpu_info = subprocess.run('lscpu', capture_output=True).stdout.decode().strip()
				try:
					m = re.search(r'CPU\(s\):\s*(\d+)', cpu_info)
					cores_per_cpu = int(m.group(1))
				except:
					print('Error trying to get the cpu core count.')
					cpubind = None
				try:
					m = re.search(r'NUMA node\(s\):\s*(\d+)', cpu_info)
					cores_per_numa = cores_per_cpu//int(m.group(1))
				except:
					print('Error trying to get the numa core count.')
					cpubind = None
				
				if cpubind and cpubind[0] == '-':
					cpubind = (membind+1)*cores_per_numa - int(cpubind[1:])
				elif cpubind and cpubind[0] == '+':
					cpubind = membind*cores_per_numa + int(cpubind[1:])
			else:
				cpubind = int(cpubind)

			if cpubind:
				cmd = ['numactl', '--physcpubind=%d'%(cpubind), '--membind=%d'%(membind)] + cmd
			
			envvar.pop(numa_argindex)
			envvar.pop(numa_argindex)
			envvar.pop(numa_argindex)
			
		for variablevalues in envvar:
			print(variablevalues)
			if ':' in variablevalues:
				pair = variablevalues.split(':')
				print('Environment variable-value pair:', pair)
				env[pair[0]] = pair[1]
	
	# print(' '.join(cmd))
	print(cmd)
	subprocess.run(cmd, env=env)

	rawspec_outputstem = inputpath
	if '-d' in rawargs:
		rawspec_outputstem = os.path.join(rawargs[rawargs.index('-d')+1], os.path.basename(inputpath))
	
	return glob.glob(rawspec_outputstem+'*.fil')

if __name__ == '__main__':
    run('-f 262144 -t 2 -I 1.0 -d /mnt/buf0/rawspec/bogus', ['bogus'], 'CUDA_VISIBLE_DEVICES:$inst$ --numa -4 0')
    run('-f 262144 -t 2 -I 1.0 -d /mnt/buf1/rawspec/bogus', ['bogus'], 'CUDA_VISIBLE_DEVICES:$inst$ --numa -4 1')
    run('-f 262144 -t 2 -I 1.0 -d /mnt/buf0/rawspec/bogus', ['bogus'], 'CUDA_VISIBLE_DEVICES:$inst$ --numa +4 0')
    run('-f 262144 -t 2 -I 1.0 -d /mnt/buf0/rawspec/bogus', ['bogus'], 'CUDA_VISIBLE_DEVICES:$inst$ --numa +4 1')
    run('-f 262144 -t 2 -I 1.0 -d /mnt/buf0/rawspec/bogus', ['bogus'], 'CUDA_VISIBLE_DEVICES:$inst$ --numa 4 0')
    run('-f 262144 -t 2 -I 1.0 -d /mnt/buf0/rawspec/bogus', ['bogus'], 'CUDA_VISIBLE_DEVICES:$inst$ --numa 4 1')