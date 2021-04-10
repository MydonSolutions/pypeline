import re
import csv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from blimpy import Waterfall

PROC_ENV_KEY = None
PROC_ARG_KEY = None
PROC_INP_KEY = 'PPFLTINP'
PROC_NAME = 'filplotter'

def run(argstr, inputs, envvar):
	filpaths = [inp for inp in inputs if '.fil' in inp]
	if len(inputs) < 1 or len(filpaths) < 1:
		print('FilPlotter requires the filterbank path(s) from rawspec\'s output.')
		return []
	
	a = filpaths[len(filpaths)+2]

	for filname in filpaths:
		obs = Waterfall(filname, max_load=20)

		hyphenDelimRE = r'(?P<val>-?[^-]+)-?'
		m = re.match(r'.*sy_(?P<id>\d+)', filname)
		if m:
			with open('/home/sonata/dev/test_setigen/generated.csv', 'r') as fio:
				gen_csv = csv.DictReader(fio)
				for entry in gen_csv:
					if entry['ID'] == m.group('id'):
						synthDict = entry
						break
			assert(synthDict)
			freqs = re.findall(hyphenDelimRE, synthDict['Freqs'])
			drates = re.findall(hyphenDelimRE, synthDict['Drates'])

		for freq_str in freqs:
			freq_inj = float(freq_str[0:-3])*1e9
			freq_inj = (int(freq_inj/1000))/1000 # round to kHz and scale to MHz
			
			fig = plt.figure()
			# obs.plot_spectrum(f_start=freq_inj-0.0125, f_stop=freq_inj+0.0125) # left sideband
			obs.plot_waterfall(f_start=freq_inj-0.00125, f_stop=freq_inj+0.00125)
			fig.savefig(''.join(filname.split('.')[0:-1])+'_'+freq_str+'.PNG')
			plt.close(fig)


# import os
# import glob
# from sigpyproc.Readers import FilReader
# import matplotlib
# matplotlib.use('Agg')
# import matplotlib.pyplot as plt

# def run_old(argstr, inputs, envvar):
# 	filpaths = [inp for inp in inputs if '.fil' in inp]
# 	if len(inputs) < 1 or len(filpaths) < 1:
# 		print('FilPlotter requires the filterbank path(s) from rawspec\'s output.')
# 		return []
	
# 	for filname in filpaths:
# 		print(filname)
# 		filstem = filname.split('.')[0:-1]
# 		fil = FilReader(filname)

# 		nsamples = fil.header.nsamples//4
# 		print('reading block:', nsamples)
# 		block = fil.readBlock(0, nsamples)

# 		fig = plt.figure()
# 		plt.imshow(block, interpolation='nearest', aspect='auto')
# 		print('saving figure 1')
# 		fig.savefig(''.join(filstem)+'_1.PNG')
# 		print('saved figure 1')

# 		# fig = plt.figure()
# 		# print('summing')
# 		# plt.plot(block.sum(axis=1))
# 		# print('saving figure 2')
# 		# fig.savefig(''.join(filstem)+'_2.PNG')

# 	filplotter_output = glob.glob(os.path.dirname(filpaths[0])+'/*.PNG')
# 	return filplotter_output if filplotter_output is not None else []