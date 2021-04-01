import subprocess
import os
import re
import csv
from operator import itemgetter

PROC_ENV_KEY = None
PROC_ARG_KEY = 'PPSGVARG'
PROC_INP_KEY = 'PPSGVINP'
PROC_NAME = 'setigen_validate'

def run(argstr, inputs, envvar):
	if len(inputs) < 1:
		print('Plotter requires both the stem and plotter\'s outputs.')
		return []
	stemOutputs = [inp for inp in inputs if '.png' not in inp]
	assert(len(stemOutputs)==1)
	plotterOutputs = [inp for inp in inputs if '.png' in inp]

	hyphenDelimRE = r'(?P<val>-?[^-]+)-?'
	m = re.match(r'.*sy_(?P<id>\d+)', stemOutputs[0])
	if m:
		print('ID:', m.group('id'))
		with open('/home/sonata/dev/test_setigen/generated.csv', 'r') as fio:
			gen_csv = csv.DictReader(fio)
			for entry in gen_csv:
				if entry['ID'] == m.group('id'):
					synthDict = entry
					break
		assert(synthDict)
		freqs = re.findall(hyphenDelimRE, synthDict['Freqs'])
		drates = re.findall(hyphenDelimRE, synthDict['Drates'])

	else:# must be older file...
		print('must be older file...')
		m = re.match(r'.*sy_N(?P<nant>\d+)_(?P<siglevel>[^_]*)_(?P<bits>\d+)b_(?P<freqs>[^_]*)_(?P<drates>.*)', stemOutputs[0])
		assert m is not None

		freqs = re.findall(hyphenDelimRE, m.group('freqs'))
		drates = re.findall(hyphenDelimRE, m.group('drates'))
		drates = [d[0:-4] for d in drates]
		print('drates', drates)

	# Collect generated signals in list
	signals = []
	for sigI in range(len(freqs)):
		signals.append({'freq':float(freqs[sigI][0:-3])*1000, 'drate':float(drates[sigI])})

	# Collect detected signals in list
	detections = []
	plotterOutputRE = r'_dr_(?P<drate>-?[^-]+)_freq_(?P<freq>.*).png'
	for pngfile in plotterOutputs:
		m = re.search(plotterOutputRE, pngfile)
		if m is not None:
			detections.append({'freq':float(m.group('freq')), 'drate':float(m.group('drate'))})

	# Sort the lists
	signals.sort(key=itemgetter('freq'), reverse=False)
	detections.sort(key=itemgetter('freq'), reverse=False)

	# Pad the lists
	while (len(detections) < len(signals)):
		detections.append({'freq':'', 'drate':''})
	while (len(detections) > len(signals)):
		signals.append({'freq':'', 'drate':''})

	# Formatted print the lists
	format_row = '{:>20}' * (4+1)
	print(format_row.format(*['-'*20 for i in range(4+1)]))
	print(format_row.format('Signal (MHz)', 'Drift Rate (Hz/s)', '||', 'Detected (MHz)', 'Drift Rate (Hz/s)'))
	for i in range(len(detections)):
		print(format_row.format(signals[i]['freq'], signals[i]['drate'], '||', detections[i]['freq'], detections[i]['drate']))
	print(format_row.format(*['-'*20 for i in range(4+1)]))
	
	return []

if __name__== "__main__":
	# run(None, ['/mnt/buf0/setigen_raw/././sy_N3_0.006_4b_6.1022GHz-6.1032GHz_-2.0Hzps-4.0Hzps'], None)
	# run(None, ['/mnt/buf0/setigen_raw/././sy_0'], None)
	run(None, ['/mnt/buf0/rawspec_setigen/sy_7/sy_7-ics.rawspec.0000.fil', '1_SYNTHETIC_dr_-2.40_freq_6175.124748.png', '1_SYNTHETIC_dr_5.28_freq_6176.437252.png', '1_SYNTHETIC_dr_-2.40_freq_6175.374748.png'], None)