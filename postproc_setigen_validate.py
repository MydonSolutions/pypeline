import subprocess
import os
import re
from operator import itemgetter

PROC_ENV_KEY = None
PROC_ARG_KEY = 'PPSGVARG'
PROC_INP_KEY = 'PPSGVINP'
PROC_NAME = 'setigen_validate'

def run(argstr, inputs, envvar):
	if len(inputs) < 1:
		print('Plotter requires only plotter\'s output.')
		return []
	stemOutputs = [inp for inp in inputs if '.png' not in inp]
	plotterOutputs = [inp for inp in inputs if '.png' in inp]
	print(stemOutputs)
	print(plotterOutputs)

	# Assume the RAW stem is in argstr
	m = re.match(r'.*sy(?P<bits>\d+)b_(?P<freqs>[^_]*)_(?P<drates>.*)', stemOutputs[0])

	hyphenDelimRE = r'(?P<val>-?[^-]+)-?'

	freqs = re.findall(hyphenDelimRE, m.group('freqs'))
	drates = re.findall(hyphenDelimRE, m.group('drates'))

	signals = []
	for sigI in range(len(freqs)):
		signals.append({'freq':float(freqs[sigI][0:-4])*1000, 'drate':float(drates[sigI][0:-5])})

	detections = []
	plotterOutputRE = r'_dr_(?P<drate>-?[^-]+)_freq_(?P<freq>.*).png'
	for pngfile in plotterOutputs:
		m = re.search(plotterOutputRE, pngfile)
		if m is not None:
			detections.append({'freq':float(m.group('freq')), 'drate':float(m.group('drate'))})

	signals.sort(key=itemgetter('freq'), reverse=False)
	detections.sort(key=itemgetter('freq'), reverse=False)

	while (len(detections) < len(signals)):
		detections.append({'freq':'', 'drate':''})
	while (len(detections) > len(signals)):
		signals.append({'freq':'', 'drate':''})


	format_row = '{:>20}' * (4+1)
	print(format_row.format(*['-'*20 for i in range(4+1)]))
	print(format_row.format('Signal (MHz)', 'Drift Rate (Hz/s)', '||', 'Detected (MHz)', 'Drift Rate (Hz/s)'))
	for i in range(len(detections)):

		print(format_row.format(signals[i]['freq'], signals[i]['drate'], '||', detections[i]['freq'], detections[i]['drate']))
	print(format_row.format(*['-'*20 for i in range(4+1)]))
	
	return []