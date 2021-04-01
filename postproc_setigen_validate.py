import subprocess
import os
import re
import csv
from operator import itemgetter
import numpy as np
import pandas as pd

PROC_ENV_KEY = None
PROC_ARG_KEY = 'PPSGVARG'
PROC_INP_KEY = 'PPSGVINP'
PROC_NAME = 'setigen_validate'

def run(argstr, inputs, envvar):
	if len(inputs) != 1:
		print('Setigen Detection Validation requires only the output from postproc_plotter.')
		return []
	candidate_detections = inputs[0]
	if argstr is None:
		print('Setigen Detection Validation expects the stem in argstr (use $stem$).')
	
	hyphenDelimRE = r'(?P<val>-?[^-]+)-?'
	m = re.match(r'.*sy_(?P<id>\d+)', argstr)
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
	else:
		print('No match found for the ID in the dat filepath:', argstr)
		return []

	# Collect generated signals in list
	signals = []
	for sigI in range(len(freqs)):
		signals.append({'freq':float(freqs[sigI][0:-3])*1000, 'drate':float(drates[sigI])})

	# Sort the lists
	signals.sort(key=itemgetter('freq'), reverse=False)
	candidate_detections = candidate_detections.sort_values(by=['FreqStart'], ascending=True)

	# Formatted print the lists
	format_row = '{:>20}' * (2+1+3)
	print(format_row.format(*['-'*20 for i in range(2+1+3)]))
	print(format_row.format('Signal (MHz)', 'Drift Rate (Hz/s)', '||', 'Detected (MHz)', 'Drift Rate (Hz/s)', 'SNR'))
	for i in range(max(len(candidate_detections), len(signals))):
		
		injectionPart = ['', '']
		if i < len(signals):
			injectionPart = [signals[i]['freq'], signals[i]['drate']]
		detectionPart = ['', '', '']
		if i < len(candidate_detections.index):
			rowid = candidate_detections.index[i]
			detectionPart = [candidate_detections.loc[rowid, 'FreqStart'], candidate_detections.loc[rowid, 'DriftRate'], candidate_detections.loc[rowid, 'SNR']]
		
		print(format_row.format(*injectionPart, '||', *detectionPart))
	print(format_row.format(*['-'*20 for i in range(2+1+3)]))
	
	return []

if __name__== "__main__":
	import postproc_plotter as candi
	from string import Template

	fil_filepath = Template('/mnt/buf0/rawspec_setigen/${stem}_old/${stem}-ics.rawspec.0000.fil')
	dat_filepath = Template('/mnt/buf0/turboseti_setigen/${stem}/${stem}-ics.rawspec.0000.dat')

	for stem in ['sy_15']:#, 'sy_14', 'sy_15']:
		run(stem, 
			candi.run('-P', [fil_filepath.substitute(stem=stem), dat_filepath.substitute(stem=stem)], None),
			None)