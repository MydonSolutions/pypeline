from turbo_seti.find_event.find_event_pipeline import find_event
from turbo_seti.find_event.plot_event_pipeline import plot_event
import argparse
import os
import glob

PROC_ENV_KEY = None
PROC_ARG_KEY = 'CANDIARG'
PROC_INP_KEY = 'CANDIINP'
PROC_NAME = 'candifilplot'

def run(argstr, inputs, envvar):
	if len(inputs) < 2:
		print('Plotter requires a two input paths, the turbo seti output and rawspec\'s output.')
		return []

	dat_filepath = [inp for inp in inputs if '.dat' in inp]
	if len(dat_filepath) > 2:
		print('Plotter requires a two input paths, the turbo seti output and rawspec\'s output. Ignoring {}'.format(dat_filepath[1:]))
	dat_filepath = dat_filepath[0]
	print(dat_filepath)

	fil_filepath = [inp for inp in inputs if '.fil' in inp]
	if len(fil_filepath) > 2:
		print('Plotter requires a two input paths, the turbo seti output and rawspec\'s output. Ignoring {}'.format(fil_filepath[1:]))
	fil_filepath = fil_filepath[0]
	print(fil_filepath)

	parser = argparse.ArgumentParser(description='Filter turboseti dat and plot candidates from filterbank file.')
	parser.add_argument('-r', '--rfi_filter', type=float,
		default=1, help='rfi-filter find_events argument [1]')
	parser.add_argument('-s', '--snr_thresh', type=float,
		default=10, help='SNR threshold find_events argument [10]')
	parser.add_argument('-o', '--offset', type=str,
		default='auto', help='offset plot_candidate_events argument ["auto"]')
	parser.add_argument('-n', '--source_name', type=str,
		default='bla', help='source_name plot_candidate_events argument ["bla"]')
	parser.add_argument('-P', '--no_plot', action='store_true',
		help='Don\'t plot the candidates, in the filterbank file\'s directory [False]')
	
	args = parser.parse_args(argstr.split(' ') if argstr is not None else '')

	candidates = find_event.find_events([dat_filepath], args.snr_thresh, False, args.rfi_filter)
	if candidates is not None and not args.no_plot:
		plot_event.plot_candidate_events(candidates, [fil_filepath], str(args.rfi_filter), args.source_name, args.offset)

	if candidates is not None:
		candidates = candidates.sort_values(by=['FreqStart'], ascending=True)

		# Formatted log the lists
		format_row = '{:>20}' * (3) + '\n'
		log_filepath = os.path.join(os.path.dirname(fil_filepath), 'candidates.txt')
		print('Writing summary of %d candidate(s) to %s'%(len(candidates), log_filepath))
		with open(log_filepath, 'w') as fio:	
			fio.write(format_row.format(*['-'*20 for i in range(3)]))
			fio.write(format_row.format('Detected (MHz)', 'Drift Rate (Hz/s)', 'SNR'))
			fio.write(format_row.format(*['-'*20 for i in range(3)]))
			for i in range(len(candidates)):
				rowid = candidates.index[i]
				detectionPart = [candidates.loc[rowid, 'FreqStart'], candidates.loc[rowid, 'DriftRate'], candidates.loc[rowid, 'SNR']]
				fio.write(format_row.format(*detectionPart))
			fio.write(format_row.format(*['-'*20 for i in range(3)]))

	return [candidates] if candidates is not None else [ None ]
	# plotter_output = glob.glob(os.path.dirname(fil_filepath)+'/*.png')

	# return plotter_output if plotter_output is not None else []

if __name__ == '__main__':
	print(run('-P', ['/mnt/buf0/rawspec_setigen/sy_15/sy_15-ics.rawspec.0000.fil', '/mnt/buf0/turboseti_setigen/sy_15/sy_15-ics.rawspec.0000.dat'], None))