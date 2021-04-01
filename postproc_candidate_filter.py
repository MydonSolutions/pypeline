from turbo_seti.find_event.find_event_pipeline import find_event
from turbo_seti.find_event.plot_event_pipeline import plot_event
import argparse
import os
import glob

PROC_ENV_KEY = None
PROC_ARG_KEY = 'PPCNDARG'
PROC_INP_KEY = 'PPCNDINP'
PROC_NAME = 'candidate_filter&plotter'

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

	cands = find_event.find_events([dat_filepath], args.snr_thresh, False, args.rfi_filter)
	if cands is not None and not args.no_plot:
		plot_event.plot_candidate_events(cands, [fil_filepath], str(args.rfi_filter), args.source_name, args.offset)
	return [cands] if cands is not None else [ None ]
	# plotter_output = glob.glob(os.path.dirname(fil_filepath)+'/*.png')

	# return plotter_output if plotter_output is not None else []

if __name__ == '__main__':
	print(run('-P', ['/mnt/buf0/rawspec_setigen/sy_15/sy_15-ics.rawspec.0000.fil', '/mnt/buf0/turboseti_setigen/sy_15/sy_15-ics.rawspec.0000.dat'], None))