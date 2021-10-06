import csv
from datetime import datetime
import argparse
import redis
import pandas as pd

PROC_ENV_KEY = None
PROC_ARG_KEY = 'PPLOGARG'
PROC_INP_KEY = 'PPLOGINP'
PROC_NAME = 'log'
PROC_STATUS_KEYS = {'OBSNDROP': None}

def run(argstr, inputs, env):
    parser = argparse.ArgumentParser(description='Log certain info')
    parser.add_argument('-H', type=str, help='The hostname (use $hnme$)')
    parser.add_argument('-i', type=str, required=True,
        help='The instance ID (use $inst$)')
    parser.add_argument('-s', type=str, required=True,
        help='The latest output stem (use $stem$)')
    parser.add_argument('-b', type=float, required=True,
        help='The begin time of the recording (use $beg$)')
    parser.add_argument('-e', type=float, required=True,
        help='The end time of the recording (use $end$)')
    parser.add_argument('-t', nargs='*', type=float,
        help='The durations of each post-processing step (use $time$)')
    parser.add_argument('-p', nargs='*', type=str,
        help='The names of each post-processing step (use $proc$)')

    args = parser.parse_args(argstr.split(' '))
    
    detection_count = -1
    if len(inputs) != 1:
        print('The logger would log the number of detections (the output from postproc_candidate_filter).')
    else:
        candidate_detections = inputs[0] if inputs[0] is not None else [] # pandas.DataFrame
        detection_count = len(candidate_detections) if isinstance(candidate_detections, list) else -1

    logfilepath = '/home/sonata/logs/obs-%s.%s.csv'%(args.H, args.i)
    with open(logfilepath, 'a', newline='') as csvfile:
        csvwr = csv.writer(csvfile, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
        obs_duration = datetime.fromtimestamp(args.e) - datetime.fromtimestamp(args.b)
        postproc_duration = datetime.now() - datetime.fromtimestamp(args.e)
        row = [
            args.s,
            str(datetime.fromtimestamp(args.b)),
            '%0.3f'%(obs_duration.seconds + obs_duration.microseconds/1e6),
            '%0.3f'%(postproc_duration.seconds + postproc_duration.microseconds/1e6),
            str(detection_count),
        ]
        if args.t is not None and args.p is not None:
            row.append(''.join([args.p[i]+'=%0.4f' % args.t[i] for i in range(len(args.t))]))
        for key, val in PROC_STATUS_KEYS.items():
            row.append('{}={}'.format(key, str(val)))
        csvwr.writerow(row)

    return []

if __name__ == '__main__':
    run('-H test -i 0 -s test_stem -b 1.0 -e 2.0', [[1, 2]], None)
    # times = [1.482, 414.123]
    # procs = ['step1', 'step2']
    
    # run('-H testnew -i 0 -s test_stem -b 1.0 -e 2.0 -t {} -p {}'.format(' '.join(map(str, times)), ' '.join(procs)), [[1, 2]], None)
    run('-H testnew -i 0 -s guppi_59366_53197_471033_Unknown_0001 -b 1622558797.2025971 -e 1622558812.1751955 -t 26.484129905700684 0.0019996166229248047 0.03121495246887207 1.5902769565582275 -p RAWSPEC Stem Logger mv rm', ['None'], None)