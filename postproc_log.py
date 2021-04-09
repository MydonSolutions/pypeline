import csv
from datetime import datetime
import argparse
import pandas as pd

PROC_ENV_KEY = None
PROC_ARG_KEY = 'PPLOGARG'
PROC_INP_KEY = 'PPLOGINP'
PROC_NAME = 'log'

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

    args = parser.parse_args(argstr.split(' '))
    
    detection_count = -1
    if len(inputs) != 1:
        print('The logger would log the number of detections (the output from postproc_candidate_filter).')
    else:
        candidate_detections = inputs[0] if inputs[0] is not None else [] # pandas.DataFrame
        detection_count = len(candidate_detections)
    
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
        csvwr.writerow(row)

    return []

if __name__ == '__main__':
    run('-H test -i 0 -s test_stem -b 1.0 -e 2.0', [[1, 2]], None)