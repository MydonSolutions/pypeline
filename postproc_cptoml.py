import subprocess
import argparse
import os
import glob
import re
import time

PROC_ENV_KEY = None
PROC_ARG_KEY = 'CPTOMLARG'
PROC_INP_KEY = None
PROC_NAME = 'cptoml'

PROC_STATUS_KEYS = {
  'PROJID': None,
  'UVH5OBSP': None,
  'UVH5TELP': None,
}
PROC_LOCAL_KEYWORD_STATUS_KEY_MAP = {
  'proj': 'PROJID'
}

def run(argstr, inputs, env):
  parser = argparse.ArgumentParser(description='Copy the UVH5 toml files to a destination.')
  parser.add_argument('destination', type=str, help='The destination directory for the copy.')
  args = parser.parse_args(argstr.split(' '))

  for (key, status_key) in PROC_LOCAL_KEYWORD_STATUS_KEY_MAP.items():
    args.destination = args.destination.replace('${}$'.format(key), PROC_STATUS_KEYS[status_key])

  dest_dir, dest_filename = os.path.split(os.path.normpath(args.destination))

  cmd = ['mkdir', '-p', dest_dir]
  print(' '.join(cmd))
  subprocess.run(cmd)

  cmd = ['cp', PROC_STATUS_KEYS['UVH5OBSP'], dest_dir]
  print(' '.join(cmd))
  subprocess.run(cmd)
  cmd = ['cp', PROC_STATUS_KEYS['UVH5TELP'], dest_dir]
  print(' '.join(cmd))
  subprocess.run(cmd)
      
  return [PROC_STATUS_KEYS['UVH5OBSP'], PROC_STATUS_KEYS['UVH5TELP']]
