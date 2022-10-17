import subprocess
import argparse
import os

PROC_ENV_KEY = None
PROC_ARG_KEY = 'CPTOMLARG'
PROC_INP_KEY = None
PROC_NAME = 'cptoml'

PROC_CONTEXT = {
  'PROJID': None,
  'UVH5OBSP': None,
  'UVH5TELP': None,
}

def run(argstr, inputs, env):
  parser = argparse.ArgumentParser(description='Copy the UVH5 toml files to a destination.')
  parser.add_argument('destination', type=str, help='The destination directory for the copy.')
  args = parser.parse_args(argstr.split(' '))

  for (key, status_key) in {
    'proj': 'PROJID'
  }.items():
    args.destination = args.destination.replace('${}$'.format(key), PROC_CONTEXT[status_key])

  dest_dir, dest_filename = os.path.split(os.path.normpath(args.destination))

  cmd = ['mkdir', '-p', dest_dir]
  print(' '.join(cmd))
  subprocess.run(cmd)

  cmd = ['cp', PROC_CONTEXT['UVH5OBSP'], dest_dir]
  print(' '.join(cmd))
  subprocess.run(cmd)
  cmd = ['cp', PROC_CONTEXT['UVH5TELP'], dest_dir]
  print(' '.join(cmd))
  subprocess.run(cmd)
      
  return [PROC_CONTEXT['UVH5OBSP'], PROC_CONTEXT['UVH5TELP']]
