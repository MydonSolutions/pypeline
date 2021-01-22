# Hpguppi_pypeline

Hpguppi_pypeline aims to provide a framework for pipelining the execution of
modular Python scripts, enabling the creation of a custom post-processing pipeline
for the data captured by a hashpipe (hpguppi_daq) instance. The pipeline typically 
consists of consequetive process calls handled by Python script 'modules', but each
step's Python script could be standalone and not execute any process calls.

## Approach

The main `process_after_obs.py` script is run and monitors the DAQSTATE key of a 
hashpipe's status hash. The change of DAQSTATE's value to and then from 'recording'
is taken as a cue that a recording is completed and the post processing of the recorded
data is to begin.

The post-process pipeline is detailed by a space-delimited list of module names in the 
POSTPROC key*. The module names specify the Python scripts that make up the pipeline. These
scripts are expected to be named `postproc_modulename.p` and be placed alongside
`process_after_obs.py`. The order in which the modules are listed in POSTPROC is the order
in which the scripts are run.

Each module is expected to provide a `run(argstr, inputs, envvar)` function, the arguments of
which are populated by the main script with processed values coming from keys in the redis hash: 

	- `argstr` is captured from the key specified by the module's POSTPROC_ARG_KEY variable. 
		It is not processed/parsed by the main script and is meant to provide static arguments for the
		step's process call. It is a string.
	- `inputs` is produced from a template captured from the key specified by the module's 
		POSTPROC_INP_KEY variable. It **is** processed/parsed by the main script and is meant to provide
		**dynamic** inputs/positional arguments for the step's process call. It is a list of strings, for
		ease of validation.
	- `envstr` is captured from the key specified by the module's POSTPROC_ENV_KEY variable. 
		It is not processed/parsed by the main script and is meant to provide environment variables
		for the step's process call, though the use as such is left up to the module. It is a string.

The values of the ARG/INP/ENV keys are expected to be space-delimited with commas delimiting different
values that are rerun (see Specifying Reiteration).
The processing/parsing of module's INP_KEY enables the specification of the output or input of a
preceding module's run() to be passed to the current module's run():

	- Occurences of 'modulename' are replaced with that module's run() last output.
	- Occurences of '^modulename' are replaced with that module's run() last input.

The return value of a module's run() is captured as its output, and is expected to be a list.
**Subsequent modules are called once per output element**.

Hashpipe recording's file stem is referenced as `hpguppi`.

*This POSTPROC key, and all others mentioned exist within the hashpipe's status redis-hash currently,
but the RedisHash class in 'redishash.py' can easily be modified to target a different redis hash.

## Specifying Reiteration

For each step in the post-process pipeline the input and output are captured. Subsequent modules/steps
can only access these latest values for input and output. Thusly the main script progresses through the
list of modules linearly. Once it reaches the end of the pipeline however, the main script steps
backwards through the pipeline's list of modules to find the last-most module that has an un-run 
input-argument permutation (the envvar is taken to be the same for each call). This is to say that one
can specify that any one of the pipeline's modules/steps is to be run more than once, by delimiting different
templates with a comma. At each iteration through a novel permutation, the pipeline proceeds towards the end
of the list again.

## An Example Showcased by the Value of Modules' Keys

`hashpipe_check_status` is used to set the value of key `-k` to `-s`.

Specify the 'postproc_*' names of the modules to be run in the post-processing, in order
	- `hashpipe_check_status -k POSTPROC -s "rawspec turboseti plotter"`

Specify that the input of rawspec (RWS) is the output of hpguppi
	- `hashpipe_check_status -k PPRWSINP -s "hpguppi"`
Specify the environment variables of the rawspec command
	- `hashpipe_check_status -k PPRWSENV -s "CUDA_VISIBLE_DEVICES=0"`
Specify the static arguments of rawspec
	- `hashpipe_check_status -k PPRWSARG -s "-f 14560 -t 16 -S -d /mnt/buf0/rawspec"`

Specify that the input of turboSETI (TBS) is the output of rawspec
	- `hashpipe_check_status -k PPTBSINP -s "rawspec"`
Specify the environment variables of the turboSETI command
	- `hashpipe_check_status -k PPTBSENV -s "CUDA_VISIBLE_DEVICES=0"`
Specify the static arguments of turboSETI
	- `hashpipe_check_status -k PPTBSARG -s "-M 20 -o /mnt/buf0/turboseti/ -g y -p 12 -n 1440"`

Specify that the input of rawspec (RWS) is the output of turboseti and the input of turboseti
	- `hashpipe_check_status -k PPPLTINP -s "turboseti ^turboseti"`

## Development of a Bespoke Pipeline

Development starts with creating a 'module' in a Python script `postproc_modulename.py`. Setup the names
of the keys required by creating POSTPROC_ARG/INP/ENV_KEY variables (set the value of ARG/ENV_KEY to none if
they are to be ignored). Then create the `run(argstr, inputs, envvar)` function that details the module's
process. Finally ensure that the redis hash has the necessary keys for the module, with appropriate values.

Exemplary modules exist for rawspec and turboSETI as well as some others, within this repository.