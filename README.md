# Pypeline

Pypeline aims to provide a framework for pipelining the execution of
modular Python scripts, enabling the creation of a custom processing pipeline.
 
`PYTHONPATH=$PWD/test/:$PYTHONPATH pypeline 3 test --log-directory /home/cosmic/dev/logs --redis-hostname redishost --redis-port 6379 -kv "#STAGES=teststage" "TESTINP=test" "TESTARG=just an arg string" "#CONTEXTENV=ENVKEY=ENVVALUE1:ENVVALUE2`

## Approach

The Pypeline is governed by key-value pairs within a [redis hash](https://redislabs.com/ebook/part-1-getting-started/chapter-1-getting-to-know-redis/1-2-what-redis-data-structures-look-like/1-2-4-hashes-in-redis/).
This hash is: `pypeline://${hostname}/${instanceID}/`

The pipeline starts with a contextual stage named under #CONTEXT. When its `run()` produces
outputs, a pipeline process is spawned in a new process to tackle the output.
The stages in the pipeline are determined by the names listed in the #STAGES key's value.
At each stage, the `run()` is called from a python script with a filename related to 
the stage's name, as listed in the #STAGES key's value.

The python script for a stage is able to provide the names of 3 keys whose values will be
pre-processed and passed as argument's to its `run()`: a key for INPUTS, ARGUMENTS
and ENVIRONMENT variables. Because the values of keys are just strings, they are
preprocessed by the primary __pypeline__ script and keywords in the strings are replaced
with dynamic values. In this way the arguments of a stage's `run()` can be the outputs
of a previous stage's `run()`: for instance an INPUT key with the value of `hpguppi`
would be pre-processed to replace `hpguppi` with the single filepath (sans `%d{4}.raw`)
of the latest RAW file recorded (which is the output of the artificial `hpguppi` stage).
The primary script also holds some static values which can be received by each stage's
`run()`.

The INPUT and ARGUMENT keys's values can be comma delimited to provide a number of inputs
that must be `run()` for the related stage. The permutations of {INPUT, ARGUMENT} for each
stage are exhausted by the primary __pypeline__ script.

At the end of each stage the primary stage moves on to the next stage, if there is another
listed in the #STAGES key's value, otherwise it rewinds up the list of stages to the
last stage with an input/argument permutation that has not been processed.

Stages can produce more than one output (each `run()` must return a list). The 
permutations of a stage's input argument is exhaustive combination of the INPUT's
references to stages' outputs (as listed in the value of the stage's PROC_INP_KEY).

Of course, it may be desired that a stage's list of outputs is input all at once, instead
of sequentially. To this end, and a few other ends, there are syntactical markers on the
keywords within INPUT values that adjust the pre-processing applied.

## Pipeline Stages (#STAGES)

The value of the #STAGES key space delimits the list of stage-scripts that make up the
post-processing pipeline. Each stage-name listed names the `stage_stagename.py` script.
It is expected that the stage scripts are in the PYTHONPATH of the executable's 
environment.

## Stage Requirements

Each stage's script is expected to have a `run()` with the following declaration, as
well as the following 4 variables:

```
def run(arg::str, input::list, env::str, logger::logging.Logger=None):
	return outputs::list
```

- ARG_KEY 		: names the key whose value determines the 1st argument for `run()`
- INP_KEY 		: names the key whose value determines the 2nd argument for `run()`
- ENV_KEY 		: names the key whose value determines the 3rd argument for `run()`
- NAME 				: the name of the stage
- *POPENED* 	: (situational) a list of the Popen objects spawned

### Stages Spawning Detached Processes

A convention is in place for stages that spawn detached processes (with
`subprocess.Popen`): the stage's name should end with an ampersand (`&`) and the `Popen`
objects should be collected in the stage's `global POPENED`. With these two pieces in
place, the primary __pypeline__ script will await the termination of a stage's
previous POPENED.


## INPUT Keywords and Modifiers

The values of INPUT keys are preprocessed for the names of previous stages, which are the
only keywords processed. It is assumed that each word (sequence of characters surrounded 
by spaces) is the name of a previous stage. It is possible however, to mark a word as 
'verbatim input' by prefixing the word with `&`, in which case only the ampersand is
removed. Otherwise the occurence of a stage's name is replaced by one of that stage's 
output values (the output values are exhausted across reruns of the referencing stage). To
have a stage's name replaced by its last input, the name can be marked with a prefixed 
`^`. To have the name replaced by all of the stage's outputs (all at once), prefix the
stage's name with `*`.

- `&`: Verbatim input word
- `^`: Input of named-stage's last input
- `*`: Exhaustive input of named-stage's output

Mutliple words in the INPUT value are listed separated by spaces, and multiple input-sets
are separated by commas (`,`).

## ARGUMENT and ENVIRONMENT Keywords

Keywords within the ARGUMENT and ENVIRONMENT keys' values are surrounded by `$`, which
are replaced by values held by the primary script.

- inst: the instanceID that the script is attached to
- hnme: the hostname of the machine
- stem: the stem of the latest RAW dump
- beg: the `time.time()` value produced as DAQSTATE changed to __recording__
- time: the duration (in seconds) of each preceding stage's run as a list of floats
- proc: the PROC_NAME of each preceding stage's run as a list of strings

Mutliple words in the ARGUMENT and ENVIRONMENT values are listed separated by spaces, and
multiple argument-sets are separated by commas (`,`).

## The Test Example

`PYTHONPATH=$PWD/test/:$PYTHONPATH pypeline 3 test --log-directory /home/cosmic/dev/logs --redis-hostname redishost --redis-port 6379 -kv "#STAGES=teststage" "TESTINP=test" "TESTARG=just an arg string" "#CONTEXTENV=ENVKEY=ENVVALUE1:ENVVALUE2`

# Development of a Bespoke Pipeline

Development starts with creating a 'stage' in a Python script `stage_stagename.py`.
Setup the names of the keys required by creating #STAGES_ARG/INP/ENV_KEY variables (set
the value of ARG/ENV_KEY to `None` if they are to be ignored). Then create the 
`run(argstr, inputs, envvar)` function that details the module's process. Finally ensure
that the redis hash has the necessary keys for the module, with appropriate values.

Exemplary modules exist for rawspec and turboSETI as well as some others, within this
repository.