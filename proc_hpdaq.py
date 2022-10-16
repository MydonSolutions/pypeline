import os
import glob


PROC_ENV_KEY = None
PROC_ARG_KEY = None
PROC_INP_KEY = None
PROC_NAME = 'hpdaq'
HASHPIPE_STATUS_KEYS = {
    "DAQSTATE": None,
    "observation_stempath": None,
}

class DaqState:
    Unknown = -1
    Idle = 0
    Armed = 1
    Record = 2

    @staticmethod
    def decode_daqstate(daqstate_str):
        if daqstate_str == 'idling':
            return DaqState.Idle
        if daqstate_str == 'armed':
            return DaqState.Armed
        if daqstate_str == 'recording':
            return DaqState.Record
        return DaqState.Unknown

STATE_hpdaq = DaqState.Unknown

def run(argstr, inputs, env):
    global STATE_hpdaq

    current_daq = DaqState.decode_daqstate(HASHPIPE_STATUS_KEYS['DAQSTATE'])
    if not (STATE_hpdaq == DaqState.Record and current_daq == DaqState.Idle):
        STATE_hpdaq = current_daq
        return None

    # STATE_hpdaq == DaqState.Record and current_daq = DaqState.Idle
    # i.e. recording just completed
    obs_stempath = f'{os.path.join(*HASHPIPE_STATUS_KEYS["observation_stempath"])}*'
    output_filepaths = glob.glob(obs_stempath)

    STATE_hpdaq = current_daq
    return output_filepaths

if __name__ == "__main__":
    import socket

    run(
        f'-H {socket.gethostname()} -i {1}',
        None,
        None
    )
