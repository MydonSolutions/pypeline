import os
import glob
import redis
from hashpipe_keyvalues.standard import HashpipeKeyValues

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

STATE_hpkv = None

def setup(hostname, instance):
    global STATE_hpkv

    STATE_hpkv = HashpipeKeyValues(
        hostname,
        instance,
        redis.Redis('redishost', decode_responses=True)
    )

def run():
    global STATE_hpkv
    prev_daq = DaqState.Unknown
    current_daq = DaqState.Idle
    while not (prev_daq == DaqState.Record and current_daq == DaqState.Idle):
        prev_daq = current_daq
        current_daq = DaqState.decode_daqstate(STATE_hpkv.get("DAQSTATE"))

    # prev_daq == DaqState.Record and current_daq = DaqState.Idle
    # i.e. recording just completed
    obs_stempath = f'{os.path.join(*STATE_hpkv.observation_stempath)}*'
    output_filepaths = glob.glob(obs_stempath)

    return output_filepaths

if __name__ == "__main__":
    import socket

    setup(socket.gethostname(), 1)
    print(run())
