#!/usr/bin/env python3
import argparse,multiprocessing,parsl,getpass,socket,json,sys,re,glob,math
from parsl.app.app import python_app,bash_app
from parsl.executors import ThreadPoolExecutor,HighThroughputExecutor
from parsl.providers import LocalProvider,SlurmProvider
from os.path import exists,join,split,splitext,abspath,basename,islink,isdir
from subscripts.utilities import *

class ArgsObject:
    def __init__(self, **entries):
        self.__dict__.update(entries)

if len(sys.argv) == 2 and sys.argv[1] not in ['-h', '--help']:
    config_json = sys.argv[1]
    with open(config_json) as f:
        raw_args = json.load(f)
    args = ArgsObject(**raw_args)
else:
    parser = argparse.ArgumentParser(description='Generate connectome and edge density images',
        usage=""" """)

    # parser.add_argument('--input_dir', '-i', help='', required=True)
    # parser.add_argument('--output_dir', '-o', help='', required=True)
    parser.add_argument('--scheduler_bank', '-b', help='Bank to charge for jobs.', required=True)
    parser.add_argument('--scheduler_partition', '-p', help='Scheduler partition to assign jobs.')
    parser.add_argument('--retries', help='Number of times to retry failed tasks')
    args = parser.parse_args()


pending_args = args.__dict__.copy()
parse_default('retries', 3, args, pending_args)
parse_default('scheduler_partition', 'pbatch', args, pending_args)

cores = int(math.floor(multiprocessing.cpu_count() / 2))
samples = glob(join(args.input_dir, '*'))

# executor = ThreadPoolExecutor()

executor = HighThroughputExecutor(
    label="SeqAnalysisWorker",
    address=address_by_hostname(),
    cores_per_worker=cores,
    provider=SlurmProvider(
        args.scheduler_partition,
        launcher=parsl.launchers.SrunLauncher(),
        nodes_per_block=1,
        init_blocks=1,
        max_blocks=1,
        walltime="00:05:00",
        scheduler_options="#SBATCH --exclusive\n#SBATCH -A {}\n".format(args.scheduler_bank),
        move_files=False,
    ),
)

if __name__ == '__main__':

    config = parsl.config.Config(executors=[executor])
    config.retries = int(args.retries)
    config.checkpoint_mode = 'task_exit'
    parsl.set_stream_logger()
    parsl.load(config)

    @python_app
    def app_random():
        import random
        with open('test.txt', 'w') as f:
            f.write('blah')
        return random.random()

    results =  []
    for i in range(0, 10):
        x = app_random()
        results.append(x)

    for r in results:
        print(r.result())
