#!/usr/bin/env python3
import argparse,multiprocessing,parsl,getpass,socket,json,sys,re,glob,math
from distutils.dir_util import copy_tree
from parsl.app.app import python_app,bash_app
from parsl.executors import ThreadPoolExecutor,HighThroughputExecutor
from parsl.providers import LocalProvider,SlurmProvider
from parsl.addresses import address_by_hostname,address_by_route
from os.path import exists,join,split,splitext,abspath,basename,islink,isdir
from subscripts.utilities import *

class ArgsObject:
    def __init__(self, **entries):
        self.__dict__.update(entries)

if len(sys.argv) == 2 and sys.argv[1] not in ['-h', '--help', '-f', '--force']:
    config_json = sys.argv[1]
    with open(config_json) as f:
        raw_args = json.load(f)
    args = ArgsObject(**raw_args)
else:
    parser = argparse.ArgumentParser(description='Generate connectome and edge density images',
        usage=""" """)

    parser.add_argument('--input_dirs', '-i', help='Path to inputs. Subdirectories must contain subject genomes in FASTQ format.')
    parser.add_argument('--output_dirs', '-o', help='Path to outputs.')
    parser.add_argument('--nnodes', '-n', help='Number of nodes.')
    parser.add_argument('--bank', '-b', help='Bank to charge for jobs.')
    parser.add_argument('--partition', '-p', help='Scheduler partition to assign jobs.')
    parser.add_argument('--retries', help='Number of times to retry failed tasks.')
    parser.add_argument('--force', '-f', help='Overwrite existing outputs.', action='store_true')
    parser.add_argument('--container', help='Path to Singularity container image.')
    parser.add_argument('--walltime', '-t', help='Walltime in format HH:MM:SS.')
    parser.add_argument('--single_subject', help='Run with just a single subject from the input directory.')
    args = parser.parse_args()

pending_args = args.__dict__.copy()
parse_default('input_dirs', 'input/', args, pending_args)
parse_default('output_dirs', 'output/', args, pending_args)
parse_default('bank', 'ncov2019', args, pending_args)
parse_default('partition', 'pbatch', args, pending_args)
parse_default('retries', 0, args, pending_args)
parse_default('force', False, args, pending_args)
parse_default('container', "container/image.sif", args, pending_args)
parse_default('nnodes', 1, args, pending_args)
parse_default('walltime', '11:59:00', args, pending_args)
parse_default('single_subject', '', args, pending_args)

if __name__ == '__main__':

    # Setup V-pipe repo
    smart_remove('tmp')
    smart_mkdir('tmp')
    git_dir = join('tmp', 'vpipe')
    git_params = {'sdir':git_dir, 'container':args.container}
    run(f'git clone https://github.com/cbg-ethz/V-pipe.git {git_dir}', git_params)
    copy_tree('vpipe_files', join(git_dir))
    run(f'sh -c "cd {git_dir} && sh init_project.sh" || true', git_params)

    # executor = ThreadPoolExecutor(label="worker")
    executor = HighThroughputExecutor(
        label="worker",
        address=address_by_hostname(),
        provider=SlurmProvider(
            args.partition,
            launcher=parsl.launchers.SrunLauncher(),
            nodes_per_block=args.nnodes,
            init_blocks=1,
            max_blocks=1,
            worker_init=f"export PYTHONPATH=$PYTHONPATH:{os.getcwd()}",
            walltime=args.walltime,
            scheduler_options="#SBATCH --exclusive\n#SBATCH -A {}\n".format(args.bank),
            move_files=False,
        ),
    )
    params = {
        'container': abspath(args.container),
        'git_dir': abspath(git_dir)
    }

    config = parsl.config.Config(executors=[executor])
    config.retries = int(args.retries)
    config.checkpoint_mode = 'task_exit'
    parsl.set_stream_logger()
    parsl.load(config)

    @python_app(executors=['worker'], cache=True)
    def run_worker(input_dir, output_dir, params):
        import math,multiprocessing,glob
        from os.path import basename,join
        from subscripts.utilities import smart_copy,smart_copy,run

        params['sdir'] = output_dir
        params['stdout'] = join(output_dir, 'worker.stdout')
        smart_copy(params['git_dir'], output_dir)
        for f in glob.glob(join(input_dir, '*.fastq.gz')):
            smart_copy(f, join(output_dir, 'samples/a/b/raw_data', basename(f)))
        
        ncores = int(math.floor(multiprocessing.cpu_count() / 2))
        # ncores = 1
        run(f'sh -c "cd {output_dir} && ./vpipe --cores {ncores}"', params)

    input_dirs = glob(join(args.input_dirs, '*'))
    if args.single_subject != '':
        input_dirs = [join(args.input_dirs, args.single_subject)]
        print(f"WARNING: only running --single_subject {args.single_subject}")
    print(input_dirs)
    exit()
    
    # Assign parallel workers
    results =  []
    for input_dir in input_dirs:
        output_dir = join(args.output_dirs, basename(input_dir))
        if exists(output_dir):
            if not args.force:
                print(f"WARNING: skipping {output_dir} since it already exists. Overwrite with --force.")
                continue
            else:
                smart_remove(output_dir)
        results.append(run_worker(abspath(input_dir), abspath(output_dir), params))

    for r in results:
        r.result()
