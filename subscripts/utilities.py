import time,datetime,os,subprocess,sys,shutil,hashlib,grp,mmap,fnmatch,gzip,re
from glob import glob
from os.path import exists,join,split,splitext,abspath,basename,dirname,isdir,samefile,getsize
from shutil import copyfile,copytree,rmtree,ignore_patterns
from os import system,environ,makedirs,remove
from subprocess import Popen,PIPE
from itertools import islice

def smart_mkdir(path):
    if exists(path):
        return
    if not isdir(path):
        makedirs(path)

def smart_remove(path):
    """Remove all files and directories if they exist
    """
    if isdir(path):
        rmtree(path)
    elif exists(path):
        try:
            remove(path)
        except OSError:
            pass

def smart_copy(src, dest, exclude=[]):
    """Copy file or directory, while ignoreing non-existent or equivalent files
    """
    if not exists(src):
        print("ERROR: Cannot find file to copy: {}".format(src))
        return
        # raise Exception("Cannot find file to copy: {}".format(src))
    if exists(dest) and samefile(src, dest):
        print("Warning: ignoring smart_copy because src and dest both point to {}".format(dest))
        return
    smart_remove(dest)
    if not exists(dirname(dest)):
        smart_mkdir(dirname(dest))
    if isdir(src):
        copytree(src, dest, ignore=ignore_patterns(*exclude))
    else:
        for pattern in exclude:
            if fnmatch.fnmatch(src, pattern):
                print('Did not copy {} because of exclude={}'.format(src, exclude))
                return
        copyfile(src, dest)

def exist_all(paths):
    for path in paths:
        if not exists(path):
            return False
    return True

def run(command, params=None, ignore_errors=False, print_output=True, print_time=False, working_dir=None):
    """Run a command in a subprocess.
    Safer than raw execution. Can also write to logs and utilize a container.
    """
    start = int(time.time())
    stdout = params['stdout'] if (params and 'stdout' in params) else None
    container = params['container'] if (params and 'container' in params) else None
    use_gpu = params['use_gpu'] if (params and 'use_gpu' in params) else None
    sdir = params['sdir'] if (params and 'sdir' in params) else None
    container_cwd = params['container_cwd'] if (params and 'container_cwd' in params) else None

    # When using a container, change all paths to be relative to its mounted directory (hideous, but works without changing other code)
    if container is not None:
        odir = split(sdir)[0]
        command = command.replace(odir, "/mnt")
        command = 'singularity exec{} -B {}:/mnt {} sh -c "{}"'.format(" --nv" if use_gpu else "", odir, container, command)
        print(command)
        if container_cwd:
            command = "cd {}; {}".format(container_cwd, command)
        if stdout:
            write(stdout, command)

    process = Popen(command, stdout=PIPE, stderr=subprocess.STDOUT, shell=True, env=environ, cwd=working_dir)
    line = ""
    while True:
        new_line = process.stdout.readline()
        new_line = str(new_line, 'utf-8')[:-1]
        if print_output and new_line:
            print(new_line)
        if stdout and not new_line.isspace():
            write(stdout, new_line)
        if new_line == '' and process.poll() is not None:
            break
        line = new_line
    if process.returncode != 0 and not ignore_errors:
        if stdout and not new_line.isspace():
            write(stdout, "Error: non zero return code")
            write(stdout, get_time_date())
        raise Exception("Non zero return code: {}\nCommand: {}".format(process.returncode, command))
    else:
        tokens = command.split(' ')
        if print_time:
            print("Running {} took {} (h:m:s)".format(tokens[0], get_time_string(int(time.time()) - start, params)))
            if len(tokens) > 1:
                print("\tArgs: {}".format(' '.join(tokens[1:])))
    return line  # return the last output line

def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def is_integer(value):
    try:
        int(value)
        return True
    except ValueError:
        return False

def clamp(value, lo, hi):
    return max(lo, min(value, hi))

def str2bool(string):
    if string is None:
        return string
    return str(string).lower() in ("yes", "true", "t", "1")

def get_time_date():
    """Returns date and time as Y-m-d H:M am/pm
    """
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M %p")

def get_time_string(seconds, params=None):
    """Returns seconds as Slurm-compatible time string
    """
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    time_string = "{:02d}:{:02d}:{:02d}".format(int(h), int(m), int(s))
    if h > 99999 or h < 0:
        if params and 'stdout' in params:
            write(params['stdout'], "Error: Invalid time string {}".format(time_string))
        return "00:00:00"
    return time_string

def get_time_seconds(string):
    """Returns Slurm-compatible time string as seconds
    """
    while (len(string.split(":")) < 3):
        string = "00:" + string
    return sum(secs * int(digit) for secs,digit in zip([3600, 60, 1], string.split(":")))

def get_start(function_name):
    return "Starting {} at {}\n".format(basename(str(function_name)), get_time_date())

def get_finish(function_name):
    return "Finished {} at {}\n".format(basename(str(function_name)), get_time_date())

def print_start():
    print(get_start())
    return time.time()

def print_finish(start_time):
    print(get_finish())

def write(path, output, params={}):
    if params and 'container' in params and 'sdir' in params:
        odir = split(params['sdir'])[0]
        output = output.replace(odir, "/share")
    # make path to file if not an empty string
    if dirname(path):
        smart_mkdir(dirname(path))
    with open(path, 'a') as f:
        f.write(str(output) + "\n")

def write_error(path, output, params={}):
    write(path, 'Exception: ' + output, params)
    raise output

def record_start(params):
    """Record step start in timing log, and write to stdout.
    """
    step = params['step']
    timing_log = params['timing_log']
    stdout = params['stdout']
    with open(stdout, 'a') as f:
        f.write("\n=====================================\n")
        f.write(get_start(step))
        f.write("---------------------------------------\n")
        f.write("Parameters:\n")
        for k, v in params.items():
            f.write(str(k) + ': ' + str(v) + '\n')
        f.write("=====================================\n\n")
    write(timing_log, "{} start".format(time.time()))

def record_apptime(params, app_start_time, substep, *args):
    """Record substep duration in timing log.
    """
    timing_log = params['timing_log']
    apptime = time.time() - app_start_time
    line = "{} {}".format(apptime, substep)
    for arg in args:
        line += ' ' + str(arg) # save additional args for debug
    write(timing_log, line)

def record_finish(params):
    """Record cumulative step duration from timing log, and write to stdout.
    """
    sdir = params['sdir']
    step = params['step']
    timing_log = params['timing_log']
    stdout = params['stdout']
    cores_per_task = int(params['cores_per_task'])
    use_gpu = params['use_gpu']
    global_timing_log = params['global_timing_log']
    sname = params['sname']
    task_start_time = -1
    task_total_time = 0
    max_apptimes = {}
    with open(timing_log, 'r') as f:
        for line in f.readlines():
            chunks = [x.strip() for x in line.strip().split() if x]
            if len(chunks) < 2 or not is_float(chunks[0]):
                write(stdout, 'Invalid time value in line: {}'.format(line))
                continue
            apptime = float(chunks[0])
            if chunks[1] == 'start':
                task_start_time = apptime
                continue
            if not is_integer(chunks[1]):
                write(stdout, 'Invalid substep in line: {}'.format(line))
                continue
            substep = int(chunks[1])
            task_total_time += apptime
            if substep not in max_apptimes:
                max_apptimes[substep] = apptime
            else:
                max_apptimes[substep] = max(apptime, max_apptimes[substep])
    if task_start_time == -1:
        write(stdout, 'Failed to find valid start time')
    ideal_walltime = get_time_string(sum(list(max_apptimes.values())), params) # find longest apptimes in same substep
    actual_walltime = get_time_string(time.time() - task_start_time, params)
    total_core_time = get_time_string(task_total_time * cores_per_task, params) # sum of apptimes, multiplied by cores
    with open(stdout, 'a') as f:
        f.write("\n=====================================\n")
        f.write(get_finish(step))
        f.write("Ideal walltime: {} (h:m:s)\n".format(ideal_walltime))
        f.write("Actual walltime: {} (h:m:s)\n".format(actual_walltime))
        f.write("Total core time: {} (h:m:s)\n".format(total_core_time))
        f.write("{} parallel cores per task\n".format(cores_per_task))
        f.write("Used GPU: {}\n".format(use_gpu))
        f.write("=====================================\n\n")
        f.write("stdout_log_complete")
    write(global_timing_log, "{},{},{},{},{},{}".format(sname, step, ideal_walltime, actual_walltime, total_core_time, use_gpu))
    run("chmod 770 {}".format(timing_log))
    run("chmod 770 {}".format(stdout))

def update_permissions(params):
    """Give user and group permissions to all generated files.
    """
    start_time = time.time()
    sdir = params['sdir']
    bids_dicom_dir = params['bids_dicom_dir']
    bids_nifti_dir = params['bids_nifti_dir']
    group = params['group']
    stdout = params['stdout']
    for directory in [bids_dicom_dir, bids_nifti_dir, sdir]:
        run("find {} -type f -print0 | xargs -0 -I _ chmod 770 _".format(directory))
        run("find {} -type d -print0 | xargs -0 -I _ chmod 2770 _".format(directory))
        if params['group']:
            run("find {} -type f -print0 | xargs -0 -I _ chgrp {} _".format(directory, group))
            run("find {} -type d -print0 | xargs -0 -I _ chgrp {} _".format(directory, group))
    write(stdout, "Updated file permissions, took {} (h:m:s)".format(get_time_string(time.time() - start_time, params)))

def generate_checksum(input_dir):
    """Return checksum of subject input files. This ensures re-computation if inputs change.
    """
    buf_size = 65536  # read file in 64kb chunks
    md5 = hashlib.md5()
    for fname in ['anat.nii.gz','bvals','bvecs','hardi.nii.gz']:
        f = open(join(input_dir, fname), 'rb')
        while True:
            data = f.read(buf_size)
            if not data:
                break
            md5.update(data)
        f.close()
    return md5.hexdigest()

def get_log_path(template):
    """Return paths for log outputs.
    """
    path, ext = splitext(template)
    idx = 0
    new_log = path + "_{:02d}".format(idx) + ext
    while exists(new_log):
        idx += 1
        if idx >= 100:
            raise Exception("Could not find valid filepath for template {}".format(template))
        new_log = path + "_{:02d}".format(idx) + ext
    prev_log = path + "_{:02d}".format(idx-1) + ext
    return new_log, prev_log, idx

def is_log_complete(path):
    if exists(path):
        with open(path, 'rb', 0) as f, mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as s:
            return s.find(b'stdout_log_complete') != -1

def running_step(steps, *argv):
    """Return true if running any step in arguments.
    """
    for step in argv:
        if step in steps:
            return True
    return False

def copy_dir(src, dest):
    if not isdir(src):
        raise Exception("Source directory {} does not exist".format(src))
    smart_mkdir(dest)
    run("cp -Rf {} {}".format(join(src,"."), join(dest,".")))

def parse_default(arg, default, args_obj, pending_args):
    if not hasattr(args_obj, arg) or getattr(args_obj, arg) is None:
        setattr(args_obj, arg, default)
    if isinstance(default, bool):
        setattr(args_obj, arg, str2bool(getattr(args_obj, arg)))
    elif isinstance(default, str):
        if str(getattr(args_obj, arg)).lower() == "none":
            setattr(args_obj, arg, None)
    pending_args.pop(arg, None)

def add_binary_vol(src, target, params={}):
    run("fslmaths {} -add {} {}".format(src, target, target), params)
    run("fslmaths {} -bin {}".format(target, target), params)

def sub_binary_vol(src, target, params={}):
    intersection = "intersection.nii.gz"
    run("fslmaths {} -mul {} {}".format(src, target, intersection), params)
    run("fslmaths {} -sub {} {}".format(target, intersection, target), params)
    run("fslmaths {} -bin {}".format(target, target), params)

def strip_trailing_slash(path):
    if path.endswith('/') or path.endswith('\\'):
        path = path[:-1]
    return path

def get_edges_from_file(file):
    edges = []
    with open(file) as f:
        for edge in f.readlines():
            if edge.isspace():
                continue
            edges.append(edge.replace("_s2fa", "").strip().split(',', 1))
    return edges

def generate_edge_list(vol_dir, path='lists/listEdgesEDIAll.txt'):
    """Not used during runtime. Generates a list of all possible edges from Freesurfer output.
    """
    with open(path,'w') as f:
        files = glob(join(vol_dir,"*_s2fa.nii.gz"))  # Assemble all files
        files = [abspath(vol) for vol in files]
        for a in files:
            for b in files:
                if a != b:
                    a1 = splitext(splitext(split(a)[1])[0])[0]
                    b1 = splitext(splitext(split(b)[1])[0])[0]
                    f.write("{},{}\n".format(basename(a1),basename(b1)))

def compress_file(file):
    compressed_file = file + '.gz'
    with open(file, 'rb') as f_in:
        with gzip.open(compressed_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    return compressed_file

def get_bids_subject_name(sname):
    sname = sname.replace('sub-', '') # make naming consistent
    regex = re.compile('[^a-zA-Z0-9]')
    return 'sub-{}'.format(regex.sub('', sname))

def validate(file, params={}):
    file = file.strip()
    if not file.endswith('.nii.gz'):
        file = file + '.nii.gz'
    mean = run("fslstats {} -m | head -n 1".format(file), params)
    assert is_float(mean), "Invalid mean value in {}".format(file)
    assert float(mean) != 0, "Zero mean value in {}".format(file)

def append_to_filename(filename, append_string):
    name, ext = os.path.splitext(filename)
    return "{}_{}{}".format(name, append_string, ext)

def deinterlace(sequence_file, forward_file, reverse_file):

    if sequence_file.endswith('.gz'):
        run(f"gzip -k -d {sequence_file}")
        sequence_file = sequence_file[:-3]

    smart_remove(forward_file)
    smart_remove(reverse_file)
    smart_remove(forward_file + '.gz')
    smart_remove(reverse_file + '.gz')

    i = 0
    forward = True
    file_size = getsize(sequence_file)
    chunk_size = 0
    with open(sequence_file) as f1:
        while True:
            lines = list(islice(f1, 4))
            if not lines:
                break
            with open(forward_file if forward else reverse_file, 'a') as f2:
                f2.write(''.join(lines))
            i += 1
            forward = not forward

            if chunk_size == 0:
                chunk_size = len(''.join(lines).encode('ascii')) - 1
            if i % 1000 == 0:
                print("Deinterlacing {}, {:.2%}...".format(sequence_file, chunk_size * i / file_size))
    
    run(f"gzip {forward_file}")
    run(f"gzip {reverse_file}")

    smart_remove(sequence_file)
            