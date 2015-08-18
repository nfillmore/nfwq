# What is it?

NFWQ is a simple system for managing scientific computing workflows.


# Dependencies

- Python 3.
- SQLite 3.
- Cooperative Computing Tools (cctools; see below for installation).
- Condor (optional).


# Installation

First, install cctools. I have most frequently used version 4.1.2 (via git,
91752b78a259cf392a44f1461a86eb05138cdad1). Make sure to include python3
support, using something like this:

    ./configure --with-python3-path $(dirname $(dirname $(which python3))) --prefix /path/to/install/cctools
    make
    make install

(You may also need --with-swig-path if your system's version of SWIG is too old.)

Add cctools to your PATH and PYTHONPATH using something like this:

    export PATH=$PATH:/path/to/install/cctools/bin
    export PYTHONPATH=$PYTHONPATH:/path/to/install/cctools/lib/python3.3/site-packages

Second, install NFWQ itself by moving the current directory somewhere, say
/path/to/nfwq_stuff, and adding it to your PATH and PYTHONPATH:

    export PATH=$PATH:/path/to/nfwq_stuff
    export PYTHONPATH=$PYTHONPATH:/path/to/nfwq_stuff


# Example usage

In one shell:

    $ mkdir /scratch/nathanae/test_dag
    $ python3 make_test_dag.py --workdir /scratch/nathanae/test_dag
    $ nfwq start_master --db /scratch/nathanae/test_dag/test_dag.db --port 9140

In another shell:

    $ nfwq refresh_master --db /scratch/nathanae/test_dag/test_dag.db
    $ nfwq start_condor_worker --server $(hostname) --port 9140 --num_cpu 1 --num_processes 5

# Detailed info

NFWQ has three components, (1) a Python API for making workflows, (2) the
`nfwq` program for running and managing the workflow, and (3) a Python API for
use by jobs.

## (1) Python API for making workflows

The Python API for making workflows centers on the `nfwq.Dag` class, with the following constructor and methods

- constructor: Takes one argument, the path where the workflow DAG should be
  stored.

- `init`: Creates the workflow DAG. Takes no arguments. If the DAG already
  exists and you just want to add jobs to it, don't call `init`.

- `add`: Adds a job to the workflow DAG. Its signature is as follows:

    `add(self, cmd, tag, parents=None, algorithm=None, preferred_host=None, files=None, dirs=None, bufs=None, io=None, cores=None, memory=None, disk=None)`

  where

  - `cmd` is a list specifying the command to run, e.g., `["ls", "-ltr", "mydir"]`.

  - `tag` is a unique id for the job.

  - `parents` is a list of tags corresponding to jobs that need to complete
    successfully before this job can run.

  - `files` is a list of input or output files for the job. Each input job
    should be specified using `nfwq.input_file(local_name, remote_name=None,
    cache=False)`, where:

    - `local_name` is the name on the master,
    - `remote_name` is the name on the worker (same as
      `os.path.basename(local_name)` if not specified),
    - `cache` specifies whether the file should be cached on the worker.
    
    Each output job should be specified using `nfwq.output_file(local_name,
    remote_name=None, cache=False)`, where the arguments have the same
    interpretation as for `nfwq.input_file`.
  
  - `dirs` is a list of input or output directories for the job. Each input or
    output job should be specified using
      `nfwq.input_dir(local_name, remote_name=None, cache=False, recursive=True)` or
      `nfwq.output_dir(local_name, remote_name=None, cache=False, recursive=True)`,
    respectively, where the first three arguments have the same interpretation
    as for `nfwq.input_file`, and the directory is copied recursively if
    recursive is `True`.

  - `bufs` is a list of input buffers for the job. Each input buffer should be
    specified using `nfwq.input_buf(buffer, remote_name, cache=False)`, where:

    - `buffer` is the contents of the buffer,
    - `remote_name` is the file name on the worker where the contents should be
      stored,
    - `cache` specifies whether the file containing the buffer should be cached
      on the worker.

  - `algorithm`, `preferred_host`, `cores`, `memory`, and `disk` are used to
    specify the corresponding attributes of the cctools work_queue task created
    to run this job. See work_queue's documentation for more information.

## (2) The `nfwq` program for running and managing the workflow

The `nfwq` program has the following commands, run with `nfwq {command}`. See
`nfwq {command} --help` for more information.

- `start_master` - Start the server. For example,
  `nfwq start_master --db test_dag.db --port 9140`

- `refresh_master` - Refresh the server. This needs to be used after update_state or retry_*. For example,
  `nfwq refresh_master --db test_dag.db`

- `get_state` - Get information about the state of the workflow. This command has
  many options, for example:

  - To see a basic summary:
    `nfwq get_state --db test_dag.db`

  - To see failed jobs:
    `nfwq get_state --db test_dag.db --state failed`

  - To see the standard output of a particular job:
    `nfwq get_state --db test_dag.db --tag node1 --fields task_output`

  - To count how many jobs finished on each machine in your pool:
     `nfwq get_state --db test_find_log_lr_optimize_in_terms_of_ll_aug5.dag --state finished --fields task_hostname | sort | uniq -c | sort -n`

- `update_state` - Update the state of the workflow. For example, to rerun just the job with tag `job1`:
  `nfwq update_state --db test_dag.db --tag job1 --state waiting && nfwq refresh_master --db test_dag.db`

- `retry_failed` - Retry all failed jobs. For example,
  `nfwq retry_failed --db test_dag.db && nfwq refresh_master --db test_dag.db`

- `retry_queued` - Retry all queued jobs. For example, 
  `nfwq retry_queued --db test_dag.db && nfwq refresh_master --db test_dag.db`

- `retry_finished` - Retry all finished jobs. For example,
  `nfwq retry_finished --db test_dag.db && nfwq refresh_master --db test_dag.db`

- `start_ssh_worker` - Start a worker or workers via SSH.

- `start_condor_worker` - Start a worker or wokers via Condor. This command has
  two modes, depending on whether one or many workers are being started. If you
  are requesting to start one worker targeting a specific machine, you can use
  something like this:

  `nfwq start_condor_worker --server mammoth-2 --port 9140 --client fugu --frac_mem 0.75 --frac_cpu 1.0`

  to start a worker on `fugu` that uses 75% of its memory and 100% of its CPUs.
  You can also `--max_mem` and `--max_cpu` to set the desired amount of memory
  and number of CPUs.
  
  If you are requesting to start multiple workers that can run on many machines
  the `--frac_mem` and `--frac_cpu` entries don't make much sense, so they are
  not allowed; but you can still use `--max_mem` and `--max_cpu`. You can also
  use --exclude_clients to blacklist certain machines. For example, to start
  1000 Condor workers, each using 4 CPUs, that do run anywhere but `fugu`, use:

  `nfwq start_condor_worker --server mammoth-2 --port 9140 --num_processes 1000 --num_cpu 4 --exclude_clients fugu`

- `condor_q` - Get information about Condor workers. This provides output
  similar to Condor's `condor_q` command, but additionally includes the host
  where each worker is running.

## (3) Python API for use by jobs

Jobs can use the following simple functions:

- `mem`: How much memory (in kilobytes) has been allocated to this worker?
- `cpu`: How many cpus have been allocated to this worker?
- `scratch`: Where is the scratch space for the worker?
- `server`: What is the hostname of the server?
- `get_file`: Copy a file from the server using rsync.
- `put_file`: Copy a file to the server using rsync.

For more information, look at the definition of each function in nfwq.py. It
isn't necessary to use Python or this Python API in your jobs. For example,
`mem` just looks at the `NFWQ_MEM` environment variable, which can also be
accessed in your favorite language.
