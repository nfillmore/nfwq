#!/usr/bin/env python3
import argparse, subprocess, sys, os

parser = argparse.ArgumentParser()
parser.add_argument("--server", required=True)
parser.add_argument("--port", required=True)
parser.add_argument("--client", required=True)
parser.add_argument("--num_processes", type=int, default=1)
parser.add_argument("--frac_mem", type=float, default=0.85)
parser.add_argument("--frac_cpu", type=float, default=1.00)
parser.add_argument("--timeout", default="900s")
parser.add_argument("--bindir", default="/tier2/deweylab/nathanae/sw/cctools/cctools-git-install/bin/")
parser.add_argument("--logdir", default="/tier2/deweylab/scratch/nathanae/wq_condor_logs")
parser.add_argument("--internal_driver", action="store_true", help="For internal use only.")
args = parser.parse_args()

if args.internal_driver:

  ls = subprocess.check_output(["free", "-k"])
  tot_mem = int(ls.decode("utf-8").split("\n")[1].split()[1])
  max_mem = int(args.frac_mem * tot_mem)

  ls = subprocess.check_output(["cat", "/proc/cpuinfo"])
  tot_cpu = 0
  for l in ls.decode("utf-8").split("\n"):
    if len(l.strip()) > 0:
      k, v = l.split(":")
      if k.strip() == "processor":
        tot_cpu += 1
  max_cpu = int(args.frac_cpu * tot_cpu)

  subprocess.check_call(["pwd"])
  subprocess.check_call(["env"])

  tmpdir = os.environ["TMPDIR"]
  print("tmpdir is {}".format(tmpdir))

  cmd = "ulimit -v {max_mem} && OMP_NUM_THREADS={max_cpu} {bindir}/work_queue_worker -t {timeout} -s {tmpdir} {server} {port}".format(
    max_mem=max_mem, max_cpu=max_cpu, bindir=args.bindir, timeout=args.timeout, tmpdir=tmpdir, server=args.server, port=args.port)
  subprocess.check_call(cmd, shell=True)

else:

  current_script = os.path.abspath(__file__)

  if args.client[-4:] != ".edu":
    args.client += ".biostat.wisc.edu"

  l = subprocess.check_output(["condor_status", "-format", "%s,", "TotalCpus", args.client])
  tot_cpu = int(l.decode("utf-8").split(",")[0])
  max_cpu = int(args.frac_cpu * tot_cpu)

  if args.num_processes == 0:
    args.num_processes = max_cpu

  # We set should_transfer_files = yes not because we want to use condor to
  # transfer files (we don't), but because this forces condor to execute in the
  # EXECUTE directory, which we do want. Cf.
  # <http://comments.gmane.org/gmane.comp.distributed.condor.user/8396>. There
  # doesn't seem to be a better way to achieve this.
  submit = """
    universe = vanilla
    notification = never
    getenv = True
    should_transfer_files = no
    #should_transfer_files = yes
    #when_to_transfer_output = ON_EXIT_OR_EVICT
    log = {logdir}/condor.log

    requirements = (TARGET.Machine == "{client}")
    #request_cpus = {max_cpu}

    executable = {current_script}
    arguments = {args} --internal_driver 

    output = {logdir}/condor.{client}.$(Process).out
    error = {logdir}/condor.{client}.$(Process).err

    queue {num_processes}
  """.format(
    logdir=args.logdir, client=args.client, max_cpu=max_cpu, current_script=current_script, args=" ".join(sys.argv[1:]), num_processes=args.num_processes)

  print(submit)

  p = subprocess.Popen(["condor_submit"], stdin=subprocess.PIPE)
  p.communicate(submit.encode("utf-8"))
  rc = p.wait()
  assert rc == 0
