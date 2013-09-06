import argparse, subprocess, sys

parser = argparse.ArgumentParser()
parser.add_argument("--server", required=True)
parser.add_argument("--port", required=True)
parser.add_argument("--client", required=True)
parser.add_argument("--frac_mem", type=float, default=0.85)
parser.add_argument("--frac_cpu", type=float, default=0.85)
parser.add_argument("--bindir", default="/tier2/deweylab/nathanae/sw/cctools/cctools-git-install/bin/")
parser.add_argument("--scratch", default="/scratch/nathanae/wq_tmp")
args = parser.parse_args()

ls = subprocess.check_output(["ssh", args.client, "free", "-k"])
tot_mem = int(ls.decode("utf-8").split("\n")[1].split()[1])
max_mem = int(args.frac_mem * tot_mem)

ls = subprocess.check_output(["ssh", args.client, "cat", "/proc/cpuinfo"])
tot_cpu = 0
for l in ls.decode("utf-8").split("\n"):
  if len(l.strip()) > 0:
    k, v = l.split(":")
    if k.strip() == "processor":
      tot_cpu += 1
max_cpu = int(args.frac_cpu * tot_cpu)

cmd = "ulimit -v {max_mem} && OMP_NUM_THREADS={max_cpu} {bindir}/work_queue_worker -t 900000000 -s {scratch} {server} {port}".format(
  max_mem=max_mem, max_cpu=max_cpu, bindir=args.bindir, server=args.server, port=args.port, scratch=args.scratch)
print(cmd)
p = subprocess.Popen(["ssh", args.client, cmd])
