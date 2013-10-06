#!/u/deweylab/sw/python-3.3.2/arch/x86_64-redhat-linux-gnu/bin/python3
import json, time, os, sys, work_queue, subprocess, glob, argparse, re, sqlite3, datetime
from collections import namedtuple

CACHE                      = work_queue.WORK_QUEUE_CACHE
DEFAULT_KEEPALIVE_INTERVAL = work_queue.WORK_QUEUE_DEFAULT_KEEPALIVE_INTERVAL
DEFAULT_KEEPALIVE_TIMEOUT  = work_queue.WORK_QUEUE_DEFAULT_KEEPALIVE_TIMEOUT
DEFAULT_PORT               = work_queue.WORK_QUEUE_DEFAULT_PORT
INPUT                      = work_queue.WORK_QUEUE_INPUT
MASTER_MODE_CATALOG        = work_queue.WORK_QUEUE_MASTER_MODE_CATALOG
MASTER_MODE_STANDALONE     = work_queue.WORK_QUEUE_MASTER_MODE_STANDALONE
NOCACHE                    = work_queue.WORK_QUEUE_NOCACHE
OUTPUT                     = work_queue.WORK_QUEUE_OUTPUT
PREEXIST                   = work_queue.WORK_QUEUE_PREEXIST
RANDOM_PORT                = work_queue.WORK_QUEUE_RANDOM_PORT
RESET_ALL                  = work_queue.WORK_QUEUE_RESET_ALL
RESET_KEEP_TASKS           = work_queue.WORK_QUEUE_RESET_KEEP_TASKS
SCHEDULE_FCFS              = work_queue.WORK_QUEUE_SCHEDULE_FCFS
SCHEDULE_FILES             = work_queue.WORK_QUEUE_SCHEDULE_FILES
SCHEDULE_RAND              = work_queue.WORK_QUEUE_SCHEDULE_RAND
SCHEDULE_TIME              = work_queue.WORK_QUEUE_SCHEDULE_TIME
SCHEDULE_UNSET             = work_queue.WORK_QUEUE_SCHEDULE_UNSET
SYMLINK                    = work_queue.WORK_QUEUE_SYMLINK
TASK_ORDER_FIFO            = work_queue.WORK_QUEUE_TASK_ORDER_FIFO
TASK_ORDER_LIFO            = work_queue.WORK_QUEUE_TASK_ORDER_LIFO
THIRDGET                   = work_queue.WORK_QUEUE_THIRDGET
THIRDPUT                   = work_queue.WORK_QUEUE_THIRDPUT
WAITFORTASK                = work_queue.WORK_QUEUE_WAITFORTASK

#File      = namedtuple("File",      ("local_name", "remote_name", "type", "flags"))
#Directory = namedtuple("Directory", ("local_name", "remote_name", "type", "flags", "recursive"))
#Buffer    = namedtuple("Buffer",    ("buffer",     "remote_name",         "flags"))
Job       = namedtuple("Job",       ("job_id", "tag", "cmd", "algorithm", "preferred_host", "cores", "memory", "disk", "parents", "files", "directories", "buffers"))

def input_file(local_name, remote_name=None, cache=True):
  if local_name[0] != "/":
    d = os.getcwd()
    local_name = d + "/" + local_name
  #assert os.path.isfile(local_name), "{} is not a file".format(local_name)
  if remote_name is None:
    remote_name = os.path.basename(local_name)
  flags = CACHE if cache else NOCACHE
  return {
    "local_name": local_name,
    "remote_name": remote_name,
    "type": INPUT,
    "flags": flags
  }

def output_file(local_name, remote_name=None, cache=True):
  if local_name[0] != "/":
    d = os.getcwd()
    local_name = d + "/" + local_name
  assert os.path.isdir(os.path.dirname(os.path.abspath(local_name)))
  if remote_name is None:
    remote_name = os.path.basename(local_name)
  flags = CACHE if cache else NOCACHE
  return {
    "local_name": local_name,
    "remote_name": remote_name,
    "type": OUTPUT,
    "flags": flags
  }

def _fetch_exactly_one(cursor):
  res = cursor.fetchall()
  assert len(res) == 1
  return res[0]

class Dag:

  def __init__(self, db):
    self.conn = sqlite3.connect(db, timeout=24*60*60)
    self.conn.row_factory = sqlite3.Row

  def init(self):
    with self.conn:

      # This table describes the job itself.
      self.conn.execute("""create table jobs (
        job_id          integer primary key autoincrement,
        tag             text unique,
        cmd             text,
        algorithm       integer,
        preferred_host  text,
        cores           integer,
        memory          integer,
        disk            integer,
        files           text,
        directories     text,
        buffers         text)
        """)
      self.conn.execute("create index jobs_id on jobs (job_id)")

      # This table describes the DAG.
      self.conn.execute("create table parents (job_id integer, parent_id integer)")
      self.conn.execute("create index parents_job_id    on parents (job_id)")
      self.conn.execute("create index parents_parent_id on parents (parent_id)")

      # This table describes the status changes of the job.
      self.conn.execute("""create table states (
        job_id                       integer,   -- These first four columns
        state                        text,      -- relate to the state itself.
        is_most_recent               integer,
        timestamp                    timestamp,
        task_tag                     text,      -- The remaining columns
        task_command                 text,      -- describe the task which
        task_algorithm               integer,   -- resulted in this
        task_output                  text,      -- finished or failed state.
        task_id                      integer,
        task_return_status           integer,
        task_result                  integer,
        task_host                    text,
        task_hostname                text,
        task_submit_time             integer,
        task_finish_time             integer,
        task_app_delay               integer,
        task_send_input_start        integer,
        task_send_input_finish       integer,
        task_execute_cmd_start       integer,
        task_execute_cmd_finish      integer,
        task_receive_output_start    integer,
        task_receive_output_finish   integer,
        task_total_bytes_transferred integer,
        task_total_transfer_time     integer,
        task_cmd_execution_time      integer)
        """)
      self.conn.execute("create index states_job_id on states (job_id, is_most_recent)")
      self.conn.execute("create index states_state on states (state, is_most_recent)")

      # This table controls the pipeline.
      self.conn.execute("create table refresh (request_refresh integer)")

  def add(self, cmd, tag, parents=None, algorithm=None, preferred_host=None, files=None, directories=None, buffers=None, cores=None, memory=None, disk=None):
    print("Adding {}".format(tag))
    with self.conn:

      # Add info about the job itself.
      if files       is None: files       = []
      if directories is None: directories = []
      if buffers     is None: buffers     = []
      files       = json.dumps(files)
      directories = json.dumps(directories)
      buffers     = json.dumps(buffers)
      c = self.conn.execute("""
        insert into jobs (tag, cmd, algorithm, preferred_host, cores, memory, disk, files, directories, buffers)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (tag, json.dumps(cmd), algorithm, preferred_host, cores, memory, disk, files, directories, buffers))
      job_id = c.lastrowid

      # Add info about the job's dependencies.
      if parents is not None:
        self.conn.executemany(
        """
          insert into parents
          select ? as job_id,
                 job_id as parent_id
          from jobs
          where tag=?
        """, [[job_id, parent] for parent in parents])

      # Init the state of the job to "waiting".
      c.execute("insert into states (job_id, state, is_most_recent, timestamp) values (?, \"waiting\", 1, ?)", (job_id, datetime.datetime.now()))

  def get_state(self, job_id):
    c = self.conn.cursor()
    c.execute("select state from states where job_id=? and is_most_recent=1", (job_id,))
    return _fetch_exactly_one(c)["state"]

  def update_state(self, job_id, state, task=None):
    with self.conn as c:
      c.execute("update states set is_most_recent=0 where job_id=? and is_most_recent=1", (job_id,))
      if task is None:
        c.execute("insert into states (job_id, state, is_most_recent, timestamp) values (?, ?, ?, ?)", (job_id, state, 1, datetime.datetime.now()))
      else:
        c.execute("insert into states values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
           (job_id,
            state,
            1,
            datetime.datetime.now(),
            task.tag,
            task.command,
            task.algorithm,
            task.output,
            task.id,
            task.return_status,
            task.result,
            task.host,
            task.hostname,
            task.submit_time,
            task.finish_time,
            task.app_delay,
            task.send_input_start,
            task.send_input_finish,
            task.execute_cmd_start,
            task.execute_cmd_finish,
            task.receive_output_start,
            task.receive_output_finish,
            task.total_bytes_transferred,
            task.total_transfer_time,
            task.cmd_execution_time))

  def is_job_ready(self, job_id):
    """For each candidate job, determine if all its parents have finished, in
    which case it is ready to be run.
    """
    c = self.conn.cursor()
    c.execute("""
      select distinct states.state
      from parents, states
      on parents.parent_id = states.job_id
      where parents.job_id = ?
        and states.is_most_recent = 1
    """, (job_id,))
    for (state,) in c:
      if state != "finished":
        return False
    return True

  def find_ready_children(self, parent_id):
    c = self.conn.cursor()
    c.execute("select distinct job_id from parents where parent_id = ?", (parent_id,))
    return [job_id for (job_id,) in c if self.get_state(job_id) == "waiting" and self.is_job_ready(job_id)]

  def find_ready_jobs(self):
    ready_job_ids = []
    c = self.conn.cursor()
    #c.execute("select distinct job_id from jobs")
    #return [job_id for (job_id,) in c if self.get_state(job_id) == "waiting" and self.is_job_ready(job_id)]
    c.execute("select job_id from states where state=\"waiting\" and is_most_recent=1")
    job_ids = [r["job_id"] for r in c]
    for job_id in job_ids:
      c.execute("""
        select distinct states.state
        from parents, states
        on parents.parent_id=states.job_id
        where parents.job_id=? and states.is_most_recent=1
      """, (job_id,))
      res = c.fetchall()
      if len(res) == 0 or \
         (len(res) == 1 and res[0][0] == "finished"):
        ready_job_ids.append(job_id)
    return ready_job_ids

  def compute_dag_stats(self):
    c = self.conn.cursor()
    c.execute("select state, count(state) from states where is_most_recent=1 group by state")
    stats = dict(c)
    stats.setdefault("waiting", 0)
    stats.setdefault("finished", 0)
    stats.setdefault("failed", 0)
    return stats

  def get_job_info(self, job_id):
    c = self.conn.cursor()

    c.execute("select * from jobs where job_id=?", (job_id,))
    info = dict(_fetch_exactly_one(c))
    info["files"]       = json.loads(info["files"])
    info["directories"] = json.loads(info["directories"])
    info["buffers"]     = json.loads(info["buffers"])

    c.execute("select parent_id from parents where job_id=?", (job_id,))
    info["parents"] = tuple(r["parent_id"] for r in c)

    return Job(**info)

  def query_refresh(self):
    # XXX fix race condition
    c = self.conn.cursor()
    c.execute("select count(*) as count from refresh")
    should_refresh = (c.fetchone()["count"] != 0)
    if should_refresh:
      with self.conn:
        self.conn.execute("delete from refresh")
    return should_refresh

  def request_refresh(self):
    with self.conn:
      self.conn.execute("insert into refresh values (1)")

  def tag_to_job_id(self, tag):
    c = self.conn.cursor()
    c.execute("select job_id from jobs where tag=?", (tag,))
    return _fetch_exactly_one(c)["job_id"]

class Master:

  def __init__(self, db, wq):
    self.dag = Dag(db)
    self.wq = wq
    self.old_status = None

  def notify(self, msg):
    now = datetime.datetime.now().isoformat(sep=" ")
    print("{}: {}".format(now, msg))

  def run(self):
    # Loop forever; each iteration, ask the workqueue to give us some
    # information about running jobs, and look in the appropriate directory for
    # new jobs.
    while True:
      task = self.wq.wait(0)
      if task:
        job_id = self.postprocess_popped_task(task)
        self.queue_ready_children(job_id)
      else:
        time.sleep(1)
      if self.dag.query_refresh():
        self.queue_ready_jobs()
      self.print_status()

  def print_status(self):
    dag_stats = self.dag.compute_dag_stats()
    new_status = "workers: init={}, ready={}, busy={}; tasks: running={}, waiting={}, complete={}; jobs: waiting={}, succeeded={}, failed={}".format(
      self.wq.stats.workers_init, self.wq.stats.workers_ready, self.wq.stats.workers_busy,
      self.wq.stats.tasks_running, self.wq.stats.tasks_waiting, self.wq.stats.tasks_complete,
      dag_stats["waiting"], dag_stats["finished"], dag_stats["failed"])
    if self.old_status != new_status:
      self.notify(new_status)
      self.old_status = new_status

  def postprocess_popped_task(self, task):
    """Check whether the task succeeded, and update the database as appropriate.
    "Success" means that the task succeeded and its subprocess returned 0."""
    job_id = self.dag.tag_to_job_id(task.tag)
    if task.result == 0 and task.return_status == 0:
      self.notify("Finished {}".format(task.tag))
      self.dag.update_state(job_id, "finished", task)
    else:
      self.notify("Failed {}".format(task.tag))
      self.dag.update_state(job_id, "failed", task)
    return job_id

  def queue_ready_children(self, parent_id):
    job_ids = self.dag.find_ready_children(parent_id)
    for job_id in job_ids:
      self.queue(job_id)

  def queue_ready_jobs(self):
    self.notify("Looking for ready jobs ...")
    job_ids = self.dag.find_ready_jobs()
    self.notify("... Found ready jobs: {}".format(job_ids))
    for job_id in job_ids:
      self.queue(job_id)

  def queue(self, job_id):
    job = self.dag.get_job_info(job_id)
    self.notify("Queuing {}".format(job.tag))

    wq_py = os.path.abspath(__file__)
    t = work_queue.Task("{} _drive".format(wq_py))
    t.specify_tag(job.tag)
    t.specify_buffer(buffer=job.cmd, remote_name="__cmd__", flags=NOCACHE)

    if job.algorithm      is not None: t.specify_algorithm     (job.algorithm)
    if job.preferred_host is not None: t.specify_preferred_host(job.preferred_host)
    if job.cores          is not None: t.specify_cores         (job.cores)
    if job.memory         is not None: t.specify_memory        (job.memory)
    if job.disk           is not None: t.specify_disk          (job.disk)

    for f in job.files:       t.specify_file(**f)
    for d in job.directories: t.specify_directory(**d)
    for b in job.buffers:     t.specify_buffer(**b)

    self.wq.submit(t)
    self.dag.update_state(job_id, "queued")


def _drive():

  # Prepare to run job
  cmd = json.load(open("__cmd__"))

  # Check that we are running in scratch.
  if re.search(r'^/scratch', os.getcwd()) is None:
    print("wq.py _drive: warning: {} does not start with /scratch".format(os.getcwd()), file=sys.stderr)

  # Run job
  rc = subprocess.call(cmd)

  # Clean up
  sys.exit(rc)

def start_master(argv):

  # Parse the arguments
  p = argparse.ArgumentParser()
  p.add_argument("--db", required=True)
  p.add_argument("--port", type=int, default=9123)
  p.add_argument("--name", default="wq")
  p.add_argument("--catalog", choices=("yes", "no"), default="no")
  p.add_argument("--exclusive", choices=("yes", "no"), default="no")
  p.add_argument("--shutdown", choices=("yes", "no"), default="yes")
  args = p.parse_args(argv)

  # Convert to python data types
  catalog = (args.catalog == "yes")
  exclusive = (args.exclusive == "yes")
  shutdown = (args.shutdown == "yes")

  # Create work queue
  wq = work_queue.WorkQueue(port=args.port, name=args.name, catalog=catalog, exclusive=exclusive) # , shutdown=shutdown)
  #wq.enable_monitoring(args.db + ".monitor")
  #wq.specify_log(args.db + ".log")

  # Start the master
  m = Master(args.db, wq)
  m.run()

def refresh_master(argv):
  p = argparse.ArgumentParser()
  p.add_argument("--db", required=True)
  args = p.parse_args(argv)
  dag = Dag(args.db)
  dag.request_refresh()

def get_state(argv):
  p = argparse.ArgumentParser(description="Print out the state of the job identified either by JOB_ID or STATE.")
  p.add_argument("--db", required=True)
  p.add_argument("--state", choices=("waiting", "queued", "finished", "failed"))
  p.add_argument("--tag")
  p.add_argument("--fields", default="tag,state,timestamp,task_cmd_execution_time,task_hostname")
  p.add_argument("--sep", default="\t")
  args = p.parse_args(argv)

  if args.state is not None and args.tag is not None:
    p.print_usage(file=sys.stderr)
    print("{}: error: --state and --tag are mutually exclusive".format(sys.argv[0]), file=sys.stderr)
    sys.exit(1)

  dag = Dag(args.db)
  c = dag.conn.cursor()

  select = "select jobs.tag, states.* from jobs, states on jobs.job_id=states.job_id"
  if args.state is not None:
    c.execute(select + " where states.is_most_recent=1 and states.state=?", (args.state,))
  elif args.tag is not None:
    c.execute(select + " where states.is_most_recent=1 and jobs.tag=?", (args.tag,))
  else:
    c.execute(select + " where states.is_most_recent=1")

  fields = args.fields.split(",")
  for row in c:
    print(args.sep.join(str(row[f]) for f in fields))

def update_state(argv):
  p = argparse.ArgumentParser(description="Change state of the job identified by JOB_ID to STATE.")
  p.add_argument("--db", required=True)
  p.add_argument("--tag", required=True)
  p.add_argument("--state", choices=("waiting", "queued", "finished", "failed"), required=True)
  args = p.parse_args(argv)
  dag = Dag(args.db)
  job_id = dag.tag_to_job_id(args.tag)
  dag.update_state(job_id, args.state)

def retry_failed(argv):
  p = argparse.ArgumentParser(description="Change the state of all failed jobs to \"waiting\".")
  p.add_argument("--db", required=True)
  args = p.parse_args(argv)
  dag = Dag(args.db)
  # Figure out which jobs are in state "failed".
  c = dag.conn.cursor()
  c.execute("select job_id from states where state=\"failed\" and is_most_recent=1")
  job_ids = [r["job_id"] for r in c]
  # Update their state to "waiting".
  with dag.conn as c:
    ts = datetime.datetime.now()
    c.executemany("update states set is_most_recent=0 where job_id=? and is_most_recent=1", [(job_id,) for job_id in job_ids])
    c.executemany("insert into states (job_id, state, is_most_recent, timestamp) values (?, ?, ?, ?)",
      [(job_id, "waiting", 1, ts) for job_id in job_ids])

def start_ssh_worker(argv):

  parser = argparse.ArgumentParser()
  parser.add_argument("--server", required=True)
  parser.add_argument("--port", required=True)
  parser.add_argument("--client", required=True)
  parser.add_argument("--frac_mem", type=float, default=0.85)
  parser.add_argument("--frac_cpu", type=float, default=0.85)
  parser.add_argument("--bindir")
  parser.add_argument("--scratch", default="/scratch/nathanae/wq_tmp")
  args = parser.parse_args(argv)

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

  if args.bindir is None:
    args.bindir = os.path.dirname(subprocess.check_output(["which", "work_queue_worker"]).decode("utf-8").strip())

  cmd = "ulimit -v {max_mem} && OMP_NUM_THREADS={max_cpu} {bindir}/work_queue_worker -t 900000000 -s {scratch} {server} {port}".format(
    max_mem=max_mem, max_cpu=max_cpu, bindir=args.bindir, server=args.server, port=args.port, scratch=args.scratch)
  print(cmd)
  p = subprocess.Popen(["ssh", args.client, cmd])

def _add_biostat_wisc_edu(host):
  if host[-4:] != ".edu":
    host += ".biostat.wisc.edu"
  return host

def _start_condor_workers_parser():
  parser = argparse.ArgumentParser()
  parser.add_argument("--server", required=True)
  parser.add_argument("--port", required=True)
  parser.add_argument("--client")
  parser.add_argument("--exclude_clients")
  parser.add_argument("--num_processes", type=int, default=1)
  parser.add_argument("--frac_mem", type=float, default=0.85)
  parser.add_argument("--frac_cpu", type=float)
  parser.add_argument("--num_cpu", type=int)
  parser.add_argument("--timeout", default="900s")
  parser.add_argument("--bindir")
  parser.add_argument("--logdir", default="/tier2/deweylab/scratch/nathanae/wq_condor_logs")
  parser.add_argument("--internal_driver", action="store_true", help="For internal use only.")
  return parser

def start_condor_worker(argv):

  parser = _start_condor_workers_parser()
  args = parser.parse_args(argv)

  if args.frac_cpu is not None and args.num_cpu is not None:
    print("--frac_cpu and --num_cpu are incompatible.")
    sys.exit(1)

  if args.frac_cpu is not None and args.client is None:
    print("--frac_cpu requires --client to be specified.")
    sys.exit(1)

  if args.frac_cpu is None and args.num_cpu is None:
    args.frac_cpu = 1.0

  if args.internal_driver:

    ls = subprocess.check_output(["free", "-k"])
    tot_mem = int(ls.decode("utf-8").split("\n")[1].split()[1])
    max_mem = int(args.frac_mem * tot_mem)

    if args.frac_cpu is not None:
      ls = subprocess.check_output(["cat", "/proc/cpuinfo"])
      tot_cpu = 0
      for l in ls.decode("utf-8").split("\n"):
        if len(l.strip()) > 0:
          k, v = l.split(":")
          if k.strip() == "processor":
            tot_cpu += 1
      max_cpu = int(args.frac_cpu * tot_cpu)
    else:
      assert args.num_cpu is not None
      max_cpu = args.num_cpu

    if args.bindir is None:
      args.bindir = os.path.dirname(subprocess.check_output(["which", "work_queue_worker"]).decode("utf-8").strip())

    subprocess.check_call(["pwd"])
    subprocess.check_call(["env"])

    tmpdir = os.environ["TMPDIR"]
    print("tmpdir is {}".format(tmpdir))

    cmd = "ulimit -v {max_mem} && OMP_NUM_THREADS={max_cpu} {bindir}/work_queue_worker -t {timeout} -s {tmpdir} {server} {port}".format(
      max_mem=max_mem, max_cpu=max_cpu, bindir=args.bindir, timeout=args.timeout, tmpdir=tmpdir, server=args.server, port=args.port)
    subprocess.check_call(cmd, shell=True)

  else:

    current_script = os.path.abspath(__file__)

    if args.frac_cpu is not None:
      l = subprocess.check_output(["condor_status", "-format", "%s,", "TotalCpus", args.client])
      tot_cpu = int(l.decode("utf-8").split(",")[0])
      max_cpu = int(args.frac_cpu * tot_cpu)
    else:
      assert args.num_cpu is not None
      max_cpu = args.num_cpu

    if args.num_processes == 0:
      l = subprocess.check_output(["condor_status", "-format", "%s,", "TotalCpus", args.client])
      tot_cpu = int(l.decode("utf-8").split(",")[0])
      args.num_processes = tot_cpu

    if args.client is not None:
      args.client = _add_biostat_wisc_edu(args.client)
      print("Queuing {} processses, each with {} cpu, on host {}.".format(args.num_processes, max_cpu, args.client))
      requirements = """
        requirements = (TARGET.Machine == "{client}")
        output = {logdir}/condor.{client}.$(Process).out
        error = {logdir}/condor.{client}.$(Process).err
      """.format(client=args.client, logdir=args.logdir)

    else:
      if args.exclude_clients is not None:
        excluded = [_add_biostat_wisc_edu(x) for x in args.exclude_clients.split(",")]
        exstr = "&&".join("TARGET.Machine != \"{}\"".format(x) for x in excluded)
        excluded_requirement = "requirements = (" + exstr + ")"
      else:
        excluded_requirement = ""
      print("Queuing {} processses, each with {} cpu.".format(args.num_processes, max_cpu, args.client))
      requirements = """
        {excluded_requirement}
        request_cpus = {max_cpu}
        output = {logdir}/condor.$(Process).out
        error = {logdir}/condor.$(Process).err
      """.format(excluded_requirement=excluded_requirement, max_cpu=max_cpu, logdir=args.logdir)

    submit = """
      universe = vanilla
      notification = never
      getenv = True
      should_transfer_files = no
      log = {logdir}/condor.log

      {requirements}

      executable = {current_script}
      arguments = start_condor_worker {args} --internal_driver 

      queue {num_processes}
    """.format(
      logdir=args.logdir, requirements=requirements, current_script=current_script, args=" ".join(argv), num_processes=args.num_processes)

    print(submit)

    p = subprocess.Popen(["condor_submit"], stdin=subprocess.PIPE)
    p.communicate(submit.encode("utf-8"))
    rc = p.wait()
    assert rc == 0

def condor_q(argv):
  def unquote(s):
    if s[0] == '"':
      assert s[-1] == '"'
      s = s[1:-1]
    return s
  def job_status(code):
    # https://htcondor-wiki.cs.wisc.edu/index.cgi/wiki?p=MagicNumbers
    codes = {
      "0": "U",
      "1": "I",
      "2": "R",
      "3": "X",
      "4": "C",
      "5": "H",
      "6": ">",
      "7": "S"
    }
    return codes[code]
  def extract_args(d):
    parser = _start_condor_workers_parser()
    tmp = unquote(d.get("Args", "-")).split()
    if len(tmp) > 0 and tmp[0] == "start_condor_worker":
      argv = tmp[1:]
    else:
      argv = ["--server", "-", "--port", "-"]
    args = parser.parse_args(argv)
    return args
  out = subprocess.check_output(["condor_q", "-long", "-attributes", "ClusterId,ProcId,JobStatus,JobPrio,RemoteHost,LastRemoteHost,Args"]).decode("utf-8")
  recs = out.split("\n\n")
  for i, rec in enumerate(recs):
    if rec.strip() != "":
      ls = rec.split("\n")
      if i == 1:
        assert re.search(r'^-- Submitter:', ls[0]) is not None
        ls = ls[1:]
      d = dict(l.split(" = ") for l in ls)
      args = extract_args(d)
      print("\t".join((
        d["ClusterId"] + "." + d["ProcId"],
        job_status(d["JobStatus"]),
        d["JobPrio"],
        str(args.num_cpu),
        "{}:{}".format(args.server, args.port),
        unquote(d.get("RemoteHost", "-")))))

if __name__ == "__main__":

  show_usage = False
  if len(sys.argv) > 1:

    if sys.argv[1] == "_drive":
      _drive()

    elif sys.argv[1] == "start_master":
      start_master(sys.argv[2:])

    elif sys.argv[1] == "refresh_master":
      refresh_master(sys.argv[2:])

    elif sys.argv[1] == "get_state":
      get_state(sys.argv[2:])

    elif sys.argv[1] == "update_state":
      update_state(sys.argv[2:])

    elif sys.argv[1] == "retry_failed":
      retry_failed(sys.argv[2:])

    elif sys.argv[1] == "start_ssh_worker":
      start_ssh_worker(sys.argv[2:])

    elif sys.argv[1] == "start_condor_worker":
      start_condor_worker(sys.argv[2:])

    elif sys.argv[1] == "condor_q":
      condor_q(sys.argv[2:])

    else:
      print("nfwq: {} is not a valid command".format(sys.argv[1]))
      show_usage = True

  else:
    show_usage = True

  if show_usage:
    print("usage: {} command args".format(sys.argv[0]))
    print("available commands:")
    print("  start_master")
    print("  refresh_master")
    print("  get_state")
    print("  update_state")
    print("  retry_failed")
    print("  start_ssh_worker")
    print("  start_condor_worker")
    print("  condor_q")
    sys.exit(1)

#dag = Dag("test_dag.db", False)
