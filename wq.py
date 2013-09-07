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
        tag             text,
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

  def run(self):
    # Loop forever; each iteration, ask the workqueue to give us some
    # information about running jobs, and look in the appropriate directory for
    # new jobs.
    while True:
      task = self.wq.wait(0)
      if task:
        print("Finished {}".format(task.tag))
        job_id = self.postprocess_popped_task(task)
        self.queue_ready_children(job_id)
      self.print_status()
      if self.dag.query_refresh():
        self.queue_ready_jobs()
      time.sleep(1)

  def print_status(self):
    dag_stats = self.dag.compute_dag_stats()
    print("workers: init={}, ready={}, busy={}; tasks: running={}, waiting={}, complete={}; jobs: waiting={}, succeeded={}, failed={}".format(
      self.wq.stats.workers_init, self.wq.stats.workers_ready, self.wq.stats.workers_busy,
      self.wq.stats.tasks_running, self.wq.stats.tasks_waiting, self.wq.stats.tasks_complete,
      dag_stats["waiting"], dag_stats["finished"], dag_stats["failed"]))

  def postprocess_popped_task(self, task):
    """Check whether the task succeeded, and update the database as appropriate.
    "Success" means that the task succeeded and its subprocess returned 0."""
    job_id = self.dag.tag_to_job_id(task.tag)
    if task.result == 0 and task.return_status == 0:
      self.dag.update_state(job_id, "finished", task)
    else:
      self.dag.update_state(job_id, "failed", task)
    return job_id

  def queue_ready_children(self, parent_id):
    job_ids = self.dag.find_ready_children(parent_id)
    for job_id in job_ids:
      self.queue(job_id)

  def queue_ready_jobs(self):
    print("Looking for ready jobs ...")
    job_ids = self.dag.find_ready_jobs()
    print("... Found ready jobs: {}".format(job_ids))
    for job_id in job_ids:
      self.queue(job_id)

  def queue(self, job_id):
    job = self.dag.get_job_info(job_id)
    print("Queuing {}".format(job.tag))

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
  p.add_argument("--fields", default="tag,state,timestamp")
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


if __name__ == "__main__":
  if len(sys.argv) > 1:

    if sys.argv[1] == "_drive":
      _drive()

    if sys.argv[1] == "start_master":
      start_master(sys.argv[2:])

    if sys.argv[1] == "refresh_master":
      refresh_master(sys.argv[2:])

    if sys.argv[1] == "get_state":
      get_state(sys.argv[2:])

    if sys.argv[1] == "update_state":
      update_state(sys.argv[2:])

  else:
    print("usage: {} command args".format(sys.argv[0]))
    print("available commands:")
    print("  start_master")
    print("  refresh_master")
    print("  get_state")
    print("  update_state")
    sys.exit(1)

#dag = Dag("test_dag.db", False)
