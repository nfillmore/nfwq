import sys, wq, work_queue

d = "/scratch/nathanae/test_dag"

dag = wq.Dag(d + "/test_dag.db")
dag.init()

dag.add(id="node1",
        cmd=["touch", "node1.output"],
        files=[wq.File(local_name=d + "/node1.output", 
                       remote_name="node1.output", 
                       type=wq.OUTPUT,
                       flags=wq.NOCACHE)])

dag.add(id="node2",
        cmd=["ls", "-ltrh", "/ua/nathanae/"],
        files=[wq.File(local_name=d + "/node1.output",
                       remote_name="node1.output",
                       type=wq.INPUT,
                       flags=wq.NOCACHE)],
        parents=["node1"])
