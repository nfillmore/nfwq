import sys, nfwq, work_queue

d = "/scratch/nathanae/test_dag"

dag = nfwq.Dag(d + "/test_dag.db")
dag.init()

dag.add(tag="node1",
        cmd=["touch", "node1.output"],
        files=[{"local_name": d + "/node1.output", 
                "remote_name": "node1.output", 
                "type": nfwq.OUTPUT,
                "flags": nfwq.NOCACHE}])

dag.add(tag="node2",
        cmd=["ls", "-ltrh", "/ua/nathanae/"],
        files=[{"local_name": d + "/node1.output",
                "remote_name": "node1.output",
                "type": nfwq.INPUT,
                "flags": nfwq.NOCACHE}],
        parents=["node1"])
