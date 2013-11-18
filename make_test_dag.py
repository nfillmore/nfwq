import sys, nfwq, work_queue, argparse

p = argparse.ArgumentParser()
p.add_argument("--workdir", required=True)
args = p.parse_args()

dag = nfwq.Dag(args.workdir + "/test_dag.db")
dag.init()

dag.add(tag="node1",
        cmd=["touch", "node1.output"],
        files=[nfwq.output_file(args.workdir + "/node1.output")])

dag.add(tag="node2",
        cmd=["ls", "-ltrh", "/ua/nathanae/"],
        files=[nfwq.input_file(args.workdir + "/node1.output")],
        parents=["node1"])
