import re
import subprocess

import numpy as np


class ReplicaAssignment(object):

    PAT_HOST = r'.*\[SUCCESS\]\s(.*)'
    PAT_DISK_SIZE = r'^(\d+)\t/(data\d+)/kafka$'
    PAT_PARTITION_SIZE = r'^(\d+)\t/(data\d+)/kafka/(.*)\-(\d+)$'

    def __init__(self, args):
        super(ReplicaAssignment, self).__init__()
        self.host_file = args[0]
        self.assignment_file = self.host_file + ".out"
        self.disks = set()
        self.broker_disk_topic_par_size = {}
        self.broker_topic_disk_par_size = {}
        self.imbalance = {}
        self.broker_topic_disk_par_count = {}
        self.broker_topic_disk_size = {}
        self.broker_disk_par_count = {}
        self.broker_disk_size = {}

    def update_assignment(self):
        cmd = 'pssh -v -i -h {hosts} "du /data*/kafka" > {out}'.format(hosts=self.host_file, out=self.assignment_file)
        subprocess.call(cmd, shell=True)

    def get_top_usage_disks(self):
        next_broker = ""
        disks = []
        with open(self.assignment_file) as f:
            for line in f.readlines():
                strip_line = line.strip()
                m = re.match(self.PAT_HOST, strip_line)
                if m is not None:
                    next_broker = m.group(1)

                m = re.match(self.PAT_DISK_SIZE, strip_line)
                if m is not None:
                    disks.append((next_broker, m.group(2), float(m.group(1)) / 1000000))
        for tp in sorted(disks, key=lambda (x, y, z): z, reverse=True):
            print "%s\t%s\t%d" % tp

    def parse_partition_distribution(self):
        next_broker = ""
        with open(self.assignment_file) as f:
            for line in f.readlines():
                strip_line = line.strip()
                m = re.match(self.PAT_HOST, strip_line)
                if m is not None:
                    next_broker = m.group(1)
                m = re.match(self.PAT_PARTITION_SIZE, strip_line)
                if m is not None:
                    size = long(float(m.group(1)) / 1000000)
                    disk = m.group(2)
                    topic = m.group(3)
                    partition = int(m.group(4))

                    self.disks.add(disk)
                    ReplicaAssignment.dict_put(self.broker_disk_topic_par_size, size, next_broker, disk, topic, partition)
                    ReplicaAssignment.dict_put(self.broker_topic_disk_par_size, size, next_broker, topic, disk, partition)

    def get_imbalance_replications(self):
        disk_size = len(self.disks)
        for broker, bv in self.broker_topic_disk_par_size.iteritems():
            for topic, tv in bv.iteritems():
                # broker-topic sizes should be ideally equal. (unless partition skew issue)
                # use stddev to indicate the imbalance
                sizes = []
                for disk, dv in tv.iteritems():
                    sum_size_in_disk = 0L
                    for partition, size in dv.iteritems():
                        sum_size_in_disk += size
                        # print "%s, %s, %s, %s, %d" % (broker, topic, disk, partition, size)
                    sizes.append(sum_size_in_disk)
                    ReplicaAssignment.dict_put(self.broker_topic_disk_size, sum_size_in_disk, broker, topic, disk)
                    ReplicaAssignment.dict_put(self.broker_topic_disk_par_count, len(dv), broker, topic, disk)
                np_sizes = np.concatenate((np.array(sizes), np.zeros(disk_size - len(sizes))))
                stddev = np_sizes.std()
                # print np_sizes
                # print stddev
                key = broker + "-" + topic
                self.imbalance[key] = stddev

        for broker, bv in self.broker_disk_topic_par_size.iteritems():
            for disk, dv in bv.iteritems():
                sum_par_count_in_disk = 0
                sum_size_in_disk = 0L
                for topic, tv in dv.iteritems():
                    for partition, size in tv.iteritems():
                        sum_par_count_in_disk += 1
                        sum_size_in_disk += size
                # key = broker + "-" + disk
                ReplicaAssignment.dict_put(self.broker_disk_par_count, sum_par_count_in_disk, broker, disk)
                ReplicaAssignment.dict_put(self.broker_disk_size, sum_size_in_disk, broker, disk)
                # self.broker_disk_par_count[key] = sum_par_count_in_disk
                # self.broker_disk_size[key] = sum_size_in_disk

    def print_imbalance_replications(self):
        print "*** std-dev:"
        for bt, stddev in sorted(self.imbalance.items(), key=lambda (k, v): (v, k), reverse=True):
            print "%s\t%d" % (bt, stddev)

    def print_broker_topic_distribution(self, topic):
        print "*** topic-distribution:"
        for broker, bv in self.broker_topic_disk_size.iteritems():
            disks = self.disks.copy()
            print broker
            for disk, dv in sorted(bv[topic].iteritems(), key=lambda (k, v): (v, k), reverse=True):
                disks.remove(disk)
                print "%s(%d, %d)\t%d" % (disk, self.broker_disk_size[broker][disk], self.broker_disk_par_count[broker][disk], dv)
            for d in disks:
                print "%s(%d, %d)\t%d" % (d, self.broker_disk_size[broker][d], self.broker_disk_par_count[broker][d], 0)
            print "==="

    def print_broker_topic_distribution_details(self, topic):
        for broker, bv in self.broker_topic_disk_par_size.iteritems():
            if topic not in bv:
                continue
            tv = bv[topic]
            for disk, dv in tv.iteritems():
                sum_size_in_disk = 0L
                for partition, size in dv.iteritems():
                    sum_size_in_disk += size
                    print "%s, %s, %s, %s, %d" % (broker, topic, disk, partition, size)

    @staticmethod
    def dict_put(obj, value, *paths):
        depth = len(paths)
        if depth == 0:
            return
        idx = 0
        point = obj
        for path in paths:
            idx += 1
            if idx == depth:
                point[path] = value
            else:
                if path not in point:
                    point[path] = {}
                point = point[path]

# if __name__ == "__main__":
#     assign = ReplicaAssignment(sys.argv[1:])
#     # assign.get_top_usage_disks()
#     assign.parse_partition_distribution()
#     assign.get_imbalance_replications()
#
#     assign.print_imbalance_replications()
#     assign.print_broker_topic_distribution("eos-mqtt.kafka.data_prod")
