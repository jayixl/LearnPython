import sys

from replica_assignment import ReplicaAssignment


def update_assignments(args):
    assign = ReplicaAssignment(args)
    assign.update_assignment()


def disk_usage(args):
    assign = ReplicaAssignment(args)
    assign.get_top_usage_disks()


def imbalance(args):
    assign = ReplicaAssignment(args)
    assign.parse_partition_distribution()
    assign.get_imbalance_replications()
    assign.print_imbalance_replications()


def distribution(args):
    assign = ReplicaAssignment(args)
    assign.parse_partition_distribution()
    assign.get_imbalance_replications()
    assign.print_broker_topic_distribution(args[-1])


def distribution_detail(args):
    assign = ReplicaAssignment(args)
    assign.parse_partition_distribution()
    assign.get_imbalance_replications()
    assign.print_broker_topic_distribution_details(args[-1])


def usage():
    return "usage:\n" + "update <kafka.env>\n" + "du <kafka.env>\n" + "imbalance <kafka.env>\n" \
          + "distribution <kafka.env> <topic>\n" + "dd <kafka.env> <topic>\n"


def main():
    args = sys.argv[1:]
    if len(args) < 2:
        print usage()
        sys.exit(1)

    action = args[0]

    if action == "update":
        update_assignments(args[1:])
    elif action == "du":
        disk_usage(args[1:])
    elif action == "imbalance":
        imbalance(args[1:])
    elif action == "distribution":
        distribution(args[1:])
    elif action == "dd":
        distribution_detail(args[1:])
    else:
        print usage()


if __name__ == "__main__":
    main()
