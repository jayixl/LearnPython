import ConfigParser
import os
import subprocess
import sys


class FakeSecHead(object):
    FAKE_HEADER = 'default'

    def __init__(self, fp):
        self.fp = fp
        self.sechead = '[{}]\n'.format(self.FAKE_HEADER)

    def readline(self):
        if self.sechead:
            try:
                return self.sechead
            finally:
                self.sechead = None
        else:
            return self.fp.readline()


def parse_properties():
    parser = ConfigParser.ConfigParser()
    property_file = os.environ['SCHEDULER_HOME']
    parser.readfp(FakeSecHead(open(os.path.join(property_file, 'conf/scheduler.env-specific.properties'))))
    return parser.get(FakeSecHead.FAKE_HEADER, "hive.server.host"), parser.get("default", "hive.server.port")


class Adaptor(object):
    HOST, PORT = parse_properties()
    SCHEDULER_HIVE_PROXY_USER = 'SCHEDULER_HIVE_PROXY_USER'
    HIVE_CMD = '${HIVE_HOME}/bin/beeline'

    def __init__(self):
        super(Adaptor, self).__init__()
        self.conn = None
        self.cmd = None
        self.init_db = 'default'
        self.user = 'envuser'

    def parse_cmd_args(self, args):
        options = []
        index = 0
        while index < len(args):
            if args[index] == '-initdb':
                if index + 1 < len(args) and not args[index + 1].startswith('-'):
                    self.init_db = args[index + 1]
                    index += 1
            else:
                arg = args[index]
                if len(arg.split(' ')) > 1:
                    arg = '"{}"'.format(arg)
                options.append(arg)
            index += 1
        if self.SCHEDULER_HIVE_PROXY_USER in os.environ.keys():
            self.user = os.environ[self.SCHEDULER_HIVE_PROXY_USER]
        self.conn = '"jdbc:hive2://{host}:{port}/{init_db};hive.server2.proxy.user={user}"'.format(
            host=self.HOST, port=self.PORT, init_db=self.init_db, user=self.user
        )
        hive_user_options = ' '.join(options)
        self.cmd = '{cmd} -u {conn} {options}'.format(cmd=self.HIVE_CMD, conn=self.conn, options=hive_user_options)

    def execute(self):
        print self.cmd
        subprocess.call(self.cmd, shell=True)


if __name__ == "__main__":
    executor = Adaptor()
    executor.parse_cmd_args(sys.argv[1:])
    executor.execute()
