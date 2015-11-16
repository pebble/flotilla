import hashlib
import os
import time


class FlotillaUnit(object):
    """Systemd unit file and configuration (environment variables)"""

    def __init__(self, name, unit_file, environment={}):
        self.name = name
        self.unit_file = unit_file
        if environment:
            self.environment = environment
        else:
            self.environment = {}

    def __str__(self):
        return 'Unit: %s' % self.name

    @property
    def unit_hash(self):
        unit_hash = hashlib.sha256()
        unit_hash.update(self.name)
        unit_hash.update(self.unit_file)
        for env_key, env_value in sorted(self.environment.items()):
            unit_hash.update(env_key)
            unit_hash.update(str(env_value))
        return unit_hash.hexdigest()

    @property
    def full_name(self):
        name, ext = os.path.splitext(self.name)
        return 'barco-%s-%s%s' % (name, self.unit_hash, ext)


class FlotillaDockerService(FlotillaUnit):
    def __init__(self, name, image, ports={}, environment={}):
        ports_flag = ''
        if ports:
            ports_flag = ' -p ' + ' -p '.join(['%s:%s' % (k, v)
                                              for k, v in ports.items()])
        environment_flag = ''
        if environment:
            environment_flag = " --env-file /etc/flotilla/%n"
        unit_file = """[Unit]
Description={0}

[Service]
User=core
TimeoutStartSec=0
Restart=always
StartLimitInterval=0
ExecStartPre=-/usr/bin/docker pull {1}
ExecStartPre=-/usr/bin/docker kill %n
ExecStartPre=-/usr/bin/docker rm %n
ExecStart=/bin/sh -c "/usr/bin/docker run --rm{3} --name %n{2} {1} > /dev/null 2>&1"
ExecStop=/usr/bin/docker stop %n
""".format(name, image, ports_flag, environment_flag)
        super(FlotillaDockerService, self).__init__(name, unit_file,
                                                    environment)


class FlotillaServiceRevision(object):
    """Weighted collection of units to be deployed together."""

    def __init__(self, label=None, weight=1, units=[]):
        self.label = label or 'rev-%d' % time.time()
        self.weight = weight
        self.units = units


class FlotillaService(object):
    """Collection of weighted revisions of units."""

    def __init__(self, name, units=[]):
        self._name = name
        self._units = units

    @property
    def name(self):
        return self._name
