import unittest
from flotilla.model import FlotillaUnit, FlotillaDockerService, \
    FlotillaServiceRevision

UNIT_NAME = 'test.service'
UNIT_FILE = '''[Unit]
Foo=Bar
'''
UNIT_HASH = 'd26616994b142ea1b91a5839600bf8972aaa1758656d84e783c8e4ab6dd54afa'
UNIT_FULL_NAME = 'flotilla-test-d26616994b142ea1b91a5839600bf8972aaa1758656d' \
                 '84e783c8e4ab6dd54afa.service'


class TestFlotillaUnit(unittest.TestCase):
    def setUp(self):
        self.unit = FlotillaUnit(UNIT_NAME, UNIT_FILE)

    def test_constructor(self):
        self.assertEqual(self.unit.name, UNIT_NAME)
        self.assertEqual(self.unit.unit_file, UNIT_FILE)

    def test_unit_hash(self):
        self.assertEqual(self.unit.unit_hash, UNIT_HASH)

    def test_to_string(self):
        self.assertTrue(self.unit.__str__().find(UNIT_NAME) > -1)

    def test_environment(self):
        self.unit = FlotillaUnit(UNIT_NAME, UNIT_FILE, environment={
            'foo': 'bar'
        })
        self.assertNotEqual(self.unit.unit_hash, UNIT_HASH)

    def test_full_name(self):
        self.assertEqual(self.unit.full_name, UNIT_FULL_NAME)


DOCKER_NAME = 'redis'
DOCKER_IMAGE = 'redis:latest'


class TestFlotillaDockerService(unittest.TestCase):
    def setUp(self):
        self.unit = FlotillaDockerService(DOCKER_NAME, DOCKER_IMAGE)

    def test_constructor(self):
        self.assertEqual(self.unit.name, DOCKER_NAME)
        pull_cmd = 'docker pull %s' % DOCKER_IMAGE
        self.assertTrue(pull_cmd in self.unit.unit_file)

    def test_constructor_ports(self):
        self.unit = FlotillaDockerService(DOCKER_NAME, DOCKER_IMAGE, ports={
            80: 8080,
            443: 8443
        })

        docker_ports = ' -p 80:8080 -p 443:8443 '
        self.assertTrue(docker_ports in self.unit.unit_file)

    def test_constructor_environment(self):
        self.unit = FlotillaDockerService(DOCKER_NAME, DOCKER_IMAGE,
                                          environment={'foo': 'bar'})
        docker_env = ' --env-file '
        self.assertTrue(docker_env in self.unit.unit_file)


REV_LABEL = 'initial'
REV_HASH = 'd6a3935f96ba53a42a1ab30ab3d492d726ef9cfc8ae5cf038cf83c14d00a3637'


class TestFlotillaServiceRevision(unittest.TestCase):
    def setUp(self):
        self.unit = FlotillaUnit(UNIT_NAME, UNIT_FILE)
        self.revision = FlotillaServiceRevision(label=REV_LABEL,
                                                units=[self.unit])

    def test_constructor(self):
        self.assertEqual(self.revision.label, REV_LABEL)
        self.assertEqual(self.revision.weight, 1)
        self.assertEqual(self.revision.units, [self.unit])

    def test_revision_hash(self):
        self.assertEqual(self.revision.revision_hash, REV_HASH)
