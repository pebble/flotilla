import unittest
from mock import MagicMock, patch

from io import BytesIO
from tarfile import TarFile, TarInfo

from flotilla.cli.revision import add_revision, files_from_tar, parse_env, \
    get_units

ENVIRONMENT = 'test'
REGIONS = ('us-east-1',)
SERVICE = 'testapp'
LABEL = 'test-revision'


class TestRevision(unittest.TestCase):
    def test_files_from_tar(self):
        tar_buf = self.generate_tar({'test.txt': 'test'})

        tar_contents = files_from_tar(tar_buf)
        self.assertEquals(len(tar_contents), 1)
        self.assertEquals(tar_contents['test.txt'], 'test')

    def test_files_from_tar_tty(self):
        mock_stream = MagicMock(spec=BytesIO)
        mock_stream.isatty.return_value = True
        tar_contents = files_from_tar(mock_stream)

        self.assertEquals(tar_contents, {})

    def test_files_from_tar_error(self):
        mock_stream = MagicMock(spec=BytesIO)
        mock_stream.isatty.return_value = False
        mock_stream.read.side_effect = Exception('kaboom')
        tar_contents = files_from_tar(mock_stream)

        self.assertEquals(tar_contents, {})

    def test_load_env(self):
        env = parse_env('FOO=bar')
        self.assertEquals(env, {'FOO': 'bar'})

    def test_load_env_comment(self):
        env = parse_env('#FOO=bar')
        self.assertEquals(env, {})

    def test_load_env_garbage(self):
        env = parse_env('FOObar')
        self.assertEquals(env, {})

    def test_load_env_multi(self):
        env = parse_env('''FOO=bar
BAZ=bing''')
        self.assertEquals(env, {'FOO': 'bar', 'BAZ': 'bing'})

    def test_get_units_service(self):
        units = get_units({'test.service': 'test'})
        self.assertEquals(len(units), 1)
        unit = units[0]
        self.assertEquals(unit.name, 'test.service')
        self.assertEquals(unit.unit_file, 'test')
        self.assertEquals(unit.unit_hash,
                          '908d6cd4f6fc4f6e9aff0e86d40fb3931495ea2bb80ef9fb3c30f3baec9f3a7c')

    def test_get_units_environment(self):
        units = get_units({
            'test.service': 'test',
            'test.env': 'FOO=bar'
        })
        self.assertEquals(len(units), 1)
        unit = units[0]

        self.assertEquals(unit.environment, {'FOO': 'bar'})
        self.assertEquals(unit.unit_hash,
                          '77be07ce134644a608226b7906fa20298fab9db9d43e815cda35b4a64be95585')

    def test_get_units_docker(self):
        units = get_units({
            'test.env': self.env_file({
                'DOCKER_IMAGE': 'redis',
                'DOCKER_PORT_80': 5601,
                'DOCKER_LOG_DRIVER': 'fluentd'
            })
        })
        self.assertEquals(len(units), 1)
        unit = units[0]
        self.assertEquals(unit.name, 'test.service')
        self.assertTrue(unit.unit_file.find('docker pull redis'))
        self.assertTrue(unit.unit_file.find(' -p 80:5601 '))
        self.assertEquals(unit.environment, {})

    def test_get_units_env_only(self):
        units = get_units({'test.env': ''})
        self.assertEquals(len(units), 0)

    def test_get_units_docker_invalid_port(self):
        units = get_units({
            'test.env': self.env_file({
                'DOCKER_IMAGE': 'redis',
                'DOCKER_PORT_80': 'kaboom',
            })
        })
        self.assertEquals(len(units), 1)
        unit = units[0]
        self.assertEquals(unit.environment, {'DOCKER_PORT_80': 'kaboom'})

    @patch('flotilla.cli.revision.DynamoDbTables')
    @patch('boto.dynamodb2.connect_to_region')
    @patch('boto.kms.connect_to_region')
    def test_add_revision(self, kms, dynamo, tables):
        mock_input = self.generate_tar({
            'test.env': self.env_file({
                'DOCKER_IMAGE': 'redis'
            })
        })
        add_revision(ENVIRONMENT, REGIONS, SERVICE, LABEL, mock_input)

        self.assertEquals(kms.call_count, len(REGIONS))

    @staticmethod
    def generate_tar(entries):
        tar_buf = BytesIO()
        tar_file = TarFile(mode='w', fileobj=tar_buf)
        for path, contents in entries.items():
            tar_info = TarInfo(name=path)
            tar_info.size = len(contents)
            tar_file.addfile(tar_info, fileobj=BytesIO(contents))
        return BytesIO(tar_buf.getvalue())

    @staticmethod
    def env_file(env):
        return '\n'.join(['%s=%s' % (k, v) for k, v in env.items()])
