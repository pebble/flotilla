import unittest
from mock import MagicMock, patch, ANY

from io import BytesIO
from tarfile import TarFile, TarInfo
import json

from flotilla.cli.revision import add_revision, files_from_tar, parse_env, \
    get_units, extract_service_updates, extract_regions, \
    get_services_environments, wait_for_deployment
from flotilla.client.db import FlotillaClientDynamo
from flotilla.scheduler.doctor import ServiceDoctor

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
        units = get_units({'test': 'test'}, {})
        self.assertEquals(len(units), 1)
        unit = units[0]
        self.assertEquals(unit.name, 'test.service')
        self.assertEquals(unit.unit_file, 'test')
        self.assertEquals(unit.unit_hash,
                          '908d6cd4f6fc4f6e9aff0e86d40fb3931495ea2bb80ef9fb3c30f3baec9f3a7c')

    def test_get_units_environment(self):
        units = get_units({
            'test': 'test',
        }, {
            'test': {
                'FOO': 'bar'
            }
        })
        self.assertEquals(len(units), 1)
        unit = units[0]

        self.assertEquals(unit.environment, {'FOO': 'bar'})
        self.assertEquals(unit.unit_hash,
                          '77be07ce134644a608226b7906fa20298fab9db9d43e815cda35b4a64be95585')

    def test_get_units_docker(self):
        units = get_units({}, {
            'test': {
                'DOCKER_IMAGE': 'redis',
                'DOCKER_PORT_80': 5601,
                'DOCKER_LOG_DRIVER': 'fluentd'
            }
        })
        self.assertEquals(len(units), 1)
        unit = units[0]
        self.assertEquals(unit.name, 'test.service')
        self.assertTrue(unit.unit_file.find('docker pull redis'))
        self.assertTrue(unit.unit_file.find(' -p 80:5601 '))
        self.assertEquals(unit.environment, {})

    def test_get_units_env_only(self):
        units = get_units({}, {'test': {}})
        self.assertEquals(len(units), 0)

    def test_get_units_docker_invalid_port(self):
        units = get_units({}, {
            'test': {
                'DOCKER_IMAGE': 'redis',
                'DOCKER_PORT_80': 'kaboom',
            }
        })
        self.assertEquals(len(units), 1)
        unit = units[0]
        self.assertEquals(unit.environment, {'DOCKER_PORT_80': 'kaboom'})

    def test_get_services_environments_service(self):
        services, environments = get_services_environments({
            'test.service': 'test'
        })
        self.assertIn('test', services)
        self.assertEquals(environments, {})

    def test_get_services_environments_env_file(self):
        services, environments = get_services_environments({
            'test.env': self.env_file({
                'MESSAGE': 'hello test'
            })
        })
        self.assertEquals(services, {})
        unit_env = environments['test']
        self.assertEquals(unit_env['MESSAGE'], 'hello test')

    def test_get_services_environments_json(self):
        services, environments = get_services_environments({
            'test.json': json.dumps({
                'flotilla': {
                    'defaults': {
                        'DOCKER_PORT_6379': 6379,
                        'DOCKER_IMAGE': 'redis',
                        'MESSAGE': 'hello default'
                    },
                    ENVIRONMENT: {
                        'MESSAGE': 'hello test'
                    }
                }
            })
        }, ENVIRONMENT)
        self.assertEquals(services, {})
        unit_env = environments['test']
        self.assertEquals(unit_env['MESSAGE'], 'hello test')

    def test_get_services_environments_json_nowrap(self):
        _, environments = get_services_environments({
            'test.json': json.dumps({
                'defaults': {
                    'DOCKER_PORT_6379': 6379,
                    'DOCKER_IMAGE': 'redis',
                    'MESSAGE': 'hello default'
                }
            })
        }, ENVIRONMENT)
        self.assertIn('test', environments)

    def test_get_services_environments_type_safe(self):
        _, environments = get_services_environments({
            'test.json': json.dumps({
                'defaults': {
                    'DOCKER_PORT_6379': 6379,
                    'DOCKER_IMAGE': 'redis',
                    'MESSAGE': {
                        'key': 'abcdef',
                        'ciphertext': 'defg1234'
                    }
                }
            })
        }, ENVIRONMENT)
        unit_env = environments['test']
        self.assertEquals(unit_env['MESSAGE']['key'], 'abcdef')

    def test_get_services_environments_override(self):
        _, environments = get_services_environments({
            'test.json': json.dumps({
                'defaults': {
                    'DOCKER_IMAGE': 'redis',
                    'MESSAGE': 'hello default'
                }
            })
        }, ENVIRONMENT, ['MESSAGE=hello test'])
        unit_env = environments['test']
        self.assertEquals(unit_env['MESSAGE'], 'hello test')

    def test_extract_service_updates(self):
        env = {'INSTANCE_TYPE': 't2.nano',
               'PUBLIC_PORT_1': '80-http',
               'PRIVATE_PORT_1': '9300-tcp',
               'MESSAGE': 'hello test'}
        updates = extract_service_updates([env])
        self.assertEquals({'instance_type': 't2.nano',
                           'public_ports': {80: 'HTTP'},
                           'private_ports': {9300: ['TCP']}
                           }, updates)
        self.assertNotIn('INSTANCE_TYPE', env)
        self.assertNotIn('PUBLIC_PORT_1', env)
        self.assertNotIn('PRIVATE_PORT_1', env)
        self.assertEqual(env['MESSAGE'], 'hello test')

    def test_extract_regions(self):
        env = {'REGION': 'us-east-1,us-west-2'}
        regions = extract_regions([env])
        self.assertEquals(set(('us-east-1', 'us-west-2')), regions)
        self.assertNotIn('REGION', env)

    def test_extract_type_safe(self):
        env = {'REGION': ['us-east-1', 'us-west-2']}
        regions = extract_regions([env])
        self.assertEquals(set(('us-east-1', 'us-west-2')), regions)

    @patch('flotilla.cli.revision.FlotillaClientDynamo')
    @patch('flotilla.cli.revision.DynamoDbTables')
    @patch('boto.dynamodb2.connect_to_region')
    @patch('boto.kms.connect_to_region')
    def test_add_revision(self, kms, dynamo, tables, db_factory):
        db = MagicMock(spec=FlotillaClientDynamo)
        db_factory.return_value = db
        mock_input = self.generate_tar({
            'test.env': self.env_file({
                'DOCKER_IMAGE': 'redis'
            })
        })
        add_revision(ENVIRONMENT, REGIONS, SERVICE, LABEL, (), 0, mock_input)

        self.assertEquals(kms.call_count, len(REGIONS))
        db.add_revision.assert_called_with(SERVICE, ANY)
        db.configure_service.assert_not_called()

    @patch('flotilla.cli.revision.FlotillaClientDynamo')
    @patch('flotilla.cli.revision.DynamoDbTables')
    @patch('boto.dynamodb2.connect_to_region')
    @patch('boto.kms.connect_to_region')
    def test_add_revision_service_update(self, kms, dynamo, tables, db_factory):
        db = MagicMock(spec=FlotillaClientDynamo)
        db_factory.return_value = db

        mock_input = self.generate_tar({
            'test.env': self.env_file({
                'DOCKER_IMAGE': 'redis',
                'INSTANCE_TYPE': 't2.nano'
            })
        })
        add_revision(ENVIRONMENT, REGIONS, SERVICE, LABEL, (), 0, mock_input)

        db.configure_service.assert_called_with(SERVICE, ANY)

    @patch('flotilla.cli.revision.wait_for_deployment')
    @patch('flotilla.cli.revision.DynamoDbTables')
    @patch('boto.dynamodb2.connect_to_region')
    @patch('boto.kms.connect_to_region')
    def test_add_revision_highlander(self, kms, dynamo, tables, wait_for):
        mock_input = self.generate_tar({
            'test.env': self.env_file({
                'DOCKER_IMAGE': 'redis'
            })
        })
        add_revision(ENVIRONMENT, REGIONS, SERVICE, LABEL, (), 30, mock_input)

        wait_for.assert_called_with(ANY, ANY, ANY, ANY, 30)

    @patch('flotilla.cli.revision.ServiceDoctor')
    @patch('flotilla.cli.revision.boto3')
    @patch('flotilla.cli.revision.sleep')
    def test_wait_for_deployment(self, mock_sleep, boto_factory,
                                 doctor_factory):
        mock_elb = MagicMock()
        boto_factory.client.return_value = mock_elb
        mock_doctor = MagicMock(spec=ServiceDoctor)
        mock_doctor.db = MagicMock()
        mock_doctor.is_healthy_revision.side_effect = [
            False,
            False,
            True
        ]
        doctor_factory.return_value = mock_doctor
        dynamo_cache = {region: MagicMock() for region in REGIONS}
        rev = '000000'

        wait_for_deployment(dynamo_cache, REGIONS, SERVICE, rev, 1)

        self.assertEqual(2, mock_sleep.call_count)
        mock_doctor.db.make_only_revision.assert_called_with(SERVICE, rev)
        mock_doctor.db.set_services.assert_not_called()

    @patch('flotilla.cli.revision.ServiceDoctor')
    @patch('flotilla.cli.revision.boto3')
    @patch('flotilla.cli.revision.sleep')
    def test_wait_for_deployment_timeout(self, mock_sleep, boto_factory,
                                         doctor_factory):
        rev = '000000'
        mock_elb = MagicMock()
        boto_factory.client.return_value = mock_elb
        mock_doctor = MagicMock(spec=ServiceDoctor)
        mock_doctor.db = MagicMock()
        mock_doctor.db.get_service.return_value = {
            rev: 1
        }
        mock_doctor.is_healthy_revision.return_value = False
        doctor_factory.return_value = mock_doctor
        dynamo_cache = {region: MagicMock() for region in REGIONS}

        wait_for_deployment(dynamo_cache, REGIONS, SERVICE, rev, 0.001)

        mock_doctor.db.make_only_revision.assert_not_called()
        mock_doctor.db.set_services.assert_called_with(ANY)

    @patch('flotilla.cli.revision.ServiceDoctor')
    @patch('flotilla.cli.revision.boto3')
    @patch('flotilla.cli.revision.sleep')
    def test_wait_for_deployment_hard_fail(self, mock_sleep, boto_factory,
                                           doctor_factory):
        rev = '000000'
        mock_elb = MagicMock()
        boto_factory.client.return_value = mock_elb
        mock_doctor = MagicMock(spec=ServiceDoctor)
        mock_doctor.db = MagicMock()
        mock_doctor.db.get_service.return_value = {
            rev: 1
        }
        mock_doctor.is_healthy_revision.side_effect = [
            False,
            ValueError('Hard fail')
        ]
        doctor_factory.return_value = mock_doctor
        dynamo_cache = {region: MagicMock() for region in REGIONS}

        wait_for_deployment(dynamo_cache, REGIONS, SERVICE, rev, 0.001)

        mock_doctor.db.make_only_revision.assert_not_called()
        mock_doctor.db.set_services.assert_called_with(ANY)

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
