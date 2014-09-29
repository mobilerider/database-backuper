#!/usr/bin/env python

import gzip
from datetime import date
import json
from posixpath import join as path_join
from os import environ
from tempfile import NamedTemporaryFile
from subprocess import check_output, STDOUT, CalledProcessError

import pyrax
from mandrill import Mandrill
from dj_database_url import parse as parse_db_url, SCHEMES as DB_ENGINES


_DEFAULT = object()


class Backuper(object):
    backups_container = 'backups'
    backups_settings = 'backuper_settings.json'
    backups_daily_path = 'daily'
    backups_monthly_path = 'monthly'

    @staticmethod
    def setting(name, default=_DEFAULT):
        value = environ.get(name, default)
        if value is _DEFAULT:
            raise EnvironmentError('Backuper: Setting `{name}` is not defined in the environment'.format(name=name))
        return value

    def __init__(self):
        pyrax.set_setting('identity_type', self.setting('PYRAX_IDENTITY_TYPE', 'rackspace'))

        pyrax_password = self.setting('PYRAX_PASSWORD', None) or self.setting('PYRAX_APIKEY', None)
        if pyrax_password is None:
            raise EnvironmentError(
                'Backuper: Settings `PYRAX_PASSWORD` and `PYRAX_APIKEY` '
                'are not defined in the environment. Backuper needs at '
                'least one of them.')
        pyrax.set_credentials(self.setting('PYRAX_USERNAME'), pyrax_password)

        self.cloudfiles = pyrax.connect_to_cloudfiles()
        self.mandrill = Mandrill(self.setting('MANDRILL_APIKEY'))
        self.settings = self.read_config()

    def read_config(self):
        return json.loads(self.cloudfiles.fetch_object(self.backups_container, self.backups_settings))

    @staticmethod
    def clean_db_connection(connection):
        needed_keys = set(['NAME', 'USER', 'PASSWORD', 'HOST', ])
        keys = set(connection.keys()).intersection(needed_keys)
        if len(keys) != len(needed_keys):
            raise ValueError('Backuper: Connection `{connection}` is missing this keys: {missing}'.format(
                connection=repr(connection),
                missing=repr(needed_keys.difference(keys)),
            ))
        return connection

    @property
    def connections(self):
        try:
            return self._connections
        except AttributeError:
            self._connections = []
            for connection_url in self.settings['databases']:
                connection = parse_db_url(connection_url)
                if connection['ENGINE'] == DB_ENGINES['mysql']:
                    self._connections.append(self.clean_db_connection(connection))
                else:
                    raise ValueError('Backuper: `Only MySQL` is supported at the moment: {url}'.format(
                        url=connection_url,
                    ))
            return self._connections

    def create_dump(self, connection):
        today = date.today()
        with NamedTemporaryFile() as temp_db_file, NamedTemporaryFile() as temp_gzip_file:
            try:
                dump_args = [
                    'mysqldump',
                    '-h',
                    connection['HOST'],
                ]
                if connection['PORT']:
                    dump_args.extend([
                        '-P',
                        connection['PORT'],
                    ])
                dump_args.extend([
                    '-u',
                    connection['USER'],
                    '--password=' + connection['PASSWORD'],
                    '--result-file=' + temp_db_file.name,
                    connection['NAME'],
                ])
                check_output(dump_args, stderr=STDOUT)
                temp_db_file.seek(0)
                gzip_file = gzip.GzipFile(fileobj=temp_gzip_file, mode='wb')
                gzip_file.writelines(temp_db_file)
                gzip_file.close()
                temp_gzip_file.seek(0)

                daily_file_name = path_join(self.backups_daily_path, connection['HOST'], connection['NAME'] + '_' + str(today)) + '.sql.gzip'
                self.cloudfiles.create_object(self.backups_container, obj_name=daily_file_name, file_or_path=temp_gzip_file)
                print 'Dump file `{name}` created'.format(name=daily_file_name)

                if today.day == 29:
                    monthly_file_name = path_join(self.backups_monthly_path, connection['HOST'], connection['NAME'] + '_' + str(today)) + '.sql.gzip'
                    print 'Copying `{src}` to `{dst}` (monthly backup)'.format(
                        src=daily_file_name,
                        dst=monthly_file_name,
                    )
                    self.cloudfiles.copy_object(self.backups_container, daily_file_name, self.backups_container, monthly_file_name)

            except CalledProcessError, error:
                print repr(error)


if __name__ == '__main__':
    backuper = Backuper()
    print 'Dumping {count} databases.'.format(count=len(backuper.connections))
    for connection in backuper.connections:
        print 'Dumping `{host}/{db}`...'.format(host=connection['HOST'], db=connection['NAME'])
        backuper.create_dump(connection)
