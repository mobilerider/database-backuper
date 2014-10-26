#!/usr/bin/env python

"""
Simple database backup utility. Dumps any accessible MySQL (at the moment)
database into a file, "gzip" it, and uploads it to Cloudfiles.
"""

from __future__ import print_function

from datetime import date, datetime
from os import environ
from posixpath import join as path_join
from subprocess32 import check_output, STDOUT, CalledProcessError
from tempfile import NamedTemporaryFile
import gzip
import json

import pyrax
from slugify import slugify
from dateutil.parser import parse as parse_datetime
from dj_database_url import parse as parse_db_url, SCHEMES as DB_ENGINES


_DEFAULT = object()


def println(*args, **kwargs):
    """
    Simple `print` stub to include a timestamp of the message being printed.
    """
    print(*tuple([datetime.now()] + list(args)), **kwargs)


class RackspaceStoredSettings(object):
    """
    Base abstract class that reads settings from a JSON file in Cloudfiles and
    also from the process environment.
    """

    cloudfiles_container = None
    settings_object_name = None

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
        self.settings = self.read_config()

    @classmethod
    def setting(cls, name, default=_DEFAULT):
        """
        Returns a value from the script's environment. Raises `EnvironmentError`
        if the `name` variable is not found.
        """
        value = environ.get(name, default)
        if value is _DEFAULT:
            raise EnvironmentError(
                '{cls}: Setting `{name}` is not defined in the environment'.format(cls=cls.__name__, name=name))
        return value

    def read_config(self):
        """
        Returns the deserialized object that contains this script's settings
        """
        return json.loads(self.cloudfiles.fetch_object(self.cloudfiles_container, self.settings_object_name))


class Backuper(RackspaceStoredSettings):
    """
    Backup (dump) creator class.
    """

    cloudfiles_container = backups_container = 'backups'
    settings_object_name = backups_settings = 'backuper_settings.json'
    backups_daily_path = 'daily'
    backups_monthly_path = 'monthly'

    @staticmethod
    def clean_db_connection(connection):
        """
        Validates/Cleans a database connection dictionary
        """
        needed_keys = set(['NAME', 'USER', 'PASSWORD', 'HOST', ])
        keys = set([key for key in connection.keys() if bool(key)]).intersection(needed_keys)
        if len(keys) != len(needed_keys):
            raise ValueError('Backuper: Connection `{connection}` is missing this keys: {missing}'.format(
                connection=repr(connection),
                missing=repr(needed_keys.difference(keys)),
            ))
        return connection

    @property
    def connections(self):
        """
        Generates and caches the list of connections read from the settings file
        This settings file must contain a `databases` key which is a dictionary
        where the keys are names/alias (used to generate folder names) and the
        values are the connection urls, in this format:

        "<db-server>://<db-username>:<db-password>@<db-host>/<db-name>"

        Where `db-server` must be `mysql`. Support for other database servers
        may be added in the future.
        """
        try:
            return self._connections
        except AttributeError:
            self._connections = []
            for folder, connection_url in self.settings['databases'].items():
                connection = parse_db_url(connection_url)
                if connection['ENGINE'] == DB_ENGINES['mysql']:
                    self._connections.append(self.clean_db_connection(connection))
                else:
                    raise ValueError('Backuper: `Only MySQL` is supported at the moment: {url}'.format(
                        url=connection_url,
                    ))
                connection['FOLDER'] = slugify(folder)
            return self._connections

    def create_dump(self, connection):
        """
        Creates a backup dump for a single connection dict, like one of the
        `connections` property. This dict also contains a `FOLDER` key, which
        specifies the "folder" where to place the backup file.
        """
        today = date.today()
        with NamedTemporaryFile() as temp_db_file, NamedTemporaryFile() as temp_gzip_file:
            try:
                dump_file_name = path_join(self.backups_daily_path, connection['FOLDER'], connection['NAME'] + '_' + str(today)) + '.sql.gzip'
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
                println('SQL Dump file `{name}` created, compressing it...'.format(name=dump_file_name))

                gzip_file = gzip.GzipFile(fileobj=temp_gzip_file, mode='wb')
                gzip_file.writelines(temp_db_file)
                gzip_file.close()
                temp_gzip_file.seek(0)
                println('SQL Dump file `{name}` compressed, uploading it...'.format(name=dump_file_name))

                self.cloudfiles.create_object(self.backups_container, obj_name=dump_file_name, file_or_path=temp_gzip_file)
                println('Dump file `{container}/{name}` uploaded'.format(container=self.backups_container, name=dump_file_name))

                if today.day == 1:
                    monthly_file_name = path_join(self.backups_monthly_path, connection['FOLDER'], connection['NAME'] + '_' + str(today)) + '.sql.gzip'
                    println('Copying `{src}` to `{dst}` (monthly backup)'.format(
                        src=dump_file_name,
                        dst=monthly_file_name,
                    ))
                    self.cloudfiles.copy_object(self.backups_container, dump_file_name, self.backups_container, monthly_file_name)

            except CalledProcessError, error:
                println(repr(error), error.__dict__)

    def house_keeping(self):
        """
        Checks for all files under `backups_daily_path` and deletes everything
        older than the numbers of days indicated in the `days_to_keep` setting.
        The default value is `7` if this setting is not present.

        WARNING: This is a destructive operation, everything under the
        `backups_daily_path` that is considered old will be deleted, no other
        checks are done besides checking the name prefix (path) and the
        modification date. Do not put anything under this path or this method
        may try to delete it.
        """
        now = datetime.now()
        days_to_keep = self.setting('days_to_keep', 7)
        for obj in self.cloudfiles.list_container_objects(self.backups_container):
            if not obj.name.startswith(self.backups_daily_path):
                continue
            println(obj.name)
            obj.last_modified_timestamp = parse_datetime(obj.last_modified)
            if (now - obj.last_modified_timestamp).days > days_to_keep:
                println('Deleting file `{name}` (timestamp: {timestamp})'.format(
                    name=obj.name,
                    timestamp=obj.last_modified_timestamp,
                ))
                obj.delete()

def main():
    """
    Script's entry point
    """
    backuper = Backuper()
    println('Dumping {count} databases.'.format(count=len(backuper.connections)))
    for connection in backuper.connections:
        println('Dumping `{host}/{db}`...'.format(host=connection['HOST'], db=connection['NAME']))
        backuper.create_dump(connection)
    backuper.house_keeping()


if __name__ == '__main__':
    main()
