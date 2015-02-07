#!/usr/bin/env python

"""
Simple database backup utility. Dumps any accessible MySQL (at the moment)
database into a file, "gzip" it, and uploads it to Cloudfiles.
"""

from __future__ import print_function

from datetime import datetime
from argparse import ArgumentParser
# from posixpath import join as path_join
from subprocess32 import check_output, STDOUT, CalledProcessError
from tempfile import NamedTemporaryFile
import gzip

import pyrax
from slugify import slugify
from dateutil.parser import parse as parse_datetime
from dj_database_url import parse as parse_db_url, SCHEMES as DB_ENGINES


from settings import RackspaceStoredSettings


def println(*args, **kwargs):
    """
    Simple `print` stub to include a timestamp of the message being printed.
    """
    print(*tuple([datetime.now()] + list(args)), **kwargs)


class Backuper(RackspaceStoredSettings):
    """
    Backup (dump) creator class.
    """

    cloudfiles_container = backups_container = 'backups'
    settings_object_name = backups_settings = 'backuper_settings.json'
    backups_hourly_path = 'hourly'
    backups_daily_path = 'daily'
    backups_monthly_path = 'monthly'
    backups_yearly_path = 'yearly'
    gzip_ext = 'gz'

    def __init__(self, *args, **kwargs):
        self.overwrite = kwargs.get('overwrite', False)
        super(Backuper, self).__init__(*args, **kwargs)

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
        today = datetime.now()
        format_str = '{folder}/{database}/{database}_{timestamp}.sql.{ext}'
        filenames = [
            format_str.format(folder=self.backups_hourly_path, database=connection['NAME'], timestamp=today.strftime('%Y-%m-%d-%H%M%S'), ext=self.gzip_ext),
            format_str.format(folder=self.backups_daily_path, database=connection['NAME'], timestamp=today.strftime('%Y-%m-%d'), ext=self.gzip_ext),
            format_str.format(folder=self.backups_monthly_path, database=connection['NAME'], timestamp=today.strftime('%Y-%m'), ext=self.gzip_ext),
            format_str.format(folder=self.backups_yearly_path, database=connection['NAME'], timestamp=today.strftime('%Y'), ext=self.gzip_ext),
        ]
        with NamedTemporaryFile() as temp_db_file, NamedTemporaryFile() as temp_gzip_file:
            try:
                dump_file_name = filenames[0]
                dump_args = ['mysqldump', '-h', connection['HOST'], ]
                if connection['PORT']:
                    dump_args.extend(['-P', connection['PORT'], ])
                dump_args.extend(['-u', connection['USER'], '--password=' + connection['PASSWORD'], '--result-file=' + temp_db_file.name, connection['NAME'], ])
                check_output(dump_args, stderr=STDOUT)
                temp_db_file.seek(0)
                println('SQL Dump file `{name}` created, compressing it...'.format(name=dump_file_name))

                gzip_file = gzip.GzipFile(fileobj=temp_gzip_file, mode='wb')
                gzip_file.writelines(temp_db_file)
                gzip_file.close()
                temp_gzip_file.seek(0)
                println('SQL Dump file `{name}` compressed, uploading it...'.format(name=dump_file_name))
                self.cloudfiles.create_object(self.backups_container, obj_name=dump_file_name, file_or_path=temp_gzip_file)
                println('Done.')

                for filename in filenames[1:]:
                    if not self.overwrite:
                        try:
                            backup_file = self.cloudfiles.get_object(self.backups_container, filename)
                            println('`{filename}` already exists, skipping it.'.format(filename=backup_file.name))
                            continue
                        except pyrax.exceptions.NoSuchObject:
                            pass

                    println('Copying `{src}` to `{dst}`'.format(src=filenames[0], dst=filename))
                    self.cloudfiles.copy_object(self.backups_container, filenames[0], self.backups_container, filename)
                    println('Done.')

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
        hours_to_keep = self.setting('hours_to_keep', 48)
        for obj in self.cloudfiles.list_container_objects(self.backups_container):
            if not obj.name.startswith(self.backups_hourly_path):
                continue
            println('Inspecting {name}...'.format(name=obj.name))
            obj.last_modified_timestamp = parse_datetime(obj.last_modified)
            if (now - obj.last_modified_timestamp).seconds > (hours_to_keep * 3600):
                println('Deleting file `{name}` (timestamp: {timestamp})'.format(
                    name=obj.name,
                    timestamp=obj.last_modified_timestamp,
                ))
                obj.delete()

        days_to_keep = self.setting('days_to_keep', 7)
        for obj in self.cloudfiles.list_container_objects(self.backups_container):
            if not obj.name.startswith(self.backups_daily_path):
                continue
            println('Inspecting {name}...'.format(name=obj.name))
            obj.last_modified_timestamp = parse_datetime(obj.last_modified)
            if (now - obj.last_modified_timestamp).days > days_to_keep:
                println('Deleting file `{name}` (timestamp: {timestamp})'.format(
                    name=obj.name,
                    timestamp=obj.last_modified_timestamp,
                ))
                obj.delete()

        months_to_keep = self.setting('months_to_keep', 24)
        for obj in self.cloudfiles.list_container_objects(self.backups_container):
            if not obj.name.startswith(self.backups_daily_path):
                continue
            println('Inspecting {name}...'.format(name=obj.name))
            obj.last_modified_timestamp = parse_datetime(obj.last_modified)
            if (now - obj.last_modified_timestamp).days > (months_to_keep * 30):
                println('Deleting file `{name}` (timestamp: {timestamp})'.format(
                    name=obj.name,
                    timestamp=obj.last_modified_timestamp,
                ))
                obj.delete()


def main():
    """
    Script's entry point
    """
    parser = ArgumentParser(description='Backuper!')
    parser.add_argument(
        '-o', '--overwrite',
        action='store_true', default=False, dest='overwrite',
        help='Overwrite all files if they exist: On each run the hourly '
             'backup is always created, but if the daily/monthly/yearly '
             'already exists, they are skipped. Use this flag to overwrite '
             'those backups from the newly created hourly backup',
    )

    backuper = Backuper(**vars(parser.parse_args()))
    println('Dumping {count} databases.'.format(count=len(backuper.connections)))
    for connection in backuper.connections:
        println('Dumping `{host}/{db}`...'.format(host=connection['HOST'], db=connection['NAME']))
        backuper.create_dump(connection)
    backuper.house_keeping()


if __name__ == '__main__':
    main()
