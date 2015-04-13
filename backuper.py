#!/usr/bin/env python

"""
Simple database backup utility. Dumps any accessible MySQL (at the moment)
database into a file, "gzip" it, and uploads it to Cloudfiles.
"""

from __future__ import print_function

from argparse import ArgumentParser
from collections import defaultdict, OrderedDict
from datetime import datetime
from subprocess32 import check_output, STDOUT, CalledProcessError
from tempfile import NamedTemporaryFile
import gzip
import re

from dateutil.parser import parse as parse_datetime
from dj_database_url import parse as parse_db_url, SCHEMES as DB_ENGINES
from slugify import slugify
import pyrax


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
    format_str = '{frequency}/{folder}/{database}_{timestamp}.sql.gz'
    format_re = re.compile(r'^(?P<frequency>[^/]+)/(?P<folder>[^/]+)/(?P<database>[^/_]+)_(?P<timestamp>.+)\.sql\.gz$')

    def __init__(self, *args, **kwargs):
        self.overwrite = kwargs.get('overwrite', False)
        self.verbose = kwargs.get('verbose', False)
        self.dry_run = kwargs.get('dry_run', False)
        self.now = datetime.now()
        super(Backuper, self).__init__(*args, **kwargs)

    def log(self, *args, **kwargs):
        if self.verbose:
            return println(*args, **kwargs)

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
        filenames = [
            self.format_str.format(
                frequency=getattr(self, attr),
                folder=connection['FOLDER'],
                database=connection['NAME'],
                timestamp=self.now.strftime(strftime_format))

            for attr, strftime_format in {
                'backups_hourly_path': '%Y-%m-%d-%H%M%S',
                'backups_daily_path': '%Y-%m-%d',
                'backups_monthly_path': '%Y-%m',
                'backups_yearly_path': '%Y', }.items()
        ]

        with NamedTemporaryFile() as temp_db_file, NamedTemporaryFile() as temp_gzip_file:
            try:
                dump_file_name = filenames[0]
                dump_args = ['mysqldump', '-h', connection['HOST'], ]
                if connection['PORT']:
                    dump_args.extend(['-P', connection['PORT'], ])
                dump_args.extend(['-u', connection['USER'], '--password=' + connection['PASSWORD'], '--result-file=' + temp_db_file.name, connection['NAME'], ])

                if not self.dry_run:
                    check_output(dump_args, stderr=STDOUT)
                    temp_db_file.seek(0)

                self.log('SQL Dump file `{name}` created, compressing it...'.format(name=dump_file_name))

                if not self.dry_run:
                    gzip_file = gzip.GzipFile(fileobj=temp_gzip_file, mode='wb')
                    gzip_file.writelines(temp_db_file)
                    gzip_file.close()
                    temp_gzip_file.seek(0)

                self.log('SQL Dump file `{name}` compressed, uploading it...'.format(name=dump_file_name))

                if not self.dry_run:
                    self.cloudfiles.create_object(self.backups_container, obj_name=dump_file_name, file_or_path=temp_gzip_file)

                self.log('Done.')

                for filename in filenames[1:]:
                    if not self.overwrite:
                        try:
                            backup_file = self.cloudfiles.get_object(self.backups_container, filename)
                            self.log('`{filename}` already exists, skipping it.'.format(filename=backup_file.name))
                            continue
                        except pyrax.exceptions.NoSuchObject:
                            pass

                    self.log('Copying `{src}` to `{dst}`'.format(src=filenames[0], dst=filename))

                    if not self.dry_run:
                        self.cloudfiles.copy_object(self.backups_container, filenames[0], self.backups_container, filename)

                    self.log('Done.')

            except CalledProcessError, error:
                self.log(repr(error), error.__dict__)

    def get_backup_objects(self, prefix=None):
        objects = [
            obj for obj in
            self.cloudfiles.list_container_objects(self.backups_container)
            if not prefix or obj.name.startswith(prefix)]
        for obj in objects:
            timestamp = parse_datetime(obj.last_modified)
            obj.last_modified_timestamp = timestamp
            obj.last_modified_timedelta = (self.now - timestamp)

        return objects

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

        self.log('Inspecting hourly backups...')

        hours_to_keep = self.setting('hours_to_keep', 48)

        for obj in self.get_backup_objects(self.backups_hourly_path):
            if ((obj.last_modified_timedelta.total_seconds() // 60) // 60) > hours_to_keep:
                self.log('Deleting file `{name}` (timestamp: {timestamp})'.format(
                    name=obj.name,
                    timestamp=obj.last_modified_timestamp,
                ))
                if not self.dry_run:
                    obj.delete()

        self.log('Inspecting daily backups...')

        days_to_keep = self.setting('days_to_keep', 7)

        for obj in self.get_backup_objects(self.backups_daily_path):
            if obj.last_modified_timedelta.days > days_to_keep:
                self.log('Deleting file `{name}` (timestamp: {timestamp})'.format(
                    name=obj.name,
                    timestamp=obj.last_modified_timestamp,
                ))
                if not self.dry_run:
                    obj.delete()

        self.log('Inspecting monthly backups...')

        months_to_keep = self.setting('months_to_keep', 24)

        for obj in self.get_backup_objects(self.backups_monthly_path):
            if obj.last_modified_timedelta.days > (months_to_keep * 30):
                self.log('Deleting file `{name}` (timestamp: {timestamp})'.format(
                    name=obj.name,
                    timestamp=obj.last_modified_timestamp,
                ))
                if not self.dry_run:
                    obj.delete()

    def backup_status(self):
        backup_objects = defaultdict(dict)
        status = defaultdict(lambda: defaultdict(dict))

        def get_key_name(obj):
            match = self.format_re.match(obj.name)
            if match:
                groupdict = match.groupdict()
                return groupdict['folder'] + '/' + groupdict['database']

        def get_frequency(obj):
            match = self.format_re.match(obj.name)
            if match:
                groupdict = match.groupdict()
                return groupdict['frequency']

        for obj in self.get_backup_objects():
            key_name = get_key_name(obj)
            if key_name:
                if backup_objects[key_name].get('objects', None) is None:
                    backup_objects[key_name]['objects'] = []
                backup_objects[key_name]['objects'].append(obj)

        for connection, data in backup_objects.items():
            for obj in data['objects']:
                frequency = get_frequency(obj)
                if data.get(frequency, None) is None:
                    data[frequency] = []
                data[frequency].append(obj)

            for obj_list in data.values():
                obj_list.sort(key=lambda x: x.last_modified_timestamp, reverse=True)

        return backup_objects


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
    parser.add_argument(
        '-d', '--dry-run',
        action='store_true', default=False, dest='dry_run',
        help='Do not perform any dump, upload or deletion, only log every action',
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=False, dest='verbose',
        help='Show more output',
    )
    script_args = parser.parse_args()

    backuper = Backuper(**vars(script_args))

    if script_args.verbose:
        print('# Dumping {count} databases.'.format(count=len(backuper.connections)))

    for connection in backuper.connections:
        if script_args.verbose:
            println('Dumping `{host}/{db}`...'.format(
                host=connection['HOST'], db=connection['NAME']))
        backuper.create_dump(connection)

    println('Backups created for: ' + ', '.join([
        (connection['FOLDER'] + '/' + connection['NAME'])
        for connection in backuper.connections]))

    backuper.house_keeping()

    print('# Status:')
    for connection, connection_data in backuper.backup_status().items():
        freqs = OrderedDict((('hourly', '%Y-%m-%d-%H%M%S', ), ('daily', '%Y-%m-%d', ), ('monthly', '%Y-%m'), ('yearly', '%Y', ), ))

        line = []
        for freq, strftime_format in freqs.items():
            line.append('{freq}: {count} ({latest})'.format(
                freq=freq,
                count=len(connection_data['objects']),
                latest=connection_data['objects'][0].last_modified_timestamp.strftime(strftime_format),
            ))
        print(connection, ' '.join(line))


if __name__ == '__main__':
    main()
