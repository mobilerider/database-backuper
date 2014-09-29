#!/usr/bin/env python

import json
from posixpath import join as path_join
from os import environ

import pyrax
from mandrill import Mandrill
from dj_database_url import parse as parse_db_url, SCHEMES as DB_ENGINES


_DEFAULT = object()


class Backuper(object):
    backups_container = 'backups'
    backups_settings = 'backuper_settings.json'
    backups_daily_path = 'daily'
    backups_monthly_path = 'daily'

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
        connections = []
        for connection_url in self.settings['databases']:
            connection = parse_db_url(connection_url)
            if connection['ENGINE'] == DB_ENGINES['mysql']:
                connections.append(self.clean_db_connection(connection))
            else:
                raise ValueError('Backuper: `Only MySQL` is supported at the moment: {url}'.format(
                    url=connection_url,
                ))

        return connections


if __name__ == '__main__':
    backuper = Backuper()
    print backuper.connections
