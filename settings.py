from os import environ

import json
import pyrax


_DEFAULT = object()


class RackspaceStoredSettings(object):
    """
    Base abstract class that reads settings from a JSON file in Cloudfiles and
    also from the process environment.
    """

    cloudfiles_container = 'backups'
    settings_object_name = 'backuper_settings.json'

    def __init__(self, *args, **kwargs):
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
