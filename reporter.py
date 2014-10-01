import sys
import json

from mandrill import Mandrill

from backuper import RackspaceStoredSettings, Backuper

_DEFAULT = object()


class ReportMailer(RackspaceStoredSettings):
    cloudfiles_container = backups_container = Backuper.backups_container
    settings_object_name = backups_settings = Backuper.backups_settings

    def __init__(self):
        super(ReportMailer, self).__init__()
        try:
            self.mandrill = Mandrill(self.setting('MANDRILL_APIKEY'))
        except KeyError:
            raise EnvironmentError(
                'Backuper: Setting `MANDRILL_APIKEY` is not defined in the environment')

    def read_config(self):
        """
        Returns the deserialized object that contains this script's settings
        """
        return json.loads(self.cloudfiles.fetch_object(self.backups_container, self.backups_settings))

    def send_email(self, content, from_=None, to=None, subject=None, options=None):
        message = {
            'track_opens': False,
            'track_clicks': False,
            'auto_html': False,
            'view_content_link': False,
        }
        if isinstance(options, dict):
            message.update(options)

        message['subject'] = subject or self.settings.get('notify_subject', ' '.join(sys.argv))
        message['text'] = content
        message['from_email'] = from_ if isinstance(from_, basestring) else self.settings['notify_from']
        if isinstance(to, basestring):
            message['to'] = [{'email': to, }]
        else:
            message['to'] = [
                {'email': addr, }
                for addr in (
                    to if isinstance(to, (tuple, list)) else self.settings['notify']
                )
            ]

        return self.mandrill.messages.send(message)

    def report_stdin(self):
        return self.send_email('\n'.join([line.strip() for line in sys.stdin.readlines()]))


def main():
    """
    Script's entry point
    """
    for response in ReportMailer().report_stdin():
        print '{email}: {status}'.format(email=response['email'], status=response['status'])


if __name__ == '__main__':
    main()
