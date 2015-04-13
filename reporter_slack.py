import sys
import json

import requests

from settings import RackspaceStoredSettings


class ReportSlacker(RackspaceStoredSettings):

    def __init__(self, *args, **kwargs):
        super(ReportSlacker, self).__init__(*args, **kwargs)
        try:
            self.webhook_url = self.setting('SLACK_WEBHOOK')
        except KeyError:
            try:
                self.webhook_url = self.settings['slack_webhook']
            except KeyError:
                raise EnvironmentError('Backuper: Setting `SLACK_WEBHOOK` is not defined in the environment')

    def send_webhook_request(self, text, channel='#logs', username='backup-bot', icon_emoji=':mega:', **kwargs):
        message = {'text': text, 'channel': channel, 'username': username, 'icon_emoji': icon_emoji, }
        message.update(kwargs)
        return requests.post(self.webhook_url, data=json.dumps(message), headers={'content-type': 'application/json'})

    def report_stdin(self):
        return self.send_webhook_request(text='\n'.join([line.strip() for line in sys.stdin.readlines()]))


def main():
    """
    Script's entry point
    """
    slack_response = ReportSlacker().report_stdin()
    print slack_response.status_code, slack_response.content


if __name__ == '__main__':
    main()
