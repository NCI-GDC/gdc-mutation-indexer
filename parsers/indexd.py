from parsers.base import BaseParser


class IndexdArgs(BaseParser):
    """
    Indexd arguments
    """

    @property
    def group(self):
        return {
            'title': 'Indexd arguments',
            'description': 'Indexd host, port and credentials',
        }

    @property
    def arguments(self):
        return {
            'indexd-host': {
                'help': 'Indexd server hostname',
                'default': 'indexd.service.consul',
            },
            'indexd-port': {
                'help': 'Indexd server port',
                'default': 80,
                'type': int,
            },
            'indexd-user': {
                'help': 'Indexd server username',
                'required': True,
            },
            'indexd-pass': {
                'help': 'Indexd server password',
                'required': True,
            },
        }
