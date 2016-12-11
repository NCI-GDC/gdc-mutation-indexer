import unittest

from config import BaseConfig


class TestConfig(unittest.TestCase):

    def test_properties(self):
        ''' Test that configuration properties are present '''
        conf = BaseConfig()
        self.assertIn('api_host', dir(conf))
        self.assertIn('signpost_host', dir(conf))
        self.assertIn('s3_host', dir(conf))
        self.assertIn('es_host', dir(conf))
