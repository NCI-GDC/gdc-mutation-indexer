from utils import SparkTestCase


class TestUtils(SparkTestCase):

    def test_setup(self):
        '''
        Test that spark was setup and context exists
        '''
        self.assertEqual(self.sc.appName, 'TestUtils')
