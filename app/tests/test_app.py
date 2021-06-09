#!flask/bin/python
from unittest import TestCase
import sys, os
from unittest.mock import patch, MagicMock

sys.path.append(os.path.abspath(os.path.join('..')))

from app import liveness_check, health_check, get_files
from werkzeug.exceptions import HTTPException


class AppTests(TestCase):

    def test_startup(self):
        self.assertEqual(liveness_check(), "OK")

    def test_health_check(self):
        with patch('app.db.session.execute', return_value=None):
            mock = MagicMock()
            mock.version.return_value = None
            with patch('app.memcached_client', return_value=mock) as mock_obj:
                # mock_obj.side_effect = Exception("test exception")
                self.assertEqual(health_check(), "OK")

    def test_health_check_failure(self):
        with patch('app.db.session.execute', return_value=None):
            with patch('app.memcached_client', return_value=MagicMock()) as mock_obj:
                mock_obj.side_effect = Exception("Mock exception")
                with self.assertRaises(HTTPException):
                    health_check()


    def test_get_files(self):
        query_mock = MagicMock()
        data_mock = MagicMock()
        data_mock.contents = "test data"
        with patch('app.File') as mock_obj:
            mock_obj.query = query_mock
            query_mock.get_or_404.return_value = data_mock
            self.assertEqual(get_files('2'), "test data")

