# -*- coding: utf-8 -*-
from __future__ import absolute_import

import dateutil.parser
import pytest
from mock import MagicMock
from mock import patch

from log_feeder.onelogin_feeder import OneLoginFeeder
from tests.sqs_feeder_test import SharedTestLogFeeder


@pytest.fixture
def olf_test_obj():
    options = MagicMock()
    options.config_path = 'path/config.yaml'
    options.instance_name = 'Test'
    options.start_time = None
    options.end_time = None
    options.relative_start_time = None
    options.relative_end_time = None
    options.stateless = False
    options.hidden_tag = False
    options.no_output = False
    olf = OneLoginFeeder(options=options)
    olf.domain = 'test_domain.com'
    return olf


class TestSharedOneLogin(SharedTestLogFeeder):

    def test_sub_api_run(self, olf_test_obj):
        olf_test_obj.write_latest_event_time = MagicMock()
        make_queries_output = [[{
            'app-id': 1, 'app-name': 2, 'custom-message': 3, 'actor-user-id': 4, 'id': 5, 'account-id': 6,
            'event-type-id': 7, 'notes': 8, 'created-at': '2000-01-01T00:00:00Z', 'actor-user-name': 9,
            'user-name': 10, 'ipaddr': 11,
        }]]
        expected = [{
            '{0}_data'.format(olf_test_obj.APP_NAME): {
                'app-id': 1, 'app-name': 2, 'custom-message': 3, 'actor-user-id': 4, 'id': 5, 'account-id': 6,
                'event-type-id': 7, 'notes': 8, 'actor-user-name': 9,
                'user-name': 10, 'ipaddr': 11,
            },
            'logfeeder_type': 'onelogin',
            'event_time': '2000-01-01T00:00:00+00:00',
            'logfeeder_subapi': olf_test_obj.APP_NAME,
            'logfeeder_account': 'test_domain.com',
            'logfeeder_instance': 'green',
        }]
        self.sub_api_run_tester(olf_test_obj, olf_test_obj.APP_NAME, 'green', make_queries_output, expected)


class TestOneLogin(object):

    EX_FILENAME = "non-existent-made-up-file.abc"
    TEST_PAGE_1 = '<?xml version="1.0" encoding="UTF-8"?>\n<nil-classes type="array"/>'
    TEST_PAGE_2 = """<?xml version="1.0" encoding="UTF-8"?>
<events type="array">
  <event>
    <account-id>12345</account-id>
    <created-at>2015-04-22T16:22:04-07:00</created-at>
    <custom-message nil="true"></custom-message>
    <ipaddr>123.456.789.987</ipaddr>
    <notes>Initiated by OneLogin via SAML</notes>
    <user-name>John Doe</user-name>
    <event-type-id>1</event-type-id>
  </event>
  <event>
    <created-at>1900-04-22T16:22:04-07:00</created-at>
    <user-name>Won't be returned</user-name>
  </event>
</events>
"""
    JSON_RESULTS_2 = [{
        'custom-message': None,
        'account-id': '12345',
        'ipaddr': '123.456.789.987',
        'user-name': 'John Doe',
        'notes': 'Initiated by OneLogin via SAML',
        'created-at': '2015-04-22T16:22:04-07:00',
        'event-type-id': '1',
        'event-type-description': 'App added to role',
    }]
    TEST_PAGE_3a = """<?xml version="1.0" encoding="UTF-8"?>
<events type="array">
  <event>
    <account-id>9</account-id>
    <created-at>3999-01-01T00:00:00-07:00</created-at>
    <event-type-id>1</event-type-id>
  </event>
  <event>
    <account-id>8</account-id>
    <created-at>2088-01-01T00:00:00-07:00</created-at>
    <event-type-id>1</event-type-id>
  </event>
  <event>
    <account-id>7</account-id>
    <created-at>2077-01-01T00:00:00-07:00</created-at>
    <event-type-id>1</event-type-id>
  </event>
</events>
"""
    TEST_PAGE_3b = """<?xml version="1.0" encoding="UTF-8"?>
<events type="array">
  <event>
    <account-id>6</account-id>
    <created-at>2066-01-01T00:00:00-07:00</created-at>
    <event-type-id>1</event-type-id>
  </event>
  <event>
    <account-id>5</account-id>
    <created-at>2055-01-01T00:00:00-07:00</created-at>
    <event-type-id>1</event-type-id>
  </event>
  <event>
    <account-id>4</account-id>
    <created-at>2044-01-01T00:00:00-07:00</created-at>
    <event-type-id>1</event-type-id>
  </event>
</events>
"""
    TEST_PAGE_3c = """<?xml version="1.0" encoding="UTF-8"?>
<events type="array">
  <event>
    <account-id>3</account-id>
    <created-at>2033-01-01T00:00:00-07:00</created-at>
    <event-type-id>1</event-type-id>
  </event>
  <event>
    <account-id>2</account-id>
    <created-at>1922-01-01T00:00:00-07:00</created-at>
    <event-type-id>1</event-type-id>
  </event>
  <event>
    <account-id>1</account-id>
    <created-at>1911-01-01T00:00:00-07:00</created-at>
    <event-type-id>1</event-type-id>
  </event>
</events>
"""
    TEST_PAGE_3d = """<?xml version="1.0" encoding="UTF-8"?>
<events type="array">
  <event>
    <account-id>0</account-id>
    <created-at>1900-01-01T00:00:00-07:00</created-at>
    <event-type-id>1</event-type-id>
  </event>
</events>
"""
    JSON_RESULTS_3 = [
        {
            'account-id': '3',
            'created-at': '2033-01-01T00:00:00-07:00',
            'event-type-id': '1',
            'event-type-description': 'App added to role',
        },
        {
            'account-id': '4',
            'created-at': '2044-01-01T00:00:00-07:00',
            'event-type-id': '1',
            'event-type-description': 'App added to role',
        },
        {
            'account-id': '5',
            'created-at': '2055-01-01T00:00:00-07:00',
            'event-type-id': '1',
            'event-type-description': 'App added to role',
        },
        {
            'account-id': '6',
            'created-at': '2066-01-01T00:00:00-07:00',
            'event-type-id': '1',
            'event-type-description': 'App added to role',
        },
        {
            'account-id': '7',
            'created-at': '2077-01-01T00:00:00-07:00',
            'event-type-id': '1',
            'event-type-description': 'App added to role',
        },
        {
            'account-id': '8',
            'created-at': '2088-01-01T00:00:00-07:00',
            'event-type-id': '1',
            'event-type-description': 'App added to role',
        },
    ]
    TEST_PAGE_4 = """<?xml version="1.0" encoding="UTF-8"?>
<events type="array">
  <event>
    <account-id>1</account-id>
    <created-at>2099-01-01T17:00:01-07:00</created-at>
    <event-type-id>1</event-type-id>
  </event>
  <event>
    <account-id>2</account-id>
    <created-at>2099-01-01T17:00:00-07:00</created-at>
    <event-type-id>1</event-type-id>
  </event>
  <event>
    <account-id>3</account-id>
    <created-at>2000-01-01T17:00:00-07:00</created-at>
    <event-type-id>1</event-type-id>
  </event>
  <event>
    <account-id>4</account-id>
    <created-at>2000-01-01T16:59:59-07:00</created-at>
    <event-type-id>1</event-type-id>
  </event>
</events>
"""
    JSON_RESULTS_4 = [
        {
            'account-id': '3',
            'created-at': '2000-01-01T17:00:00-07:00',
            'event-type-id': '1',
            'event-type-description': 'App added to role',
        },
        {
            'account-id': '2',
            'created-at': '2099-01-01T17:00:00-07:00',
            'event-type-id': '1',
            'event-type-description': 'App added to role',
        },
    ]

    def test_read_api_creds(self, olf_test_obj):
        with patch('log_feeder.onelogin_feeder.read_yaml_file', return_value={'api_key': 'abc'}):
            expected = {'onelogin_key': 'abc'}
            actual = olf_test_obj.read_api_creds('fake_filename')
            assert expected == actual

    def helper_make_queries_test(self, olf_test_obj, expected_records):
        records_generator = olf_test_obj._make_queries_wrapper(
            '2000-01-02T00:00:00Z', '2099-01-02T00:00:00Z', {'onelogin_key': 'abc'}, olf_test_obj.APP_NAME,
        )
        for actual_records in records_generator:
            actual_records = list(actual_records)
            actual_records.sort(key=lambda rec: rec[olf_test_obj.TIMESTAMP_KEYNAME])
            assert actual_records == expected_records

    def test__make_queries_no_entries(self, olf_test_obj):
        # Tests a query that returns no events because the API has run out of events to return (e.g. if page_num=99999)
        olf_test_obj.query_page = MagicMock(return_value=(200, TestOneLogin.TEST_PAGE_1))
        self.helper_make_queries_test(olf_test_obj, expected_records=[])

    def test__make_queries_one_event(self, olf_test_obj):
        # Tests a query with 1 event (in the specified time range) as a result
        olf_test_obj.query_page = MagicMock(return_value=(200, TestOneLogin.TEST_PAGE_2))
        self.helper_make_queries_test(olf_test_obj, expected_records=TestOneLogin.JSON_RESULTS_2)

    def test__make_queries_three_events(self, olf_test_obj):
        # Tests a query with 3 pages of results, some of which don't fall in the time-range
        olf_test_obj.query_page = MagicMock(side_effect=[
            (200, TestOneLogin.TEST_PAGE_3a),
            (500, 'fake_xml'),
            (200, TestOneLogin.TEST_PAGE_3b),
            (200, TestOneLogin.TEST_PAGE_3c),
            (200, TestOneLogin.TEST_PAGE_3d),
        ])
        self.helper_make_queries_test(olf_test_obj, expected_records=TestOneLogin.JSON_RESULTS_3)

    def test__make_queries_boundary_events(self, olf_test_obj):
        # Tests a query with 4 results in one page, two of which fall right on the time boundary and two
        # of which fall right outside the time boundary
        olf_test_obj.query_page = MagicMock(return_value=(200, TestOneLogin.TEST_PAGE_4))
        self.helper_make_queries_test(olf_test_obj, expected_records=TestOneLogin.JSON_RESULTS_4)

    def test__make_queries_five_err_pages(self, olf_test_obj):
        # Query returns 5 errors pages causing a ValueError to be raised
        olf_test_obj.query_page = MagicMock(side_effect=[
            (200, TestOneLogin.TEST_PAGE_3a),
            (500, 'fake_xml'),
            (500, 'fake_xml'),
            (500, 'fake_xml'),
            (500, 'fake_xml'),
            (500, 'fake_xml'),
            (200, TestOneLogin.TEST_PAGE_3b),
        ])
        with pytest.raises(ValueError):
            self.helper_make_queries_test(olf_test_obj, expected_records=None)

    def test_query_page(self, olf_test_obj):
        results = 'test_results'
        with patch(
            'log_feeder.onelogin_feeder.BytesIO',
            return_value=MagicMock(getvalue=MagicMock(return_value=results))
        ):
            actual = olf_test_obj.query_page(MagicMock(), 'page1')[1]
            expected = results
            assert actual == expected

    def test_add_event_descriptions(self, olf_test_obj):
        olf_test_obj.event_id_to_description = {'1': 'App added to role'}
        records = [
            {'event-type-id': '1'},
            {'event-type-id': '999'},
        ]
        actual = olf_test_obj.add_event_descriptions(records)
        expected = [
            {'event-type-id': '1', 'event-type-description': 'App added to role'},
            {'event-type-id': '999', 'event-type-description': 'No Event Description Provided by OneLogin'},
        ]
        assert actual == expected

    def test_convert_dt_to_api_timestring(self, olf_test_obj):
        input = dateutil.parser.parse("2001-02-03T00:00:00Z")
        actual = olf_test_obj.convert_dt_to_api_timestring(input)
        expected = "2001-02-03T00:00:00+00:00"
        assert actual == expected

    def test_convert_api_timestring_to_iso8601(self, olf_test_obj):
        input = "2001-02-03T00:00:00-07:00"
        actual = olf_test_obj.convert_api_timestring_to_iso8601(input)
        expected = "2001-02-03T07:00:00+00:00"
        assert actual == expected
