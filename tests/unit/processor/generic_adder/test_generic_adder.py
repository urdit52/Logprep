from logging import getLogger
import time

import pytest
from unittest import mock

pytest.importorskip('logprep.processor.generic_adder')

from logprep.processor.base.exceptions import InvalidRuleFileError

from logprep.processor.generic_adder.factory import GenericAdderFactory
from logprep.processor.generic_adder.processor import DuplicationError


logger = getLogger()
rules_dir = 'tests/testdata/unit/generic_adder/rules'
rules_dir_missing = 'tests/testdata/unit/generic_adder/rules_missing'
rules_dir_invalid = 'tests/testdata/unit/generic_adder/rules_invalid'
rules_dir_first_existing = 'tests/testdata/unit/generic_adder/rules_first_existing'


class MockedDB:
    class Cursor:
        def __init__(self):
            self._checksum = 0
            self._data = []
            self._table_result = [[0, 'TEST_0', 'foo', 'bar'],
                                  [1, 'TEST_1', 'uuu', 'vvv'],
                                  [2, 'TEST_2', '123', '456']]

        def execute(self, statement):
            if statement == 'CHECKSUM TABLE test_table':
                self._data = [self._checksum]
            elif statement == 'desc test_table':
                self._data = [['id'], ['a'], ['b'], ['c']]
            elif statement == 'SELECT * FROM test_table':
                self._data = self._table_result
            else:
                self._data = []

        def mock_simulate_table_change(self):
            self._checksum += 1
            self._table_result[0] = [0, 'TEST_0', 'fi', 'fo']

        def mock_clear_all(self):
            self._checksum = 0
            self._data = []
            self._table_result = []

        def __next__(self):
            return self._data

        def __iter__(self):
            return iter(self._data)

    def cursor(self):
        return self.Cursor()

    def commit(self):
        pass


@pytest.fixture()
@mock.patch('mysql.connector.connect',
            mock.MagicMock(return_value=MockedDB()))
def generic_adder():
    config = {
        'type': 'generic_adder',
        'rules': [rules_dir],
        'tree_config': 'tests/testdata/unit/shared_data/tree_config.json',
        'sql_config': {
            'user': 'test_user',
            'password': 'foo_bar_baz',
            'host': '127.0.0.1',
            'database': 'test_db',
            'table': 'test_table',
            'target_column': 'a',
            'timer': 0.1
        }
    }

    generic_adder = GenericAdderFactory.create('test-generic-adder', config, logger)
    return generic_adder


@pytest.fixture()
@mock.patch('mysql.connector.connect',
            mock.MagicMock(return_value=MockedDB()))
def generic_adder_with_target():
    config = {
        'type': 'generic_adder',
        'rules': [rules_dir],
        'tree_config': 'tests/testdata/unit/shared_data/tree_config.json',
        'sql_config': {
            'user': 'test_user',
            'password': 'foo_bar_baz',
            'host': '127.0.0.1',
            'database': 'test_db',
            'table': 'test_table',
            'target_column': 'a',
            'add_target_column': True,
            'timer': 0.1
        }
    }

    generic_adder = GenericAdderFactory.create('test-generic-adder', config, logger)
    return generic_adder


class TestGenericAdder:
    def test_add_generic_fields(self, generic_adder):
        assert generic_adder.ps.processed_count == 0
        expected = {
            'add_generic_test': 'Test', 'event_id': 123,
            'some_added_field': 'some value',
            'another_added_field': 'another_value',
            'dotted': {'added': {'field': 'yet_another_value'}}
        }
        document = {'add_generic_test': 'Test', 'event_id': 123}

        generic_adder.process(document)

        assert document == expected

    def test_add_generic_fields_from_file(self, generic_adder):
        assert generic_adder.ps.processed_count == 0
        expected = {
            'add_list_generic_test': 'Test', 'event_id': 123,
            'some_added_field': 'some value',
            'another_added_field': 'another_value',
            'dotted': {'added': {'field': 'yet_another_value'}}
        }
        document = {'add_list_generic_test': 'Test', 'event_id': 123}

        generic_adder.process(document)

        assert document == expected

    def test_add_generic_fields_from_file_list_one_element(self, generic_adder):
        assert generic_adder.ps.processed_count == 0
        expected = {
            'add_lists_one_generic_test': 'Test', 'event_id': 123,
            'some_added_field': 'some value',
            'another_added_field': 'another_value',
            'dotted': {'added': {'field': 'yet_another_value'}}
        }
        document = {'add_lists_one_generic_test': 'Test', 'event_id': 123}

        generic_adder.process(document)

        assert document == expected

    def test_add_generic_fields_from_file_list_two_elements(self, generic_adder):
        assert generic_adder.ps.processed_count == 0
        expected = {
            'add_lists_two_generic_test': 'Test', 'event_id': 123,
            'added_from_other_file': 'some field from another file',
            'some_added_field': 'some value',
            'another_added_field': 'another_value',
            'dotted': {'added': {'field': 'yet_another_value'}}
        }
        document = {'add_lists_two_generic_test': 'Test', 'event_id': 123}

        generic_adder.process(document)

        assert document == expected

    def test_add_generic_fields_from_file_first_existing(self):
        config = {
            'type': 'generic_adder',
            'rules': [rules_dir_first_existing],
            'tree_config': 'tests/testdata/unit/shared_data/tree_config.json'
        }

        generic_adder = GenericAdderFactory.create('test-generic-adder', config, logger)

        assert generic_adder.ps.processed_count == 0
        expected = {
            'add_first_existing_generic_test': 'Test', 'event_id': 123,
            'some_added_field': 'some value',
            'another_added_field': 'another_value',
            'dotted': {'added': {'field': 'yet_another_value'}}
        }
        document = {'add_first_existing_generic_test': 'Test', 'event_id': 123}

        generic_adder.process(document)

        assert document == expected

    def test_add_generic_fields_from_file_first_existing_with_missing(self):
        config = {
            'type': 'generic_adder',
            'rules': [rules_dir_first_existing],
            'tree_config': 'tests/testdata/unit/shared_data/tree_config.json'
        }

        generic_adder = GenericAdderFactory.create('test-generic-adder', config, logger)

        assert generic_adder.ps.processed_count == 0
        expected = {
            'add_first_existing_with_missing_generic_test': 'Test', 'event_id': 123,
            'some_added_field': 'some value',
            'another_added_field': 'another_value',
            'dotted': {'added': {'field': 'yet_another_value'}}
        }
        document = {'add_first_existing_with_missing_generic_test': 'Test', 'event_id': 123}

        generic_adder.process(document)

        assert document == expected

    def test_add_generic_fields_from_file_missing_and_existing_with_all_required(self):
        with pytest.raises(InvalidRuleFileError, match=r'files do not exist'):
            config = {
                'type': 'generic_adder',
                'rules': [rules_dir_missing],
                'tree_config': 'tests/testdata/unit/shared_data/tree_config.json'
            }

            GenericAdderFactory.create('test-generic-adder', config, logger)

    def test_add_generic_fields_from_file_invalid(self):
        with pytest.raises(InvalidRuleFileError, match=r'must be a dictionary with string values'):
            config = {
                'type': 'generic_adder',
                'rules': [rules_dir_invalid],
                'tree_config': 'tests/testdata/unit/shared_data/tree_config.json'
            }

            GenericAdderFactory.create('test-generic-adder', config, logger)

    def test_add_generic_fields_to_co_existing_field(self, generic_adder):
        expected = {
            'add_generic_test': 'Test', 'event_id': 123,
            'some_added_field': 'some value',
            'another_added_field': 'another_value',
            'dotted': {'added': {'field': 'yet_another_value'},
                       'i_exist': 'already'}
        }
        document = {'add_generic_test': 'Test', 'event_id': 123, 'dotted': {'i_exist': 'already'}}

        generic_adder.process(document)

        assert document == expected

    def test_add_generic_fields_to_existing_value(self, generic_adder):
        expected = {
            'add_generic_test': 'Test', 'event_id': 123,
            'some_added_field': 'some_non_dict',
            'another_added_field': 'another_value',
            'dotted': {'added': {'field': 'yet_another_value'}}
        }
        document = {'add_generic_test': 'Test', 'event_id': 123, 'some_added_field': 'some_non_dict'}

        with pytest.raises(DuplicationError):
            generic_adder.process(document)

        assert document == expected

    def test_sql_database_enriches_via_table(self, generic_adder):
        expected = {
            'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123',
            'db': {
                'test': {'b': 'foo', 'c': 'bar'}
            }
        }
        document = {'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123'}

        generic_adder.process(document)

        assert document == expected

    def test_sql_database_adds_target_field(self, generic_adder_with_target):
        expected = {
            'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123',
            'db': {
                'test': {'a': 'TEST_0', 'b': 'foo', 'c': 'bar'}
            }
        }
        document = {'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123'}

        generic_adder_with_target.process(document)

        assert document == expected

    def test_sql_database_enriches_via_table_ignore_case(self, generic_adder):
        expected = {
            'add_from_sql_db_table': 'Test', 'source': 'test_0.test.123',
            'db': {
                'test': {'b': 'foo', 'c': 'bar'}
            }
        }
        document = {'add_from_sql_db_table': 'Test', 'source': 'test_0.test.123'}

        generic_adder.process(document)

        assert document == expected

    def test_sql_database_does_not_enrich_via_table_if_value_does_not_exist(self, generic_adder):
        expected = {'add_from_sql_db_table': 'Test', 'source': 'TEST_I_DO_NOT_EXIST.test.123'}
        document = {'add_from_sql_db_table': 'Test', 'source': 'TEST_I_DO_NOT_EXIST.test.123'}

        generic_adder.process(document)

        assert document == expected

    def test_sql_database_does_not_enrich_via_table_if_pattern_does_not_match(self, generic_adder):
        expected = {'add_from_sql_db_table': 'Test', 'source': 'TEST_0%FOO'}
        document = {'add_from_sql_db_table': 'Test', 'source': 'TEST_0%FOO'}

        generic_adder.process(document)

        assert document == expected

    def test_sql_database_reloads_table_on_change_after_wait(self, generic_adder):
        expected_1 = {
            'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123',
            'db': {
                'test': {'b': 'foo', 'c': 'bar'}
            }
        }
        expected_2 = {
            'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123',
            'db': {
                'test': {'b': 'fi', 'c': 'fo'}
            }
        }
        document_1 = {'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123'}
        document_2 = {'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123'}

        generic_adder.process(document_1)
        time.sleep(0.2)
        generic_adder._db_connector.cur.mock_simulate_table_change()
        generic_adder.process(document_2)

        assert document_1 == expected_1
        assert document_2 == expected_2

    def test_sql_database_with_empty_table_load_after_change(self, generic_adder):
        expected = {
            'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123',
            'db': {
                'test': {'b': 'fi', 'c': 'fo'}
            }
        }
        document = {'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123'}

        generic_adder._db_table = {}
        generic_adder._db_connector.cur.mock_simulate_table_change()
        time.sleep(0.2)
        generic_adder.process(document)

        assert document == expected

    def test_sql_database_does_not_reload_table_on_change_if_no_wait(self, generic_adder):
        expected = {
            'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123',
            'db': {
                'test': {'b': 'foo', 'c': 'bar'}
            }
        }
        document_1 = {'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123'}
        document_2 = {'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123'}

        generic_adder.process(document_1)
        generic_adder._db_connector.cur.mock_simulate_table_change()
        generic_adder.process(document_2)

        assert document_1 == expected
        assert document_2 == expected

    def test_sql_database_raises_exception_on_duplicate(self, generic_adder):
        expected = {
            'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123',
            'db': {
                'test': {'b': 'foo', 'c': 'bar'}
            }
        }
        document = {'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123'}

        generic_adder.process(document)
        with pytest.raises(DuplicationError):
            generic_adder.process(document)

        assert document == expected

    def test_sql_database_no_enrichment_with_empty_table(self, generic_adder):
        expected = {'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123'}
        document = {'add_from_sql_db_table': 'Test', 'source': 'TEST_0.test.123'}

        generic_adder._db_connector.cur.mock_clear_all()
        generic_adder._db_table = {}
        generic_adder.process(document)

        assert document == expected
