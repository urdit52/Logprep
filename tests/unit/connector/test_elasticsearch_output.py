from copy import deepcopy
from datetime import datetime
from json import loads, dumps
from math import isclose

from pytest import fail, raises

from logprep.input.confluent_kafka_input import ConfluentKafkaInput
from logprep.output.es_output import ElasticsearchOutput
from logprep.output.output import CriticalOutputError
import elasticsearch.helpers


class NotJsonSerializableMock:
    pass


def mock_bulk(_, documents: list):
    for document in documents:
        try:
            loads(dumps(document))
        except TypeError:
            raise CriticalOutputError('Error storing output document: Serialization Error',
                                      document)


elasticsearch.helpers.bulk = mock_bulk


class TestElasticsearchOutput:
    default_configuration = {
        'bootstrap.servers': 'bootstrap1,bootstrap2',
        'group.id': 'consumer_group',
        'enable.auto.commit': False,
        'enable.auto.offset.store': True,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'acks': 'all',
        'compression.type': 'none',
        'queue.buffering.max.messages': 31337,
        'linger.ms': 0
    }

    def setup_method(self, _):
        self.config = deepcopy(self.default_configuration)
        self.kafka_input = ConfluentKafkaInput(['bootstrap1', 'bootstrap2'],
                                               'consumer_topic',
                                               'consumer_group',
                                               True)
        self.es_output = ElasticsearchOutput('host', 123, 'default_index', 'error_index', 1, 5000)

    def remove_options(self, *args):
        for key in args:
            del self.config[key]

    def test_implements_abstract_methods(self):
        try:
            ElasticsearchOutput('host', 123, 'default_index', 'error_index', 2, 5000)
        except TypeError as err:
            fail('Must implement abstract methods: %s' % str(err))

    def test_describe_endpoint_returns_kafka_with_first_boostrap_config(self):
        assert self.es_output.describe_endpoint() == 'Elasticsearch Output: host123'

    def test_store_sends_event_to_expected_topic(self):
        default_index = 'producer_topic'
        event = {'field': 'content'}
        expected = {'field': 'content',
                    '_index': default_index}

        es_output = ElasticsearchOutput('host', 123, default_index, 'error_index', 1, 5000)
        es_output.store(event)

        assert es_output._message_backlog[0] == expected

    def test_store_custom_sends_event_to_expected_topic(self):
        custom_index = 'custom_topic'
        event = {'field': 'content'}

        expected = {'field': 'content',
                    '_index': custom_index}

        es_output = ElasticsearchOutput('host', 123, 'default_index', 'error_index', 1, 5000)
        es_output.store_custom(event, custom_index)

        assert es_output._message_backlog[0] == expected

    def test_store_failed(self):
        error_index = 'error_index'
        event_received = {'field': 'received'}
        event = {'field': 'content'}
        error_message = 'error message'

        expected = {'error': error_message,
                    'original': event_received,
                    'processed': event,
                    '_index': error_index,
                    'timestamp': str(datetime.now())}

        es_output = ElasticsearchOutput('host', 123, 'default_index', error_index, 1, 5000)
        es_output.store_failed(error_message, event_received, event)

        error_document = es_output._message_backlog[0]
        # timestamp is compared to be approximately the same,
        # since it is variable and then removed to compare the rest
        date_format = '%Y-%m-%d %H:%M:%S.%f'
        error_time = datetime.timestamp(datetime.strptime(error_document['timestamp'], date_format))
        expected_time = datetime.timestamp(datetime.strptime(error_document['timestamp'],
                                                             date_format))
        assert isclose(error_time, expected_time)
        del error_document['timestamp']
        del expected['timestamp']

        # assert error_topic == expected
        assert error_document == expected

    def test_create_confluent_settings_contains_expected_values2(self):
        with raises(CriticalOutputError,
                    match=r'Error storing output document\: Serialization Error'):
            self.es_output.store({'invalid_json': NotJsonSerializableMock(), 'something_valid': 'im_valid!'})
