# pylint: disable=missing-module-docstring
from random import sample
from string import ascii_letters
from logging import getLogger

from pytest import raises

from logprep.processor.clusterer.processor import Clusterer
from logprep.processor.donothing.processor import DoNothing
from logprep.processor.labeler.processor import Labeler
from logprep.processor.normalizer.processor import Normalizer
from logprep.processor.processor_factory import ProcessorFactory
from logprep.processor.processor_factory_error import (
    InvalidConfigurationError,
    UnknownProcessorTypeError,
    NotExactlyOneEntryInConfigurationError,
    NoTypeSpecifiedError,
    InvalidConfigSpecificationError,
)
from logprep.processor.pseudonymizer.processor import Pseudonymizer
from tests.testdata.metadata import path_to_schema, path_to_single_rule

logger = getLogger()


class TestProcessorFactory:
    def test_create_fails_for_an_empty_section(self):
        with raises(
            NotExactlyOneEntryInConfigurationError,
            match="There must be exactly one processor definition per pipeline entry.",
        ):
            ProcessorFactory.create({}, logger)

    def test_create_fails_if_config_is_not_an_object(self):
        with raises(
            InvalidConfigSpecificationError,
            match="The processor configuration must be specified as an object.",
        ):
            ProcessorFactory.create({"processorname": "string"}, logger)

    def test_create_fails_if_config_does_not_contain_type(self):
        with raises(NoTypeSpecifiedError, match="The processor type specification is missing"):
            ProcessorFactory.create({"processorname": {"other": "value"}}, logger)

    def test_create_fails_for_unknown_type(self):
        for type_name in ["unknown", "no such processor"] + [
            "".join(sample(ascii_letters, 6)) for i in range(5)
        ]:
            with raises(UnknownProcessorTypeError):
                ProcessorFactory.create({"processorname": {"type": type_name}}, logger)

    def test_create_donothing_returns_donothing_processor(self):
        processor = ProcessorFactory.create({"nothing": {"type": "donothing"}}, logger)

        assert isinstance(processor, DoNothing)

    def test_create_pseudonymizer_returns_pseudonymizer_processor(self):
        processor = ProcessorFactory.create(
            {
                "pseudonymizer": {
                    "type": "pseudonymizer",
                    "pubkey_analyst": "tests/testdata/unit/pseudonymizer/example_analyst_pub.pem",
                    "pubkey_depseudo": "tests/testdata/unit/pseudonymizer/example_depseudo_pub.pem",
                    "hash_salt": "a_secret_tasty_ingredient",
                    "specific_rules": ["some specific rules"],
                    "generic_rules": ["some generic rules"],
                    "regex_mapping": "tests/testdata/unit/pseudonymizer/rules/regex_mapping.yml",
                    "pseudonyms_topic": "pseudonyms",
                    "max_cached_pseudonyms": 1000000,
                    "max_caching_days": 1,
                    "tld_list": "-",
                }
            },
            logger,
        )

        assert isinstance(processor, Pseudonymizer)

    def test_create_normalizer_returns_normalizer_processor(self):
        processor = ProcessorFactory.create(
            {
                "normalizer": {
                    "type": "normalizer",
                    "specific_rules": ["tests/testdata/unit/normalizer/rules/specific"],
                    "generic_rules": ["tests/testdata/unit/normalizer/rules/generic"],
                    "regex_mapping": "tests/testdata/unit/normalizer/regex_mapping.yml",
                }
            },
            logger,
        )

        assert isinstance(processor, Normalizer)

    def test_create_clusterer_returns_clusterer_processor(self):
        processor = ProcessorFactory.create(
            {
                "clusterer": {
                    "type": "clusterer",
                    "output_field_name": "cluster_signature",
                    "specific_rules": "test_rules",
                    "generic_rules": "test_rules",
                }
            },
            logger,
        )

        assert isinstance(processor, Clusterer)

    def test_fails_when_section_contains_more_than_one_element(self):
        with raises(
            InvalidConfigurationError,
            match="There must be exactly one processor definition per pipeline entry.",
        ):
            ProcessorFactory.create(
                {"first": {"type": "donothing"}, "second": {"type": "donothing"}}, logger
            )

    def test_create_labeler_creates_labeler_processor(self):
        processor = ProcessorFactory.create(
            {
                "labelername": {
                    "type": "labeler",
                    "schema": path_to_schema,
                    "rules": [path_to_single_rule],
                }
            },
            logger,
        )

        assert isinstance(processor, Labeler)
