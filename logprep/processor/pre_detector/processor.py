"""This module contains functionality for pre-detecting attacks."""

from typing import List
from logging import Logger, DEBUG
from os import walk
from os.path import isdir, realpath, join
from uuid import uuid4
from time import time
from multiprocessing import current_process

from logprep.framework.rule_tree.rule_tree import RuleTree
from logprep.processor.base.processor import RuleBasedProcessor
from logprep.processor.pre_detector.rule import PreDetectorRule
from logprep.processor.base.exceptions import (
    NotARulesDirectoryError,
    InvalidRuleDefinitionError,
    InvalidRuleFileError,
)
from logprep.processor.pre_detector.ip_alerter import IPAlerter

from logprep.util.processor_stats import ProcessorStats
from logprep.util.time_measurement import TimeMeasurement


class PreDetectorError(BaseException):
    """Base class for PreDetector related exceptions."""

    def __init__(self, name: str, message: str):
        super().__init__(f"PreDetector ({name}): {message}")


class PreDetectorConfigurationError(PreDetectorError):
    """Generic PreDetector configuration error."""


class PreDetector(RuleBasedProcessor):
    """Processor used to pre_detect log events."""

    def __init__(self, name: str, configuration: dict, logger: Logger):
        tree_config = configuration.get("tree_config")
        pre_detector_topic = configuration.get("pre_detector_topic")
        alert_ip_list_path = configuration.get("alert_ip_list_path")
        specific_rules_dirs = configuration.get("specific_rules")
        generic_rules_dirs = configuration.get("generic_rules")
        super().__init__(name, tree_config, logger)
        self._logger = logger
        self.ps = ProcessorStats()

        self._name = name
        self._pre_detector_topic = pre_detector_topic
        self._event = None

        self._ids = []
        self._generic_tree = RuleTree(config_path=tree_config)
        self._specific_tree = RuleTree(config_path=tree_config)

        self._ip_alerter = IPAlerter(alert_ip_list_path)
        self.add_rules_from_directory(
            specific_rules_dirs=specific_rules_dirs, generic_rules_dirs=generic_rules_dirs
        )

    # pylint: disable=arguments-differ
    def add_rules_from_directory(
        self, specific_rules_dirs: List[str], generic_rules_dirs: List[str]
    ):
        for specific_rules_dir in specific_rules_dirs:
            rule_paths = self._list_json_files_in_directory(specific_rules_dir)
            for rule_path in rule_paths:
                rules = PreDetectorRule.create_rules_from_file(rule_path)
                for rule in rules:
                    self._specific_tree.add_rule(rule, self._logger)
        for generic_rules_dir in generic_rules_dirs:
            rule_paths = self._list_json_files_in_directory(generic_rules_dir)
            for rule_path in rule_paths:
                rules = PreDetectorRule.create_rules_from_file(rule_path)
                for rule in rules:
                    self._generic_tree.add_rule(rule, self._logger)
        if self._logger.isEnabledFor(DEBUG):
            self._logger.debug(
                f"{self.describe()} loaded {self._specific_tree.rule_counter} "
                f"specific rules ({current_process().name})"
            )
            self._logger.debug(
                f"{self.describe()} loaded {self._generic_tree.rule_counter} generic rules "
                f"({current_process().name})"
            )
        self.ps.setup_rules(
            [None] * self._generic_tree.rule_counter + [None] * self._specific_tree.rule_counter
        )

    # pylint: enable=arguments-differ

    def _load_rules_from_file(self, path: str):
        try:
            return PreDetectorRule.create_rules_from_file(path)
        except InvalidRuleDefinitionError as error:
            raise InvalidRuleFileError(self._name, path) from error

    def describe(self) -> str:
        return f"PreDetector ({self._name})"

    def setup(self):
        pass

    @TimeMeasurement.measure_time("pre_detector")
    def process(self, event: dict) -> tuple:
        self._event = event
        detection_results = []

        for rule in self._generic_tree.get_matching_rules(event):
            begin = time()
            if not (
                self._ip_alerter.has_ip_fields(rule)
                and not self._ip_alerter.is_in_alerts_list(rule, event)
            ):
                if self._logger.isEnabledFor(DEBUG):
                    self._logger.debug("{} processing matching event".format(self.describe()))
                self._get_detection_result(rule, detection_results)
                processing_time = float("{:.10f}".format(time() - begin))
                idx = self._generic_tree.get_rule_id(rule)
                self.ps.update_per_rule(idx, processing_time)

        for rule in self._specific_tree.get_matching_rules(event):
            begin = time()
            if not (
                self._ip_alerter.has_ip_fields(rule)
                and not self._ip_alerter.is_in_alerts_list(rule, event)
            ):
                if self._logger.isEnabledFor(DEBUG):
                    self._logger.debug("{} processing matching event".format(self.describe()))
                self._get_detection_result(rule, detection_results)
                processing_time = float("{:.10f}".format(time() - begin))
                idx = self._specific_tree.get_rule_id(rule)
                self.ps.update_per_rule(idx, processing_time)

        if "@timestamp" in event:
            for detection in detection_results:
                detection["@timestamp"] = event["@timestamp"]

        self.ps.increment_processed_count()
        return (detection_results, self._pre_detector_topic) if detection_results else None

    def _get_detection_result(self, rule: PreDetectorRule, detection_results: list):
        if self._event.get("pre_detection_id") is None:
            self._event["pre_detection_id"] = str(uuid4())

        detection_results.append(self._generate_detection_result(rule))

    def _generate_detection_result(self, rule: PreDetectorRule):
        detection_result = rule.detection_data
        detection_result["rule_filter"] = rule.filter_str
        detection_result["description"] = rule.description
        detection_result["pre_detection_id"] = self._event["pre_detection_id"]
        if "host" in self._event and "name" in self._event["host"]:
            detection_result["host"] = {"name": self._event["host"]["name"]}

        return detection_result
