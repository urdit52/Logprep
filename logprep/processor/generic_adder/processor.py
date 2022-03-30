"""This module contains functionality for adding fields using lists or a SQL table."""

from time import time
from typing import List, Optional, Union
from logging import Logger, DEBUG

from os import walk
from os.path import isdir, realpath, join

from multiprocessing import current_process

from logprep.processor.generic_adder.rule import GenericAdderRule
from logprep.processor.generic_adder.mysql_connector import MySQLConnector
from logprep.processor.base.processor import RuleBasedProcessor
from logprep.processor.base.exceptions import (
    NotARulesDirectoryError,
    InvalidRuleDefinitionError,
    InvalidRuleFileError,
)

from logprep.util.processor_stats import ProcessorStats
from logprep.util.time_measurement import TimeMeasurement


class GenericAdderError(BaseException):
    """Base class for GenericAdder related exceptions."""

    def __init__(self, name: str, message: str):
        super().__init__(f"GenericAdder ({name}): {message}")


class DuplicationError(GenericAdderError):
    """Raise if field already exists."""

    def __init__(self, name: str, skipped_fields: List[str]):
        message = (
            "The following fields already existed and " "were not overwritten by the GenericAdder: "
        )
        message += " ".join(skipped_fields)

        super().__init__(name, message)


class GenericAdder(RuleBasedProcessor):
    """Add arbitrary fields and values to a processed events.

    Fields and values can be added directly from the rule definition or from a file specified within
    a rule. Furthermore, a SQL table can be used to to add multiple keys and values if a specified
    field's value within the SQL table matches a specified field's value withing the rule
    definition.

    The generic adder can not overwrite existing values.

    """

    db_table = None

    def __init__(self, name: str, tree_config: str, sql_config: dict, logger: Logger):
        super().__init__(name, tree_config, logger)
        """Initialize a generic adder instance.
        
        Performs a basic processor initialization. Furthermore, a SQL database and a SQL table are 
        being initialized if a SQL configuration exists.

        Parameters
        ----------
        name : str
           Name for the generic adder.
        tree_config : str
           Path to configuration for rule trees.
        sql_config : dict
           SQL configuration dictionary.
        logger : logging.Logger
           Logger to use.

        """

        self._db_connector = MySQLConnector(sql_config, logger) if sql_config else None

        if GenericAdder.db_table is None:
            GenericAdder.db_table = self._db_connector.get_data() if self._db_connector else None
        self._db_table = GenericAdder.db_table

        self.ps = ProcessorStats()

    # pylint: disable=arguments-differ
    def add_rules_from_directory(self, rule_paths: List[str]):
        """Load generic adder rules from files within directories.

        Parameters
        ----------
        rule_paths : list of str
           Paths to directories with rule files.

        Raises
        ------
        NotARulesDirectoryError
            Raises if a path is not a directory.

        """
        for path in rule_paths:
            if not isdir(realpath(path)):
                raise NotARulesDirectoryError(self._name, path)

            for root, _, files in walk(path):
                json_files = []
                for file in files:
                    if (file.endswith(".json") or file.endswith(".yml")) and not file.endswith(
                        "_test.json"
                    ):
                        json_files.append(file)
                for file in json_files:
                    rules = self._load_rules_from_file(join(root, file))
                    for rule in rules:
                        self._tree.add_rule(rule, self._logger)

        if self._logger.isEnabledFor(DEBUG):
            self._logger.debug(
                f"{self.describe()} loaded {self._tree.rule_counter} rules "
                f"({current_process().name})"
            )

        self.ps.setup_rules([None] * self._tree.rule_counter)

    # pylint: enable=arguments-differ

    def _load_rules_from_file(self, path: str):
        """Load generic adder rules from a file.

        Parameters
        ----------
        path : str
           Path to a rules file.

        Returns
        -------
        list of GenericAdderRule
            List of generic adder rules.

        Raises
        ------
        InvalidRuleFileError
            Raises if the rule definition is invalid.

        """
        try:
            return GenericAdderRule.create_rules_from_file(path)
        except InvalidRuleDefinitionError as error:
            raise InvalidRuleFileError(self._name, path, str(error)) from error

    def describe(self) -> str:
        """Get the name and type of the generic adder processor.

        Returns
        -------
        str
            The description of the generic adder.

        """
        return f"GenericAdder ({self._name})"

    @TimeMeasurement.measure_time("generic_adder")
    def process(self, event: dict):
        """Process an event by the generic adder.

        Add fields and values to the event according to the rules it matches for.
        Additions can come from the rule definition, from a file or from a SQL table.

        The SQL table is initially loaded from the database and then reloaded if it changes.

        Parameters
        ----------
        event : dict
           Event that will be processed.

        """
        self.ps.increment_processed_count()

        if self._db_connector and self._db_connector.check_change():
            self._db_table = self._db_connector.get_data()

        self._event = event

        for rule in self._tree.get_matching_rules(event):
            begin = time()
            self._apply_rules(event, rule)
            processing_time = float("{:.10f}".format(time() - begin))
            idx = self._tree.get_rule_id(rule)
            self.ps.update_per_rule(idx, processing_time)

    def _apply_rules(self, event: dict, rule: GenericAdderRule):
        """Apply a matching generic adder rule to the event.

        At first it checks if a SQL table exists and if it will be used. If it does, it adds all
        values from a matching row in the table to the event. To determine if a row matches, a
        pattern is used on a defined value of the event to extract a subvalue that is then matched
        against a value in a defined column of the SQL table. A dotted path prefix can be applied to
        add the new fields into a shared nested location.

        If no table exists, fields defined withing the rule itself or in a rule file are being added
        to the event.

        Parameters
        ----------
        event : dict
           Name of the event to add keys and values to.
        rule : GenericAdderRule
           A matching generic adder rule.

        Raises
        ------
        DuplicationError
            Raises if an addition would overwrite an existing field or value.

        """
        conflicting_fields = list()

        # Either add items from a sql db table or from the rule definition and/or a file
        if rule.db_target and self._db_table:
            # Create items to add from a db table
            items_to_add = []
            if rule.db_pattern:
                # Get the sub part of the value from the event using a regex pattern
                value_to_check_in_db = self._get_dotted_field_value(event, rule.db_target)
                match_with_value_in_db = rule.db_pattern.match(value_to_check_in_db)
                if match_with_value_in_db:
                    # Get values to add from db table using the sub part
                    value_to_map = match_with_value_in_db.group(1).upper()
                    to_add_from_db = self._db_table.get(value_to_map, [])
                    for item in to_add_from_db:
                        if rule.db_destination_prefix:
                            if not item[0].startswith(rule.db_destination_prefix):
                                item[0] = f"{rule.db_destination_prefix}.{item[0]}"
                        items_to_add.append(item)
        else:
            # Use items from rule definition and/or file
            items_to_add = rule.add.items()

        # Add the items to the event
        for dotted_field, value in items_to_add:
            keys = dotted_field.split(".")
            dict_ = event
            for idx, key in enumerate(keys):
                if key not in dict_:
                    if idx == len(keys) - 1:
                        dict_[key] = value
                        break
                    dict_[key] = dict()

                if isinstance(dict_[key], dict):
                    dict_ = dict_[key]
                else:
                    conflicting_fields.append(keys[idx])

        if conflicting_fields:
            raise DuplicationError(self._name, conflicting_fields)

    @staticmethod
    def _get_dotted_field_value(event: dict, dotted_field: str) -> Optional[Union[dict, list, str]]:
        """Get a field's value from a nested dict by using a dot-notation to denote subfields.

        Example
        -------
        The following example shows how this method can access a nested value via the dot-notation.

        >>> nested_dict = {'some': {'nested': {'field': 'nested_value'}}}
        >>> self._get_dotted_field_value(nested_dict, 'some.nested.field')
        nested_value

        Parameters
        ----------
        event : dict
           Name of the event to retrieve a value from.
        dotted_field : str
           The dotted path for a nested field.

        Returns
        -------
        str, list, dict, None
            The value that was found in the dict if the dotted path exists,
            None otherwise.

            The result type reflects the possible types of values that can be expected in the dict.

        """
        fields = dotted_field.split(".")
        dict_ = event
        for field in fields:
            if field in dict_:
                dict_ = dict_[field]
            else:
                return None
        return dict_
