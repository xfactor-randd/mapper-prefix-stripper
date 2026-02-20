"""PrefixStripper mapper class."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

import singer_sdk.typing as th
from singer_sdk import singerlib as singer
from singer_sdk.mapper import PluginMapper
from singer_sdk.mapper_base import InlineMapper

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import PurePath


class PrefixStripperMapper(InlineMapper):
    """Mapper for PrefixStripper."""

    name = "mapper-prefix-stripper"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "strip_prefixes",
            th.ArrayType(th.StringType()),
            title="The prefixes to replace",
            description="The field prefixes that needs removal",
        ),
    ).to_dict()

    def __init__(
        self,
        *,
        config: dict | PurePath | str | list[PurePath | str] | None = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
    ) -> None:
        """Create a new inline mapper.

        Args:
            config: Mapper configuration. Can be a dictionary, a single path to a
                configuration file, or a list of paths to multiple configuration
                files.
            parse_env_config: Whether to look for configuration values in environment
                variables.
            validate_config: True to require validation of config settings.
        """
        super().__init__(
            config=config,
            parse_env_config=parse_env_config,
            validate_config=validate_config,
        )

        self.mapper = PluginMapper(plugin_config=dict(self.config), logger=self.logger)

    @override
    def map_schema_message(self, message_dict: dict) -> Iterable[singer.Message]:
        """Map a schema message to zero or more new messages.

        Args:
            message_dict: A SCHEMA message JSON dictionary.
        """
        new_properties = {}
        for field, schema in message_dict["schema"]["properties"].items():
            new_field = self.strip_prefix(field)
            new_properties[new_field] = schema
        message_dict["schema"]["properties"] = new_properties
        yield singer.SchemaMessage.from_dict(message_dict)

    def strip_prefix(self, field: str) -> str:
        """Strip any configured prefix from the specified field.

        Args:
            field : A string to parse
        """
        prefixes = self.config.get("strip_prefixes", [])
        for prefix in prefixes:
            if field.startswith(prefix):
                return field[len(prefix) :]
        return field

    @override
    def map_record_message(
        self,
        message_dict: dict,
    ) -> Iterable[singer.RecordMessage]:
        """Map a record message to zero or more new messages.

        Args:
            message_dict: A RECORD message JSON dictionary.
        """
        new_record = {}
        for field, value in message_dict["record"].items():
            new_field = self.strip_prefix(field)
            new_record[new_field] = value
        message_dict["record"] = new_record
        yield singer.RecordMessage.from_dict(message_dict)

    @override
    def map_state_message(self, message_dict: dict) -> Iterable[singer.Message]:
        """Map a state message to zero or more new messages.

        Args:
            message_dict: A STATE message JSON dictionary.
        """
        yield singer.StateMessage.from_dict(message_dict)

    @override
    def map_activate_version_message(
        self,
        message_dict: dict,
    ) -> Iterable[singer.Message]:
        """Map a version message to zero or more new messages.

        Args:
            message_dict: An ACTIVATE_VERSION message JSON dictionary.
        """
        yield singer.ActivateVersionMessage.from_dict(message_dict)


if __name__ == "__main__":
    PrefixStripperMapper.cli()
