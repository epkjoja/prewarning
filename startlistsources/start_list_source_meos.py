# -*- coding: utf-8 -*-

import logging
from typing import List, Dict

from pymysql import OperationalError

from utils.config import ConfigSectionDefinition, Config
from utils.config_definitions import ConfigSectionEnableType
from utils.meos_info_service import MeosInfoService
from ._base import _StartListSourceBase


class StartListSourceMeos(_StartListSourceBase):
    """
    A Start List Source that fetches the runner data from MeOS information service.
    """

    name = __qualname__

    display_name = 'MeOS Start List Source'

    description = 'Looks up the team start number and relay leg through MeOS info service.' \
                  'NOTE: Make sure to set the "startNumber" to the same value as the "bibNumber" '\
                  'for each team in MeOS!'

    START_LIST_SOURCE_MEOS_CONFIG_SECTION_DEFINITION = ConfigSectionDefinition(
        name=name,
        display_name=display_name,
        option_definitions=[
        ],
        enable_type=ConfigSectionEnableType.IF_ENABLED,
        requires=[
            MeosInfoService.config_section_definition(),
        ],
        sort_key_prefix=40,
    )

    Config.register_config_section_definition(START_LIST_SOURCE_MEOS_CONFIG_SECTION_DEFINITION)

    @classmethod
    def config_section_definition(cls) -> ConfigSectionDefinition:
        return cls.START_LIST_SOURCE_MEOS_CONFIG_SECTION_DEFINITION

    def __repr__(self) -> str:
        return f'StartListSourceMeos(running={self._running})'

    def __str__(self) -> str:
        return repr(self)

    def __init__(self):
        super().__init__()

        self.logger = logging.getLogger(self.__class__.__name__)

        self.meos_info_service = MeosInfoService()

        self._running = False

        self.logger.debug(self)

    def __del__(self):
        self.stop()

    def start(self):
        self._running = True
        self.update()

    def stop(self):
        self._running = False

    def is_running(self) -> bool:
        return self._running

    def on_modified(self, event):
        pass

    def config_updated(self, section_names: List[str]):
        self.update()

    def update(self):
        self._parse_config()

    def _parse_config(self):
        pass

    def lookup_from_card_number(self, card_number: str) -> Dict[str, str] or None:
        """Returns Bib-Number and Relay Leg for the provided Card Number.

        :param str card_number: The Card Number to look up.
        :return: A dict with the Bib-Number (bibNumber) and Relay Leg (relayLeg).
        :rtype: Dict[str, Str] or None
        """
        if not self._running:
            self.logger.debug('NOT started, ignoring request!')
            return None
        try:
            pre_warning_data = self.meos_info_service.get_event_race_pre_warning_data(card_number)
            self.logger.debug(pre_warning_data)
            return pre_warning_data
        except OperationalError as oe:
            self.logger.error(oe)

        return None
