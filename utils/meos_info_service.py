# -*- coding: utf-8 -*-

import logging
from typing import List, Any, Dict
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from xml.etree import ElementTree
from xml.etree.ElementTree import Element

from imurl import URL

from utils.config import Config
from utils.config_consumer import ConfigConsumer
from utils.config_definitions import ConfigOptionDefinition, ConfigSectionDefinition, ConfigSectionEnableType, \
    ConfigSectionOptionDefinition, VerificationResult
from utils.config_verification import ConfigVerifierDefinition
from utils.singleton import Singleton

LOGGER_NAME = 'MeosInfoService'

DEFAULT_RESPONSE_ENCODING = 'utf-8'
NS = {'ns': 'http://www.melin.nu/mop'}


def _get_data(element: Element, selector: str, ns):
    data = element.find(selector, ns)
    if data is not None:
        return data.text
    else:
        return None


def _verify_info_service_url(url: str) -> VerificationResult:
    try:
        if url is None:
            raise ValueError('Info service URL must be configured.')

        url = URL(url)
        url = url.set_query('get', 'competition')
        req = Request(url.url)
        response = urlopen(req)

        response_encoding = response.info().get_content_charset()
        if response_encoding is None:
            response_encoding = DEFAULT_RESPONSE_ENCODING

        data = response.read().decode(response_encoding)
        root = ElementTree.fromstring(data)

        compName = _get_data(root, 'ns:competition', NS)

        return VerificationResult(message=f'URL is valid. Found competition "{compName}".')
    except Exception as e:
        logging.getLogger(LOGGER_NAME).debug('_verify_info_service_url: %s', e)
        return VerificationResult(message=str(e), status=False)


class _MeosInfoServiceMeta(type(ConfigConsumer), type(Singleton)):
    pass


class MeosInfoService(ConfigConsumer, Singleton, metaclass=_MeosInfoServiceMeta):
    """
    Util for interacting with the MeOS Info service.
    """

    CONFIG_SECTION_MEOS = __qualname__

    CONFIG_OPTION_MEOS_URL = ConfigOptionDefinition(
        name='MeosUrl',
        display_name='Info service URL',
        value_type=str,
        description='The base URL to where MeOS info service is up and running.',
        mandatory=True,
        default_value='http://localhost:2009/meos',
    )

    VERIFIER_MEOS_URL = ConfigVerifierDefinition(
        function=_verify_info_service_url,
        parameters=[
            ConfigSectionOptionDefinition(
                section_name=CONFIG_SECTION_MEOS,
                option_definition=CONFIG_OPTION_MEOS_URL,
            ),
        ],
        message='The Info service URL was not valid.',
    )

    CONFIG_OPTION_MEOS_URL.set_verifier(VERIFIER_MEOS_URL)

    MEOS_CONFIG_SECTION_DEFINITION = ConfigSectionDefinition(
        name=CONFIG_SECTION_MEOS,
        display_name='MeOS Info service',
        option_definitions=[CONFIG_OPTION_MEOS_URL],
        enable_type=ConfigSectionEnableType.IF_REQUIRED,
        sort_key_prefix=20,
    )

    Config.register_config_section_definition(MEOS_CONFIG_SECTION_DEFINITION)

    @classmethod
    def config_section_definition(cls) -> ConfigSectionDefinition:
        return cls.MEOS_CONFIG_SECTION_DEFINITION

    def __repr__(self) -> str:
        return f'MeosInfoService(url={self.meos_url})'

    def __str__(self) -> str:
        return repr(self)

    def __init__(self):
        super().__init__()

        if LOGGER_NAME != self.__class__.__name__:
            raise ValueError('LOGGER_NAME not correct: {} vs {}'.format(LOGGER_NAME, self.__class__.__name__))

        self.logger = logging.getLogger(self.__class__.__name__)

        self.meos_url = None
        self.is_relay = None

        self._parse_config()

        self.logger.debug(self)

    def config_updated(self, section_names: List[str]):
        self._parse_config()

    def _parse_config(self):
        config_section = Config().get_section(self.CONFIG_SECTION_MEOS)

        self.meos_url = self.CONFIG_OPTION_MEOS_URL.get_value(config_section)
        self.is_relay = None

    @staticmethod
    def _has_no_result(runner: Element) -> bool:
        statusElem = runner.find('ns:Status', NS)
        if statusElem is None:
            return False

        status = statusElem.attrib['code']
        if status == '0':
            return True
        else:
            return False

    def get_event_race_pre_warning_data(self, card_number: str) -> Dict[str, Any] or None:
        self.logger.debug('get_event_pre_warning_data')

        if self.meos_url is None:
            raise ValueError('MeOS Info server URL must be configured.')

        url = URL(self.meos_url)

        url = url.set_query('lookup', 'competitor')
        url = url.set_query('card', card_number)

        req = Request(url.url)
        try:
            response = urlopen(req)
            response_encoding = response.info().get_content_charset()
            if response_encoding is None:
                response_encoding = DEFAULT_RESPONSE_ENCODING

            data = response.read().decode(response_encoding)

        except HTTPError as e:
            self.logger.error('_fetch_punches: The server could not fulfill the request. Error code: %s',
                              e.code)
            raise
        except URLError as e:
            self.logger.error('_fetch_punches: We failed to reach a server. Reason: %s', e.reason)
            raise
        except Exception as e:
            self.logger.error('_fetch_punches: Unknown Exception. %s', e)
            raise

        root = ElementTree.fromstring(data)

        runners = root.findall('ns:Competitor', NS)
        self.logger.debug('Got these runners back: %s', runners)

        if len(runners) == 0:
            self.logger.info('No runner was found with card: %s', card_number)
            return None

        if len(runners) > 1:
            self.logger.info('More then one runner found with card: %s', card_number)

            # Try to find a runner that has a status of 0, i.e. doesn't have a result yet
            runner = next(filter(self._has_no_result, runners), None)
        else:
            runner = runners[0]

        teamElem = runner.find('ns:Team', NS)
        if teamElem is None:
            return None

        # Fetching the teams "StartNum" since "BibNum" isn't available.
        # Make sure to always set StartNum and BibNum to same value in MeOS!
        bib_number = teamElem.attrib['id']
        relay_leg = _get_data(runner, 'ns:Leg', NS)

        return {
            'bibNumber': bib_number,
            'relayLeg': relay_leg
        }
