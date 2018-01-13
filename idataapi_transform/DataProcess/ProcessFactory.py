# config
from .Config.MainConfig import main_config

from .Config.ConfigUtil import GetterConfig
from .Config.ConfigUtil import WriterConfig

from .DataGetter.ESGetter import ESScrollGetter
from .DataGetter.CSVGetter import CSVGetter
from .DataGetter.APIGetter import APIGetter, APIBulkGetter
from .DataGetter.JsonGetter import JsonGetter
from .DataGetter.XLSXGetter import XLSXGetter

from .DataWriter.CSVWriter import CSVWriter
from .DataWriter.ESWriter import ESWriter
from .DataWriter.JsonWriter import JsonWriter
from .DataWriter.TXTWriter import TXTWriter
from .DataWriter.XLSXWriter import XLSXWriter


class ProcessFactory(object):
    @staticmethod
    def create_getter(config):
        """
        create a getter based on config
        :return: getter
        """
        if isinstance(config, GetterConfig.RAPIConfig):
            return APIGetter(config)
        elif isinstance(config, GetterConfig.RCSVConfig):
            return CSVGetter(config)
        elif isinstance(config, GetterConfig.RESConfig):
            return ESScrollGetter(config)
        elif isinstance(config, GetterConfig.RJsonConfig):
            return JsonGetter(config)
        elif isinstance(config, GetterConfig.RXLSXConfig):
            return XLSXGetter(config)
        elif isinstance(config, GetterConfig.RAPIBulkConfig):
            return APIBulkGetter(config)
        else:
            raise ValueError("create_getter must pass one of the instance of [RAPIConfig, RCSVConfig, "
                             "RESConfig, RJsonConfig, RXLSXConfig, RAPIBulkConfig]")

    @staticmethod
    def create_writer(config):
        """
        create a writer based on config
        :return: a writer
        """
        if isinstance(config, WriterConfig.WCSVConfig):
            return CSVWriter(config)
        elif isinstance(config, WriterConfig.WESConfig):
            return ESWriter(config)
        elif isinstance(config, WriterConfig.WJsonConfig):
            return JsonWriter(config)
        elif isinstance(config, WriterConfig.WTXTConfig):
            return TXTWriter(config)
        elif isinstance(config, WriterConfig.WXLSXConfig):
            return XLSXWriter(config)
        else:
            raise ValueError("create_writer must pass one of the instance of [WCSVConfig, WESConfig, WJsonConfig, "
                             "WTXTConfig, WXLSXConfig]")
