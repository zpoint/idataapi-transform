# config
from .Config.MainConfig import main_config

from .Config.ConfigUtil import GetterConfig
from .Config.ConfigUtil import WriterConfig

from .DataGetter.ESGetter import ESScrollGetter
from .DataGetter.CSVGetter import CSVGetter
from .DataGetter.APIGetter import APIGetter, APIBulkGetter
from .DataGetter.JsonGetter import JsonGetter
from .DataGetter.XLSXGetter import XLSXGetter
from .DataGetter.RedisGetter import RedisGetter
from .DataGetter.MySQLGetter import MySQLGetter
from .DataGetter.MongoGetter import MongoGetter

from .DataWriter.CSVWriter import CSVWriter
from .DataWriter.ESWriter import ESWriter
from .DataWriter.JsonWriter import JsonWriter
from .DataWriter.TXTWriter import TXTWriter
from .DataWriter.XLSXWriter import XLSXWriter
from .DataWriter.RedisWriter import RedisWriter
from .DataWriter.MySQLWriter import MySQLWriter
from .DataWriter.MongoWriter import MongoWriter


class ProcessFactory(object):
    config_getter_map = {
        GetterConfig.RAPIConfig: APIGetter,
        GetterConfig.RCSVConfig: CSVGetter,
        GetterConfig.RESConfig: ESScrollGetter,
        GetterConfig.RJsonConfig: JsonGetter,
        GetterConfig.RXLSXConfig: XLSXGetter,
        GetterConfig.RAPIBulkConfig: APIBulkGetter,
        GetterConfig.RRedisConfig: RedisGetter,
        GetterConfig.RMySQLConfig: MySQLGetter,
        GetterConfig.RMongoConfig: MongoGetter
    }

    config_writer_map = {
        WriterConfig.WCSVConfig: CSVWriter,
        WriterConfig.WESConfig: ESWriter,
        WriterConfig.WJsonConfig: JsonWriter,
        WriterConfig.WTXTConfig: TXTWriter,
        WriterConfig.WXLSXConfig: XLSXWriter,
        WriterConfig.WRedisConfig: RedisWriter,
        WriterConfig.WMySQLConfig: MySQLWriter,
        WriterConfig.WMongoConfig: MongoWriter
    }

    @staticmethod
    def create_getter(config):
        """
        create a getter based on config
        :return: getter
        """
        for config_class, getter_class in ProcessFactory.config_getter_map.items():
            if isinstance(config, config_class):
                return getter_class(config)
        raise ValueError("create_getter must pass one of the instance of [RAPIConfig, RCSVConfig, RESConfig, "
                         "RJsonConfig, RXLSXConfig, RAPIBulkConfig, RRedisConfig, RMySQLConfig, RMongoConfig]")

    @staticmethod
    def create_writer(config):
        """
        create a writer based on config
        :return: a writer
        """
        for config_class, writer_class in ProcessFactory.config_writer_map.items():
            if isinstance(config, config_class):
                return writer_class(config)
        else:
            raise ValueError("create_writer must pass one of the instance of [WCSVConfig, WESConfig, WJsonConfig, "
                             "WTXTConfig, WXLSXConfig, WRedisConfig, WMySQLConfig, WMongoConfig]")
