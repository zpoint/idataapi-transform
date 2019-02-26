"""convert data from a format to another format, read or write from file or database, suitable for iDataAPI"""

from .cli import main
from .DataProcess.Config.ConfigUtil import WriterConfig
from .DataProcess.Config.ConfigUtil import GetterConfig
from .DataProcess.ProcessFactory import ProcessFactory


class ManualConfig(object):
    @staticmethod
    def set_config(ini_path):
        from .DataProcess.Config.MainConfig import main_config_box
        from .DataProcess.Config.DefaultValue import DefaultVal
        main_config_box.read_config(ini_path)
        DefaultVal.refresh()

    @staticmethod
    def disable_log():
        from .DataProcess.Config.LogConfig import remove_log
        remove_log()

    @staticmethod
    def set_log_path(log_path, max_log_file_bytes):
        """
        :param log_path: directory where log stores, i.e ===> /Desktop/logs/
        :param max_log_file_bytes: max log file size, in bytes, i.e: 5242880(5MB)
        :return:
        """
        from .DataProcess.Config.MainConfig import main_config_box
        from .DataProcess.Config.DefaultValue import DefaultVal
        main_config_box.config_log(log_path, max_log_file_bytes)


__version__ = '1.6.5'
