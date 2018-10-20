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


__version__ = '1.4.7'
