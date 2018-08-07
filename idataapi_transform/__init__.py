"""convert data from a format to another format, read or write from file or database, suitable for iDataAPI"""

from .cli import main
from .DataProcess.Config.ConfigUtil import WriterConfig
from .DataProcess.Config.ConfigUtil import GetterConfig
from .DataProcess.ProcessFactory import ProcessFactory
__version__ = '1.4.2'
