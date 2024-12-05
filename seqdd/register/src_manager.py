
import importlib
import inspect
import logging
import pkgutil
from collections.abc import KeysView
from types import ModuleType
from typing import Type

from .data_type import DataType


class SourceManager:
    """
    Class to handle all kind of available source of data
    """


    def __init__(self, tmpdir: str, bindir: str, logger: logging.Logger) -> None:
        """

        :param tmpdir: The temporary directory path. Where the downloaded intermediate files are located.
        :param bindir: Where the helper binaries tools are stored.
        :param logger: The logger object for logging messages.
        """
        available_data_sources = DataSourceLoader().items()
        self.sources = {name: klass(tmpdir, bindir, logger) for name, klass in available_data_sources}


    def keys(self) -> KeysView[str]:
        """
        :return: the name of the available sources for instance 'ena', 'ncbi', ...
        """
        return self.sources.keys()


    def get(self, source_name:str) -> Type[DataType] | None:
        """
        :param source_name: the name of the source
        :return: The DataSource corresponding to the source_name or None if the source_name is not found.
        """
        return self.sources.get(source_name, None)


class DataSourceLoader:
    """
    class to discover and download the DataSources

    To be registered

    1. The DataSource must be in a module in seqdd.register.data_sources
    2. The class must inherit and implement :class:`seqdd.register.data_sources.DatSource` abstract class

    """
    _inst = None

    def __new__(cls):
        """
        This class implement a Singleton pattern
        """
        if cls._inst is None:
            cls._inst = super().__new__(cls)
        return cls._inst


    def __init__(self) -> None:
        """Initialize a DataSourceLoader"""
        self._src_module_name = 'seqdd.register.data_type'
        self._data_sources = {klass.__name__.lower(): klass for klass in self._get_available_data_sources()}


    def keys(self) -> list[str]:
        """
        :return: The list of the names of available data sources
        """
        return list(self._data_sources.keys())


    def data_sources(self) -> list[Type[DataType]]:
        """
        :return: The list of data sources
        """
        return list(self._data_sources.values())


    def __getitem__(self, ds_name) -> Type[DataType]:
        """
        :param ds_name: the name of the data source (in lower case)
        :return: The corresponding class (not the object).
        :raise KeyError: if the ds_name does not exists
        """
        try:
            return self._data_sources[ds_name]
        except KeyError:
            raise KeyError(f"The data source {ds_name} does not exists. The available data source are: {self.keys()}")


    def items(self) -> list[tuple[str, Type[DataType]]]:
        """

        :return: The list of tuple data source name, DataSource
        """
        return list(self._data_sources.items())


    def _list_and_load_sources(self) -> list[ModuleType]:
        """
        :return: The list of modules in sources package
        """
        # load the root data_source package
        module = importlib.import_module(self._src_module_name)

        # List all sub-modules
        submodules = [name for _, name, _ in pkgutil.iter_modules(module.__path__)]

        # Dynamically load each sub module and add them to a list
        loaded_sources = [importlib.import_module(f"{self._src_module_name}.{submodule}") for submodule in submodules]
        return loaded_sources


    def _get_available_data_sources(self) -> set[Type[DataType]]:
        """
        dynamically load available data sources **class** (not instantiated)
        a data source is:

         * a Class which is in data_sources package
         * This class must inherits from :class:`seqdd.register.data_sources.DatSource`
         * This class must implement :class:`seqdd.register.data_sources.DatSource`

        """
        def is_data_source(ds):
            return issubclass(ds, DataType) and not inspect.isabstract(ds)

        src_modules = self._list_and_load_sources()
        klasses = set()
        for module in src_modules:
            for _, kl in inspect.getmembers(module, inspect.isclass):
                if is_data_source(kl):
                    klasses.add(kl)
                    continue

        return klasses
