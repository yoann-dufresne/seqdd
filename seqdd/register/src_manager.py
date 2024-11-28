
import importlib
import inspect
import logging
import pkgutil
from collections.abc import KeysView
from types import ModuleType
from typing import Type

from .data_sources import DataSource


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


    def get(self, source_name:str) -> Type[DataSource] | None:
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
        self._src_module_name = 'seqdd.register.data_sources'
        self._data_sources = {klass.__name__.lower(): klass for klass in self._get_available_data_sources()}


    def keys(self) -> list[str]:
        """
        :return: The list of the names of available data sources
        """
        return list(self._data_sources.keys())


    def data_sources(self) -> list[DataSource]:
        """
        :return: The list of data sources
        """
        return list(self._data_sources.values())


    def __getitem__(self, ds_name) -> DataSource:
        """
        :param ds_name: the name of the data source (in lower case)
        :return: The corresponding
        :raise KeyError: if the ds_name does not exists
        """
        try:
            return self._data_sources[ds_name]
        except KeyError:
            raise KeyError(f"The data source {ds_name} does not exists. The available data source are: {self.keys()}")


    def items(self) -> list[tuple[str, DataSource]]:
        """

        :return: The list of tuple data source name, DataSource
        """
        return list(self._data_sources.items())


    def _list_and_load_sources(self) -> list[ModuleType]:
        """
        :return: The list of modules in sources package
        """
        # Charger le module principal

        module = importlib.import_module(self._src_module_name)

        # Lister les sous-modules
        submodules = [name for _, name, _ in pkgutil.iter_modules(module.__path__)]

        # Charger dynamiquement chaque sous-module et les ajouter à une liste
        loaded_sources = [importlib.import_module(f"{self._src_module_name}.{submodule}") for submodule in submodules]
        return loaded_sources


    def _get_available_data_sources(self) -> set:
        """
        dynamically fill the register with the different available data sources
        a data source is:

         * a Class which is in data_sources package
         * This class must inherits from :class:`seqdd.register.data_sources.DatSource`
         * This class must implement :class:`seqdd.register.data_sources.DatSource`

        :param tmpdir: The temporary directory path. Where the downloaded intermediate files are located.
        :param bindir: Where the helper binaries tools are stored.
        :param logger: The logger object for logging messages.
        """
        def is_data_source(ds):
            return issubclass(ds, DataSource) and not inspect.isabstract(ds)

        src_modules = self._list_and_load_sources()
        klasses = set()
        for module in src_modules:
            klasses.update(
                {ds for ds in [klass for _, klass in inspect.getmembers(module, inspect.isclass)] if is_data_source(ds)}
            )
        return klasses
