
import importlib
import inspect
import logging
import pkgutil

from seqdd.register.data_type import DataContainer


class DataTypeManager:
    """
    Class to handle all kind of available types of data
    """
    _datatypes: dict[str, DataContainer] = None
    
    def __init__(self, logger: logging.Logger, tmpdir: str = '/tmp', bindir: str = 'bin') -> None:
        """
        Initializes the DataTypeManager object.

        :param logger: The logger object for logging messages.
        :param tmpdir: The temporary directory path. Where the downloaded intermediate files are located.
        :param bindir: The binary directory path. Where the helper binaries tools are stored.
        """
        self.logger = logger
        self.tmpdir = tmpdir
        self.bindir = bindir
    
    def get_data_types(self) -> dict[str, DataContainer]:
        """
        Returns a list of all available data types.

        :returns: A list of DataContainer instances.
        """
        if self._datatypes is None:
            self.fill_datatypes()
        return self._datatypes
    
    def fill_datatypes(self) -> None:
        """
        Fills the datatypes class variable with all available data types.
        This method finds all subclasses of DataContainer in the 'seqdd.register.data_type' package
        and populates the datatypes dictionary with their names and declared source types.
        """
        
        data_container_classes = find_subclasses_in_package('seqdd.register.data_type', DataContainer)
        # debug
        # print(f"Available data containers: {data_container_classes}")
        
        self._datasources = {}
        self._datatypes = {}
        # Loop to instantiate each data source linked to each data container
        for data_container_cls in data_container_classes:
            # Detect the declared data source type of the data container and instantiate it
            data_source_cls = get_declared_source_type(data_container_cls)
            if data_source_cls.__name__ not in self._datasources:
                self._datasources[data_source_cls.__name__] = data_source_cls(self.tmpdir, self.bindir, self.logger)
            data_source = self._datasources[data_source_cls.__name__]
            # Instanciate the data container
            self._datatypes[data_container_cls.__name__.lower()] = data_container_cls(data_source, self.logger)
        # debug print
        # for name, datatype in self._datatypes.items():
        #     print(f"Data container {name} is declared with source type: {datatype}")




def find_subclasses_in_package(package_name: str, base_class: type) -> list[type]:
    subclasses = []

    # Import the package (if not already imported)
    package = importlib.import_module(package_name)
    package_path = package.__path__

    # Parcours tous les sous-modules du package
    for _, module_name, is_pkg in pkgutil.walk_packages(package_path, prefix=package_name + "."):
        try:
            module = importlib.import_module(module_name)
        except Exception as e:
            print(f"Erreur d'import pour {module_name}: {e}")
            continue

        for name, obj in inspect.getmembers(module, inspect.isclass):
            # Vérifie si c'est une sous-classe de base_class, mais pas base_class elle-même
            if issubclass(obj, base_class) and obj is not base_class:
                subclasses.append(obj)

    return subclasses


from typing import get_type_hints

def get_declared_source_type(cls):
    while cls is not object:
        try:
            hints = get_type_hints(cls.__init__)
            return hints.get('source')
        except Exception:
            pass
        cls = cls.__base__
    return None