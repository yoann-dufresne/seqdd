
import importlib
import inspect
import logging
import pkgutil

from seqdd.register.data_type import DataContainer
from seqdd.register.sources import DataSource


class SourceManager:
    """
    Class to handle all kind of available source of data
    """

    def get_sources() -> list[DataSource]:
        """
        Returns a list of all available data sources.

        :returns: A list of DataSource instances.
        """
        return find_subclasses_in_package('seqdd.register.sources', DataSource)


    def __init__(self, tmpdir: str, bindir: str, logger: logging.Logger) -> None:
        """

        :param tmpdir: The temporary directory path. Where the downloaded intermediate files are located.
        :param bindir: Where the helper binaries tools are stored.
        :param logger: The logger object for logging messages.
        """
        
        data_containers = find_subclasses_in_package('seqdd.register.data_type', DataContainer)
        data_sources = find_subclasses_in_package('seqdd.register.sources', DataSource)
        print(f"Available data containers: {data_containers}")
        print(f"Available data sources: {data_sources}")
        
        for data_container in data_containers:
            source = get_declared_source_type(data_container)
            print(f"Data container {data_container.__name__} is declared with source type: {source}")



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