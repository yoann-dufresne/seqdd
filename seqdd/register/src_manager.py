
import importlib
import logging
import pkgutil
from collections.abc import KeysView
from typing import Type

from sources import Source

class SourceManager:

    def __init__(self, tmpdir: str, bindir: str, logger: logging.Logger) -> None:
        """

        :param tmpdir: The temporary directory path. Where the downloaded intermediate files are located.
        :param bindir: Where the helper binaries tools are stored.
        :param logger: The logger object for logging messages.
        """
        src_modules = SourceManager.list_and_load_sources()
        self.sources = dict()
        for src_module in src_modules:
            if not hasattr(src_module, 'naming'):
                continue
            src_init = getattr(src_module, src_module.naming['classname'])
            self.sources[src_module.naming['key']] = src_init(tmpdir, bindir, logger)

    def keys(self) -> KeysView[str]:
        """
        :return: the name of the available sources for instance 'ena', 'ncbi', ...
        """
        return self.sources.keys()
    
    def get(self, source_name:str) -> Type[Source] | None:
        """
        :param source_name: the name of the source
        :return: The Source corresponding to the source_name
        """
        return self.sources.get(source_name, None)

    # --- Submodules ---
    @staticmethod
    def source_keys() -> list[str]:
        """
        :return: The list of available source name
        """
        src_modules = SourceManager.list_and_load_sources()
        return [mod.naming['key'] for mod in src_modules if hasattr(mod, 'naming')]

    @staticmethod
    def list_and_load_sources() -> list[Type[Source]]:
        """
        :return: The list of modules in sources package
        """
        # Charger le module principal
        src_module_name = 'seqdd.register.sources'
        module = importlib.import_module(src_module_name)
        
        # Lister les sous-modules
        submodules = [name for _, name, _ in pkgutil.iter_modules(module.__path__)]
        
        # Charger dynamiquement chaque sous-module et les ajouter à une liste
        loaded_sources = [importlib.import_module(f"{src_module_name}.{submodule}") for submodule in submodules]
        
        return loaded_sources
