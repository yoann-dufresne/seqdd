
import importlib
import pkgutil


class SourceManager:

    def __init__(self, tmpdir, bindir, logger):
        src_modules = SourceManager.list_and_load_sources()
        self.sources = dict()
        for src_module in src_modules:
            src_init = getattr(src_module, src_module.naming['classname'])
            self.sources[src_module.naming['key']] = src_init(tmpdir, bindir, logger)

    def keys(self):
        return self.sources.keys()
    
    def get(self, key):
        return self.sources.get(key, None)


    # --- Submodules ---
    
    def source_keys():
        src_modules = SourceManager.list_and_load_sources()
        return [mod.naming['key'] for mod in src_modules]


    def list_and_load_sources():
        # Charger le module principal
        src_module_name = 'seqdd.register.sources'
        module = importlib.import_module(src_module_name)
        
        # Lister les sous-modules
        submodules = [name for _, name, _ in pkgutil.iter_modules(module.__path__)]
        
        # Charger dynamiquement chaque sous-module et les ajouter à une liste
        loaded_sources = [importlib.import_module(f"{src_module_name}.{submodule}") for submodule in submodules]
        
        return loaded_sources