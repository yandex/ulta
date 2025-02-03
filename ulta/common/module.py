from types import ModuleType
import inspect
import importlib


def load_plugins(plugins_list: list[str]):
    for p in plugins_list:
        if not p:
            continue
        try:
            _ = importlib.import_module(p)
        except ImportError as e:
            raise PluginModuleError(f'Unknown package {str(p)}: ensure package exists') from e


def load_class(module: str | ModuleType, *, base_class: type | None = None, member_name: str | None = None):
    '''
    Lookup requested class from module.
    First try to find descendant of base_class if specified.
    Next look by class_name exact match
    '''

    try:
        if isinstance(module, str):
            module = importlib.import_module(module)
    except ImportError as e:
        raise PluginModuleError(f'Unknown package {str(module)}: ensure package exists') from e

    if base_class is not None:
        classes = inspect.getmembers(module, lambda t: inspect.isclass(t) and base_class in inspect.getmro(t))
        if classes:
            return classes[0][1]
        raise PluginModuleError(f'Package {str(module)} doesn\'t export {str(base_class)} type')
    if member_name is not None:
        members = inspect.getmembers(module)
        for m in members:
            if m[0] == member_name:
                return m[1]
        raise PluginModuleError(f'Package {str(module)} doesn\'t export {str(member_name)} member')
    raise PluginModuleError('Please specify types to load')


def load_plugin(plugin: str, base_class: type) -> tuple[str, bool]:
    try:
        pkg, member_name = plugin.rsplit('.', maxsplit=1)
    except ValueError:
        pkg, member_name = None, None

    try:
        # first try to load plugin as package var then as class var
        plugin_obj = load_class(pkg, member_name=member_name)
        if isinstance(plugin_obj, base_class):
            return plugin_obj, False
        if issubclass(plugin_obj, base_class):
            return plugin_obj, True
    except TypeError:
        # plugin_obj is not a type
        pass
    except PluginModuleError:
        pass

    plugin_obj = load_class(plugin, base_class=base_class)
    return plugin_obj, True


class PluginModuleError(RuntimeError): ...
