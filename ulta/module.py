from types import ModuleType
from typing import Type, Union, Optional, List
import inspect
import importlib


def load_plugins(plugins_list: List[str]):
    for p in plugins_list:
        if not p:
            continue
        try:
            _ = importlib.import_module(p)
        except ImportError as e:
            raise RuntimeError(f'Unknown package {str(p)}: ensure package exists') from e


def load_class(module: Union[str, ModuleType], *, base_class: Optional[Type] = None, member_name: Optional[str] = None):
    '''
    Lookup requested class from module.
    First try to find descendant of base_class if specified.
    Next look by class_name exact match
    '''

    try:
        if isinstance(module, str):
            module = importlib.import_module(module)
    except ImportError as e:
        raise RuntimeError(f'Unknown package {str(module)}: ensure package exists') from e

    if base_class is not None:
        classes = inspect.getmembers(module, lambda t: inspect.isclass(t) and base_class in inspect.getmro(t))
        if classes:
            return classes[0][1]
        raise RuntimeError(f'Package {str(module)} doesn\'t export {str(base_class)} type')
    if member_name is not None:
        members = inspect.getmembers(module)
        for m in members:
            if m[0] == member_name:
                return m[1]
        raise RuntimeError(f'Package {str(module)} doesn\'t export {str(member_name)} member')
    raise RuntimeError('Please specify types to load')
