from types import MethodType


def _on_demand(cls):
    def rebind(dct):
        for name, obj in dct.items():
            try:
                wrap, obj = obj
                if wrap is MethodType:
                    setattr(cls, name, wrap(obj, None, cls))
            except (TypeError, ValueError):
                pass
    rebind(cls.__dict__)
    return cls


def bind(callable):
    return MethodType, callable
bind.on_demand = _on_demand
