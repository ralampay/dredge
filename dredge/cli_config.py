"""Normalize parsed inputs without loading environment or provider dependencies."""


def build_runtime_config(namespace):
    return {name: value for name, value in vars(namespace).items()
            if name != "doctor" and value is not None}
