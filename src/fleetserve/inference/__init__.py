"""CPU inference fabric over actual MLServer batching and multiprocessing."""
from .fabric import ServingFabric
from .runtime import MatrixRuntime

__all__ = ['ServingFabric', 'MatrixRuntime']
