"""A CPU-executable regional inference release platform."""
from .service import Fleet
from .errors import FleetError

__all__ = ['Fleet', 'FleetError']
