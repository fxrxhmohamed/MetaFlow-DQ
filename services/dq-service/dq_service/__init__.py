"""Shared data quality engine: one validate() for every dataset."""
from .engine import DQResult, RuleResult, validate

__all__ = ["DQResult", "RuleResult", "validate"]
