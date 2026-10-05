"""Operational incident handover entry point; see docs/handoff-contract.md."""
import argparse
import asyncio
from pathlib import Path
from fleetserve.operations.handoff import reconcile

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    asyncio.run(reconcile(args.input.resolve(), args.output.resolve()))
