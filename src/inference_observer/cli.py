from __future__ import annotations

import argparse

from .engine import run


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="replay continuous-batching LLM serving telemetry")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    run(args.input, args.output)
