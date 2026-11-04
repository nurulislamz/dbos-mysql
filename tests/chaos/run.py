import argparse
import dataclasses
import random
import sys

from tests.chaos.harness import ALL_FAULTS, ChaosConfig, run_chaos


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the crash test gate")
    parser.add_argument("--backend", choices=["sqlite", "mysql", "postgres"], required=True)
    parser.add_argument("--runs", type=int, default=20)
    parser.add_argument("--seed", type=int, default=None, help="Run one seed, to reproduce a failure")
    parser.add_argument("--workflows", type=int, default=1000)
    parser.add_argument("--fault-seconds", type=float, default=15.0)
    parser.add_argument("--faults", default=",".join(ALL_FAULTS))
    args = parser.parse_args()

    seeds = [args.seed] if args.seed is not None else [random.randrange(2**31) for _ in range(args.runs)]  # noqa: S311
    failures = 0
    for number, seed in enumerate(seeds, start=1):
        config = ChaosConfig(args.backend, seed, tuple(filter(None, args.faults.split(","))), workflows=args.workflows, fault_seconds=args.fault_seconds)
        result = run_chaos(config)
        summary = {key: value for key, value in dataclasses.asdict(result).items() if key != "violations"}
        print(f"run {number}/{len(seeds)} {'CLEAN' if not result.violations else 'FAILED'} {summary}", flush=True)
        for violation in result.violations[:12]:
            print(f"  {violation}", flush=True)
        failures += int(bool(result.violations))
    print(f"{args.backend}: {len(seeds) - failures} clean, {failures} failed", flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
