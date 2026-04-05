"""CLI entry point for the leadgen pipeline."""

import argparse
import asyncio
import logging
import os
import sys

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="leadgen",
        description="Automated lead generation pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # run
    run_parser = subparsers.add_parser("run", help="Run scraping agents")
    run_parser.add_argument(
        "--agent",
        type=str,
        default=None,
        help="Run a specific agent by name (e.g. ksl_job_seekers). Omit to run all.",
    )
    run_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate run without writing to database",
    )

    # score
    score_parser = subparsers.add_parser("score", help="Score unscored leads")
    score_parser.add_argument(
        "--rescore",
        action="store_true",
        help="Re-score all leads, not just unscored ones",
    )

    # export
    export_parser = subparsers.add_parser("export", help="Export leads to CSV")
    export_parser.add_argument(
        "--tier",
        type=str,
        choices=["A", "B", "C", "all"],
        default="all",
        help="Export only leads of a specific tier (default: all)",
    )
    export_parser.add_argument(
        "--output",
        type=str,
        default="exports/leads.csv",
        help="Output file path (default: exports/leads.csv)",
    )

    # status
    subparsers.add_parser("status", help="Show agent and pipeline status")

    return parser


def _ensure_data_dir() -> None:
    """Create the data/ directory if it doesn't exist."""
    os.makedirs("data", exist_ok=True)


async def _cmd_run(args: argparse.Namespace) -> None:
    """Run scraping agents via the Orchestrator."""
    from leadgen.agents.orchestrator import Orchestrator

    _ensure_data_dir()

    orchestrator = Orchestrator()
    await orchestrator.db.init_db()

    try:
        if args.agent:
            print(f"Running agent: {args.agent}" + (" (dry run)" if args.dry_run else ""))
            await orchestrator.start(agent_names=[args.agent])
        else:
            print("Running all agents" + (" (dry run)" if args.dry_run else ""))
            await orchestrator.start()
    finally:
        await orchestrator.db.close()

    print("Run complete.")


async def _cmd_score(args: argparse.Namespace) -> None:
    """Score leads using the ScoringEngine."""
    from leadgen.scoring.engine import ScoringEngine

    engine = ScoringEngine()

    # Load leads from the orchestrator's in-memory store (or DB in future)
    _ensure_data_dir()

    from leadgen.agents.orchestrator import Orchestrator

    orchestrator = Orchestrator()
    await orchestrator.db.init_db()

    try:
        leads = orchestrator._existing_leads

        if not leads:
            print("No leads found to score.")
            return

        if args.rescore:
            to_score = leads
            print(f"Re-scoring all {len(to_score)} leads...")
        else:
            to_score = [ld for ld in leads if ld.total_score == 0]
            print(f"Scoring {len(to_score)} unscored leads (of {len(leads)} total)...")

        scored_count = 0
        for lead in to_score:
            engine.score_lead(lead)
            scored_count += 1

        print(f"Scored {scored_count} leads.")

        # Print tier summary
        tiers = {"A": 0, "B": 0, "C": 0, "D": 0}
        for lead in leads:
            if lead.tier in tiers:
                tiers[lead.tier] += 1
        print("\nTier Summary:")
        for tier, count in tiers.items():
            print(f"  Tier {tier}: {count}")
    finally:
        await orchestrator.db.close()


async def _cmd_export(args: argparse.Namespace) -> None:
    """Export leads to CSV via the Exporter."""
    from leadgen.pipeline.exporter import Exporter

    _ensure_data_dir()

    from leadgen.agents.orchestrator import Orchestrator

    orchestrator = Orchestrator()
    await orchestrator.db.init_db()

    try:
        leads = orchestrator._existing_leads

        if not leads:
            print("No leads found to export.")
            return

        exporter = Exporter()
        tier_filter = args.tier if args.tier != "all" else None

        filepath = await exporter.export_csv(
            leads=leads,
            filepath=args.output,
            tier_filter=tier_filter,
        )
        print(f"Exported leads to {filepath}")
    finally:
        await orchestrator.db.close()


async def _cmd_status(args: argparse.Namespace) -> None:
    """Show pipeline status from the Orchestrator."""
    _ensure_data_dir()

    from leadgen.agents.orchestrator import Orchestrator

    orchestrator = Orchestrator()
    await orchestrator.db.init_db()

    try:
        status = await orchestrator.get_status()

        print("Pipeline Status")
        print("=" * 50)
        print(f"  Total agents configured:  {status['total_agents']}")
        print(f"  Enabled agents:           {status['enabled_agents']}")
        print(f"  Total leads in memory:    {status['total_leads']}")
        print(f"  Checked at:               {status['checked_at']}")

        agents = status.get("agents", {})
        if agents:
            print("\nAgent Details:")
            print(f"  {'Name':<25} {'Status':<12} {'Found':<8} {'New':<8} {'Last Run'}")
            print("  " + "-" * 75)
            for name, info in agents.items():
                print(
                    f"  {name:<25} {info.get('status', 'unknown'):<12} "
                    f"{info.get('items_found', 0):<8} {info.get('items_new', 0):<8} "
                    f"{info.get('last_run', 'never')}"
                )
        else:
            print("\n  No agent run history yet.")
    finally:
        await orchestrator.db.close()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    try:
        if args.command == "run":
            asyncio.run(_cmd_run(args))
        elif args.command == "score":
            asyncio.run(_cmd_score(args))
        elif args.command == "export":
            asyncio.run(_cmd_export(args))
        elif args.command == "status":
            asyncio.run(_cmd_status(args))
    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)
    except Exception as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        logger.debug("Full traceback:", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
