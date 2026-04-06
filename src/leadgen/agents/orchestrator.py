"""
Central orchestrator: manages all scraping agents, runs the full
normalize -> dedupe -> score pipeline, and handles scheduling.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import yaml

from leadgen.models.lead import Lead, AgentRun
from leadgen.pipeline.normalizer import Normalizer
from leadgen.pipeline.deduplicator import Deduplicator
from leadgen.db.queue import JobQueue

# Import all agent classes for auto-registration
from leadgen.agents.ksl import (
    KSLJobSeekersAgent, KSLServicesOfferedAgent, KSLBusinessForSaleAgent,
    KSLResumePostsAgent, KSLCareerServicesAgent, KSLGigWorkersAgent,
    KSLProfessionalServicesAgent, KSLCoachingConsultingAgent,
)

logger = logging.getLogger(__name__)

# Map of platform+name to agent class for lookup
_ALL_AGENTS: dict[str, type] = {
    "ksl_job_seekers": KSLJobSeekersAgent,
    "ksl_services_offered": KSLServicesOfferedAgent,
    "ksl_business_for_sale": KSLBusinessForSaleAgent,
    "ksl_resume_posts": KSLResumePostsAgent,
    "ksl_career_services": KSLCareerServicesAgent,
    "ksl_gig_workers": KSLGigWorkersAgent,
    "ksl_professional_services": KSLProfessionalServicesAgent,
    "ksl_coaching_consulting": KSLCoachingConsultingAgent,
}


class Orchestrator:
    """
    Central scheduler that manages all scraping agents.

    Loads agent configs from YAML, runs them concurrently,
    and pushes results through the processing pipeline.
    """

    # Registry mapping platform names to agent classes.
    # Populated at import time or via register_agent().
    _agent_registry: dict[str, type] = {}

    def __init__(self, config_dir: str = "config/agents", db_path: str = "data/leadgen.db"):
        self.config_dir = config_dir
        self.agent_configs: dict[str, dict] = {}
        self.agent_statuses: dict[str, dict] = {}
        self.normalizer = Normalizer()
        self.deduplicator = Deduplicator()
        self.db = JobQueue(db_path)
        self._existing_leads: list[Lead] = []

        # Load all YAML agent configs
        self._load_configs()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self, agent_names: Optional[list[str]] = None) -> None:
        """
        Run agents concurrently.

        Args:
            agent_names: If provided, run only these agents.
                         Otherwise run all registered/enabled agents.
        """
        configs = self._select_agents(agent_names)
        if not configs:
            logger.warning("No agents to run.")
            return

        logger.info("Starting %d agent(s): %s", len(configs), list(configs.keys()))

        tasks = [
            self.run_agent(name)
            for name in configs
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for name, result in zip(configs, results):
            if isinstance(result, Exception):
                logger.error("Agent %s failed: %s", name, result)
                self.agent_statuses[name] = {
                    "status": "error",
                    "error": str(result),
                    "last_run": datetime.now(timezone.utc).isoformat(),
                }

    async def run_agent(self, agent_name: str) -> None:
        """
        Load config, instantiate, run a single agent, and process results.
        """
        config = self.agent_configs.get(agent_name)
        if not config:
            raise ValueError(f"Unknown agent: {agent_name}")

        agent_meta = config.get("agent", {})
        platform = agent_meta.get("platform", "unknown")
        enabled = agent_meta.get("enabled", True)

        if not enabled:
            logger.info("Agent %s is disabled, skipping.", agent_name)
            return

        # Create run record
        run = AgentRun(agent_name=agent_name, status="running")

        logger.info("Running agent: %s (platform=%s)", agent_name, platform)

        try:
            # Look up agent class by name first, then by platform
            agent_cls = _ALL_AGENTS.get(agent_name) or self._agent_registry.get(platform)
            if agent_cls is None:
                logger.warning(
                    "No agent class registered for '%s' (platform '%s'). "
                    "Register via Orchestrator.register_agent().",
                    agent_name, platform,
                )
                run.status = "error"
                run.error_message = f"No agent class for: {agent_name}"
                self._update_status(agent_name, run)
                return

            # Try (config, db) signature first (KSL agents),
            # fall back to (name, platform, config, db) for BaseAgent direct
            try:
                agent_instance = agent_cls(config=config, db=self.db)
            except TypeError:
                agent_instance = agent_cls(
                    name=agent_name, platform=platform,
                    config=config, db=self.db,
                )
            raw_scrapes = await agent_instance.run()

            # BaseAgent.run() returns None (stores internally) or list[dict]
            if raw_scrapes is None:
                raw_scrapes = []

            run.items_found = len(raw_scrapes)

            # Process through pipeline
            new_count, dupe_count = await self.run_pipeline(raw_scrapes)
            run.items_new = new_count
            run.items_duplicate = dupe_count
            run.status = "completed"

        except Exception as exc:
            logger.error("Agent %s errored: %s", agent_name, exc, exc_info=True)
            run.status = "error"
            run.error_message = str(exc)[:2000]

        run.finished_at = datetime.now(timezone.utc)
        self._update_status(agent_name, run)

        logger.info(
            "Agent %s finished: found=%d new=%d dupes=%d status=%s",
            agent_name, run.items_found, run.items_new, run.items_duplicate, run.status,
        )

    async def run_pipeline(self, raw_scrapes: list[dict]) -> tuple[int, int]:
        """
        Process raw scrapes through the full pipeline.

        Steps per scrape:
          1. Normalize  (raw dict -> Lead)
          2. Deduplicate (check existing leads)
          3. If new: score and store
          4. If duplicate: merge, re-score, update

        Returns:
            (new_count, duplicate_count)
        """
        new_count = 0
        dupe_count = 0

        for raw in raw_scrapes:
            # Step 1: Normalize
            lead = await self.normalizer.normalize(raw)
            if lead is None:
                continue

            # Step 2: Deduplicate
            existing = await self.deduplicator.is_duplicate(lead, self._existing_leads)

            if existing is None:
                # New lead
                # Step 3: Score (via external scorer -- placeholder)
                # Scoring is handled by the scoring module, not inlined here.
                self._existing_leads.append(lead)
                new_count += 1
                logger.debug("New lead: %s %s", lead.first_name, lead.last_name)
            else:
                # Duplicate -- merge
                merged = await self.deduplicator.merge_leads(existing, lead)
                # Step 4: Re-score after merge (placeholder for scoring module)
                dupe_count += 1
                logger.debug(
                    "Duplicate merged: %s %s (sources=%d)",
                    merged.first_name, merged.last_name, merged.sources_count,
                )

        return new_count, dupe_count

    async def schedule_loop(self) -> None:
        """
        Infinite scheduling loop.

        Checks agent schedules and runs agents when their polling
        interval has elapsed. Uses asyncio.sleep between checks.
        """
        logger.info("Starting schedule loop...")

        # Track last run time per agent
        last_run: dict[str, float] = {}

        while True:
            now = time.time()

            for name, config in self.agent_configs.items():
                agent_meta = config.get("agent", {})
                if not agent_meta.get("enabled", True):
                    continue

                scraping_cfg = config.get("scraping", {})
                interval_mins = scraping_cfg.get("schedule_minutes", 60)
                interval_secs = interval_mins * 60

                agent_last = last_run.get(name, 0)
                if now - agent_last >= interval_secs:
                    logger.info("Scheduled run for agent: %s", name)
                    try:
                        await self.run_agent(name)
                    except Exception as exc:
                        logger.error("Scheduled agent %s failed: %s", name, exc)
                    last_run[name] = time.time()

            # Sleep 60 seconds between schedule checks
            await asyncio.sleep(60)

    async def get_status(self) -> dict:
        """
        Return status summary of all agents.

        Returns:
            Dict with per-agent status (last run, items found, errors)
            and overall pipeline stats.
        """
        return {
            "agents": self.agent_statuses.copy(),
            "total_agents": len(self.agent_configs),
            "enabled_agents": sum(
                1 for cfg in self.agent_configs.values()
                if cfg.get("agent", {}).get("enabled", True)
            ),
            "total_leads": len(self._existing_leads),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    # ------------------------------------------------------------------
    # Agent registration
    # ------------------------------------------------------------------

    @classmethod
    def register_agent(cls, platform: str, agent_class: type) -> None:
        """
        Register an agent class for a given platform.

        Args:
            platform: Platform identifier matching the 'platform' field in agent YAML.
            agent_class: The agent class (must implement async def run() -> list[dict]).
        """
        cls._agent_registry[platform] = agent_class
        logger.debug("Registered agent class for platform: %s", platform)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_configs(self) -> None:
        """Load all YAML agent configs from the config directory."""
        config_path = Path(self.config_dir)
        if not config_path.is_dir():
            logger.warning("Agent config directory not found: %s", self.config_dir)
            return

        for yaml_file in sorted(config_path.glob("*.yaml")):
            try:
                with open(yaml_file, "r", encoding="utf-8") as fh:
                    config = yaml.safe_load(fh)
                if config and "agent" in config:
                    name = config["agent"].get("name", yaml_file.stem)
                    self.agent_configs[name] = config
                    self.agent_statuses[name] = {
                        "status": "idle",
                        "last_run": None,
                        "items_found": 0,
                        "items_new": 0,
                        "errors": 0,
                    }
                    logger.debug("Loaded agent config: %s", name)
            except Exception as exc:
                logger.error("Failed to load %s: %s", yaml_file, exc)

        logger.info("Loaded %d agent config(s) from %s", len(self.agent_configs), self.config_dir)

    def _select_agents(self, agent_names: Optional[list[str]]) -> dict[str, dict]:
        """Return the subset of configs to run."""
        if agent_names:
            selected = {}
            for name in agent_names:
                if name in self.agent_configs:
                    selected[name] = self.agent_configs[name]
                else:
                    logger.warning("Requested agent '%s' not found in configs.", name)
            return selected
        # All enabled agents
        return {
            name: cfg
            for name, cfg in self.agent_configs.items()
            if cfg.get("agent", {}).get("enabled", True)
        }

    def _update_status(self, agent_name: str, run: AgentRun) -> None:
        """Update the in-memory status tracker for an agent."""
        self.agent_statuses[agent_name] = {
            "status": run.status,
            "last_run": (run.finished_at or run.started_at).isoformat(),
            "items_found": run.items_found,
            "items_new": run.items_new,
            "items_duplicate": run.items_duplicate,
            "error": run.error_message,
        }
