import logging
from datetime import date, timedelta
from typing import Any, List

import pendulum
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from orchestration.common.af_utils import (
    FREQ_TO_CODE,
    decorated_message,
    get_config,
    normalize_frequency,
)
from orchestration.common.calculator_metadata import calculator_catalogue_provider
from orchestration.common.constants import DAG_DEFAULT_ARGUMENTS
from orchestration.observability.obs_run_tasks import obs_complete_run, obs_start_run

from dags.control.dag_constants import CAPITAL_TAGS
from orchestration.obs_backfill_helpers import (
    COMPLETE_EVENT,
    INCLUSION_VARIABLE,
    START_EVENT,
    assign_run_numbers,
    extract_calculator_name,
    extract_complete_run_id,
    extract_reporting_date,
    extract_start_run_id,
    fetch_events,
    group_completions_by_calculator,
    index_latest_by_run_id,
    is_finish_event,
    resolve_inclusion_allowlist,
    resolve_run_number,
    resolve_sla_time,
    truncate_for_log,
)

logger = logging.getLogger("dags")

FAILED_ID_CAP = 50


@dag(
    dag_id="observability_backfill_dag",
    description="Backfill historical calculator runs from enriched events into observability /start and /complete",
    default_args=DAG_DEFAULT_ARGUMENTS,
    tags=[CAPITAL_TAGS.MAINTENANCE],
    schedule=None,
    start_date=pendulum.datetime(year=2021, month=1, day=1, tz="UTC"),
    catchup=False,
    params={
        "n_days": Param(default=30, type="integer", minimum=1, title="Number of reporting days to scan"),
        "end_date": Param(
            default=None,
            type=["string", "null"],
            description="Last reporting date to scan, YYYY-MM-DD. Omit for today (UTC).",
        ),
        "frequency": Param(
            default=None,
            type=["string", "null"],
            description="Optional filter - D/DAILY or M/MONTHLY. Normalised to the short code before use.",
        ),
        "tenant_id": Param(default="CAPITAL", type="string"),
        "dry_run": Param(default=False, type="boolean", description="Log intended calls without posting"),
        "included_calculator_ids": Param(
            default=[],
            type=["array", "null"],
            description=(
                f"Calculator UUIDs to replay for this run. Narrows the {INCLUSION_VARIABLE} "
                "Variable - it cannot widen it. Omit to replay every calculator in the Variable."
            ),
        ),
    },
)
def observability_backfill_dag():

    @task(task_id="validate_params")
    def validate_params() -> dict[str, Any]:
        from airflow.operators.python import get_current_context

        conf = get_current_context()["dag_run"].conf or {}

        n_days = int(conf.get("n_days", 30))
        if n_days < 1:
            raise ValueError("n_days must be >= 1")

        raw_end = conf.get("end_date") or pendulum.now("UTC").date().isoformat()
        try:
            date.fromisoformat(raw_end)
        except (ValueError, TypeError):
            raise ValueError(f"end_date must be YYYY-MM-DD, got: {raw_end!r}")

        # Normalise to the platform's short code, so everything downstream - the
        # two fetch queries, run-number grouping and the SLA lookup - sees one
        # vocabulary. The accepted set is derived from FREQ_TO_CODE rather than
        # restated here, so it stays correct if a frequency is added upstream.
        raw_frequency = conf.get("frequency")
        frequency = normalize_frequency(raw_frequency) if raw_frequency else None
        if raw_frequency and not frequency:
            raise ValueError(
                f"frequency must be one of {sorted(FREQ_TO_CODE)} - got {raw_frequency!r}"
            )

        # -- inclusion allowlist ------------------------------------------
        env_raw = (
            Variable.get(INCLUSION_VARIABLE, default_var=[], deserialize_json=True) or []
        )
        resolved_ids, env_ids, override_ids, dropped = resolve_inclusion_allowlist(
            env_raw, conf.get("included_calculator_ids")
        )

        logger.info(
            decorated_message(
                f"[allowlist]  env_inclusions={len(env_ids)} override={len(override_ids)} "
                f"resolved={len(resolved_ids)} dropped_not_in_env={truncate_for_log(dropped)}"
            )
        )

        return {
            "n_days": n_days,
            "end_date": raw_end,
            "frequency": frequency,
            "tenant_id": conf.get("tenant_id", "CAPITAL"),
            "dry_run": bool(conf.get("dry_run", False)),
            "included_calculator_ids": resolved_ids,
        }

    @task(task_id="build_reporting_dates")
    def build_reporting_dates(user_data: dict[str, Any]) -> list[str]:
        end = date.fromisoformat(user_data["end_date"])
        n = user_data["n_days"]
        reporting_dates = [
            (end - timedelta(days=i)).isoformat() for i in range(n - 1, -1, -1)
        ]
        logger.info(
            decorated_message(
                f"Reporting dates window: {reporting_dates[0]} to {reporting_dates[-1]} ({n} days)"
            )
        )
        return reporting_dates

    @task(task_id="replay_runs_for_date")
    def replay_runs_for_date(reporting_date: str, data: dict[str, Any]) -> dict[str, Any]:
        config = get_config()
        frequency = data.get("frequency")
        tenant_id = data["tenant_id"]
        dry_run = data.get("dry_run", False)
        allowlist: list[str] = data["included_calculator_ids"]

        # 1. Completions: one unscoped fetch per date.
        #
        # Completions pair to starts by runId alone, never by calculator, so the
        # completion fetch does not need calculator scoping - which also avoids
        # depending on whether taskId filters the type=CALC_EVENT query shape
        # server-side. Grouping by taskId client-side keeps orphan_complete
        # meaning exactly what it always meant, at no extra API calls.
        completes_raw = fetch_events(config, reporting_date, COMPLETE_EVENT, frequency=frequency)
        completes_by_calc, unmatched_taskid, unparseable_taskid = group_completions_by_calculator(
            completes_raw, allowlist
        )
        matched_to_allowlist = sum(len(v) for v in completes_by_calc.values())

        logger.info(
            "[fetch]      date=%s completion_events_fetched=%d matched_to_allowlist=%d "
            "unmatched_taskid=%d unparseable_taskid=%d",
            reporting_date,
            len(completes_raw),
            matched_to_allowlist,
            unmatched_taskid,
            unparseable_taskid,
        )

        # Tripwire. A systematic taskId mismatch - field renamed upstream, or a
        # malformed UUID in the Variable - yields zero completions and posts
        # start-only for every run, which is indistinguishable from "these runs
        # never completed" unless it is called out here.
        if unparseable_taskid or (completes_raw and not matched_to_allowlist):
            logger.warning(
                "[fetch]      date=%s SUSPECT taskId extraction: fetched=%d matched=%d "
                "unmatched=%d unparseable=%d - if matched=0 the whole date will post start-only",
                reporting_date,
                len(completes_raw),
                matched_to_allowlist,
                unmatched_taskid,
                unparseable_taskid,
            )

        # 2. Per-calculator replay, sequentially, one log section each.
        started_total = 0
        matched_pairs = 0
        posted_both = 0
        start_only = 0
        orphan_complete = 0
        failed_count = 0
        run1 = 0
        run2 = 0
        calculators_processed = 0
        calculators_with_no_events = 0
        failed_run_ids: list[str] = []

        for calculator_id in allowlist:
            starts_raw = fetch_events(
                config,
                reporting_date,
                START_EVENT,
                calculator_id=calculator_id,
                frequency=frequency,
            )

            if not starts_raw:
                calculators_with_no_events += 1
                continue

            starts_by_run = index_latest_by_run_id(starts_raw, extract_start_run_id)
            calc_completes = completes_by_calc.get(calculator_id, [])

            # completes_by_run drives what gets POSTed to /complete, so it spans
            # ALL terminal states - a FAILED run must be completed in
            # observability, not left perpetually in flight.
            # success_run_ids drives run numbering, so it stays FINISH-only: a
            # failed run must not satisfy the "first successful pair" rule.
            completes_by_run = index_latest_by_run_id(calc_completes, extract_complete_run_id)
            finished_by_run = index_latest_by_run_id(
                [e for e in calc_completes if is_finish_event(e)], extract_complete_run_id
            )
            success_run_ids = {rid for rid in finished_by_run if rid in starts_by_run}
            inferred_numbers = assign_run_numbers(starts_by_run, success_run_ids)

            calc_name = next(
                (
                    name
                    for name in (extract_calculator_name(ev) for ev in starts_by_run.values())
                    if name
                ),
                None,
            )
            calc_metadata = calculator_catalogue_provider.get_metadata(calc_name) if calc_name else None

            calc_orphans = sum(1 for rid in completes_by_run if rid not in starts_by_run)
            calc_pairs = sum(1 for rid in starts_by_run if rid in completes_by_run)

            calculators_processed += 1
            started_total += len(starts_by_run)
            matched_pairs += calc_pairs
            orphan_complete += calc_orphans

            logger.info(
                "[calculator] date=%s calculator_id=%s calculator=%s metadata=%s",
                reporting_date,
                calculator_id,
                calc_name,
                calc_metadata,
            )
            logger.info(
                "[fetch]      date=%s calculator=%s run_start_events=%d "
                "run_completion_events_all_states=%d run_completion_events_successful=%d",
                reporting_date,
                calc_name,
                len(starts_by_run),
                len(completes_by_run),
                len(finished_by_run),
            )
            logger.info(
                "[correlate]  date=%s calculator=%s runs_started=%d runs_paired_with_completion=%d "
                "runs_never_completed=%d completions_without_matching_start=%d",
                reporting_date,
                calc_name,
                len(starts_by_run),
                calc_pairs,
                len(starts_by_run) - calc_pairs,
                calc_orphans,
            )
            if calc_orphans:
                logger.warning(
                    "date=%s calculator=%s orphan_complete=%d (no matching start - skipped)",
                    reporting_date,
                    calc_name,
                    calc_orphans,
                )

            for run_id, start_event in starts_by_run.items():
                complete_event = completes_by_run.get(run_id)
                run_number, run_number_source = resolve_run_number(
                    start_event, inferred_numbers.get(run_id)
                )
                rd = extract_reporting_date(start_event) or reporting_date
                run_frequency = (
                    start_event.get("context", {}).get("data", {}).get("frequency") or frequency
                )

                if run_number == 1:
                    run1 += 1
                else:
                    run2 += 1

                try:
                    sla = resolve_sla_time(calc_name, run_frequency, run_number)
                except Exception:  # SLA is a log field only - never fail a replay for it
                    logger.warning(
                        "SLA lookup failed for calculator=%s frequency=%s run_number=%d",
                        calc_name,
                        run_frequency,
                        run_number,
                        exc_info=True,
                    )
                    sla = None

                if dry_run:
                    logger.info(
                        "[replay]     date=%s calculator=%s run_id=%s run_number=%d "
                        "run_number_source=%s posted=DRY-RUN(%s) sla=%s",
                        rd,
                        calc_name,
                        run_id,
                        run_number,
                        run_number_source,
                        "start+complete" if complete_event else "start",
                        sla,
                    )
                    if complete_event:
                        posted_both += 1
                    else:
                        start_only += 1
                    continue

                try:
                    posted_run_id = (
                        obs_start_run(
                            enriched_event=start_event,
                            tenant_id=tenant_id,
                            calculator_metadata=calc_metadata,
                            run_number=run_number,
                        )
                        or run_id
                    )

                    if complete_event:
                        obs_complete_run(
                            run_id=posted_run_id,
                            event=complete_event,
                            tenant_id=tenant_id,
                            reporting_date=rd,
                        )
                        posted_both += 1
                    else:
                        start_only += 1

                    logger.info(
                        "[replay]     date=%s calculator=%s run_id=%s run_number=%d "
                        "run_number_source=%s posted=%s terminal_state=%s sla=%s",
                        rd,
                        calc_name,
                        run_id,
                        run_number,
                        run_number_source,
                        "start+complete" if complete_event else "start",
                        ("FINISH" if is_finish_event(complete_event) else "FAILED")
                        if complete_event
                        else "none",
                        sla,
                    )
                except Exception:
                    logger.exception(
                        "Replay failed for run_id=%s date=%s calculator_id=%s calculator=%s run_number=%d",
                        run_id,
                        rd,
                        calculator_id,
                        calc_name,
                        run_number,
                    )
                    failed_count += 1
                    if len(failed_run_ids) < FAILED_ID_CAP:
                        failed_run_ids.append(run_id)

        logger.info(
            "[summary]    date=%s calculators=%d calculators_with_no_events=%d "
            "posted_both=%d start_only=%d failed=%d run1=%d run2=%d orphan_complete=%d",
            reporting_date,
            calculators_processed,
            calculators_with_no_events,
            posted_both,
            start_only,
            failed_count,
            run1,
            run2,
            orphan_complete,
        )

        return {
            "reporting_date": reporting_date,
            "started_total": started_total,
            "matched_pairs": matched_pairs,
            "posted_both": posted_both,
            "start_only": start_only,
            "orphan_complete": orphan_complete,
            "failed": failed_count,
            "run1": run1,
            "run2": run2,
            "failed_run_ids": failed_run_ids,
            "calculators_processed": calculators_processed,
            "calculators_with_no_events": calculators_with_no_events,
            "unmatched_taskid": unmatched_taskid,
            "unparseable_taskid": unparseable_taskid,
        }

    @task(task_id="summarise", trigger_rule=TriggerRule.ALL_DONE)
    def summarise(per_date: List[dict[str, Any]]) -> dict[str, Any]:
        results = [r for r in per_date or [] if isinstance(r, dict)]
        total_started = sum(r.get("started_total", 0) for r in results)
        matched_pairs = sum(r.get("matched_pairs", 0) for r in results)
        posted_both = sum(r.get("posted_both", 0) for r in results)
        start_only = sum(r.get("start_only", 0) for r in results)
        orphan_complete = sum(r.get("orphan_complete", 0) for r in results)
        failed = sum(r.get("failed", 0) for r in results)
        run1 = sum(r.get("run1", 0) for r in results)
        run2 = sum(r.get("run2", 0) for r in results)
        unparseable_taskid = sum(r.get("unparseable_taskid", 0) for r in results)

        all_failed_ids: list[str] = []
        for r in results:
            all_failed_ids.extend(r.get("failed_run_ids", []))

        summary = {
            "total_started": total_started,
            "matched_pairs": matched_pairs,
            "posted_both": posted_both,
            "start_only": start_only,
            "orphan_complete": orphan_complete,
            "failed": failed,
            "run1": run1,
            "run2": run2,
            "unparseable_taskid": unparseable_taskid,
            "failed_run_ids": all_failed_ids[:FAILED_ID_CAP],
            "per_date": results,
        }

        logger.info(decorated_message("OBS backfill summary"))
        logger.info(
            "started=%d matched_pairs=%d posted_both=%d "
            "start_only=%d orphan=%d failed=%d run1=%d run2=%d unparseable_taskid=%d",
            total_started,
            matched_pairs,
            posted_both,
            start_only,
            orphan_complete,
            failed,
            run1,
            run2,
            unparseable_taskid,
        )

        return summary

    # -- wiring ----------------------------------------------------
    start = EmptyOperator(task_id="START")
    finish = EmptyOperator(task_id="FINISH")
    user_params = validate_params()
    dates = build_reporting_dates(user_params)
    per_date_results = replay_runs_for_date.partial(data=user_params).expand(reporting_date=dates)

    start >> user_params
    summarise(per_date_results) >> finish


observability_backfill_dag()
