import logging
import uuid
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, Tuple, Union

import requests
from orchestration.common.af_utils import normalize_frequency
from orchestration.common.collection_utils import search_dict
from orchestration.common.eventservice import get_events

logger = logging.getLogger(__name__)

START_EVENT = "STARTED"
COMPLETE_EVENT = "FINISH|FAILED"

FINISH_STATE = "FINISH"

INCLUSION_VARIABLE = "calculator_backfill_inclusion_ids"
LIST_LOG_CAP = 20


def _safe_get_events(config: dict[str, Any], query_params: dict[str, Any]) -> list[dict[str, Any]]:
    """Call event service but tolerate empty-result RuntimeError from helper."""
    try:
        events = get_events(config=config, query_params=query_params)
        return events if isinstance(events, list) else []
    except RuntimeError as exc:
        if "Failed to fetch valid data" in str(exc):
            logger.info("No events for query=%s", query_params)
            return []
        raise
    except requests.exceptions.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            logger.info("No events for query=%s (404)", query_params)
            return []
        raise


def fetch_events(
    config: dict[str, Any],
    reporting_date: str,
    event_type: str,
    *,
    calculator_id: str | None = None,
    frequency: str | None = None,
) -> list[dict[str, Any]]:
    """Fetch events of one type for a single reporting date.

    `calculator_id` is only ever supplied for START_EVENT queries by the backfill DAG.
    Completion events are fetched unscoped and grouped by taskId client-side, because
    completions correlate to starts by runId alone - see the design doc, section 3.
    """
    if event_type == START_EVENT:
        query = {
            "taskEventType": "START_EVENT",
            "successful": "true",
            "reporting-date": reporting_date,
        }
    else:
        query = {
            "type": "CALC_EVENT",
            "reporting-date": reporting_date,
            "STATE": COMPLETE_EVENT,
        }

    if frequency:
        query["frequency"] = frequency

    if calculator_id:
        query["taskId"] = calculator_id

    return _safe_get_events(config=config, query_params=query)


# -- field extraction -------------------------------------------------------
#
# taskId and runId sit at *opposite* paths in the two event types, and neither
# exists at the other's path:
#
#   START     event.additionalData.taskId / .runId
#   COMPLETE  context.data.taskId         / .runId
#
# So there is no single fixed path that works for both. Each extractor below
# tries the paths we have actually observed, in the order that is correct for
# its event type, then falls back to `search_dict` for anything unobserved.
# The explicit paths keep extraction deterministic where the shape is known;
# the fallback keeps it working if the platform moves a field.


def _dig(event: Any, *path: str) -> Any:
    """Walk a fixed key path, returning None rather than raising on any miss."""
    node = event
    for part in path:
        if not isinstance(node, dict):
            return None
        node = node.get(part)
    return node


def _search(event: Any, key: str) -> Any:
    """`search_dict` with its miss-behaviour normalised to None.

    Call sites elsewhere in the codebase treat the result as falsy-on-miss
    (e.g. `if not search_dict(event, key="jobLink")`), but the exact miss value
    is not contractual, so empty results are normalised here.
    """
    try:
        value = search_dict(event, key=key)
    except (KeyError, AttributeError, TypeError, IndexError):
        return None
    if value is None or value == "" or value == {} or value == []:
        return None
    return value


def _first_present(event: Dict[str, Any], key: str, *paths: tuple) -> Union[str, None]:
    for path in paths:
        value = _dig(event, *path)
        if value:
            return str(value)
    value = _search(event, key)
    return str(value) if value else None


def extract_task_id(event: Dict[str, Any]) -> Union[str, None]:
    """Calculator UUID, wherever it lives in this event type."""
    return _first_present(
        event,
        "taskId",
        ("event", "additionalData", "taskId"),
        ("context", "data", "taskId"),
    )


def extract_start_run_id(event: Dict[str, Any]) -> Union[str, None]:
    return _first_present(
        event,
        "runId",
        ("event", "additionalData", "runId"),
        ("context", "data", "runId"),
    )


def extract_complete_run_id(event: Dict[str, Any]) -> Union[str, None]:
    return _first_present(
        event,
        "runId",
        ("context", "data", "runId"),
        ("event", "additionalData", "runId"),
    )


def extract_reporting_date(event: Dict[str, Any]) -> Union[str, None]:
    """Reporting date off a START event.

    Deliberately *not* a `search_dict` lookup: START spells the key
    `reporting-date` while COMPLETE spells it `reportingDate`, so no single
    key works across both event types.
    """
    ctx_data = _dig(event, "context", "data")
    if not isinstance(ctx_data, dict):
        return None
    value = ctx_data.get("reporting-date") or ctx_data.get("reportingDate")
    return str(value) if value else None


def extract_calculator_name(event: Dict[str, Any]) -> Union[str, None]:
    """Calculator class name off a START event - the catalogue is name-keyed."""
    value = _dig(event, "context", "data", "class")
    return str(value) if value else None


def is_finish_event(event: Dict[str, Any]) -> bool:
    """True for STATE=FINISH completions; False for FAILED and anything else."""
    state = _dig(event, "event", "additionalData", "STATE") or _search(event, "STATE")
    return bool(state) and str(state).strip().upper() == FINISH_STATE


def normalised_frequency(value: Any) -> Union[str, None]:
    """Collapse a frequency onto its canonical short code via `normalize_frequency`.

    A single event can carry two vocabularies at once - the completion sample has
    `additionalData.FREQUENCY="DAILY"` alongside `context.data.frequency="D"` - so
    anything that groups or looks up by frequency must normalise first or it will
    treat the same frequency as two.

    Unrecognised values (`normalize_frequency` returns None for anything outside
    FREQ_TO_CODE) fall back to the upper-cased raw value rather than to None, so
    two genuinely different unknown frequencies stay distinct as group keys.
    """
    if not value:
        return None
    try:
        text = str(value).strip()
        return normalize_frequency(text) or text.upper()
    except (AttributeError, TypeError):
        return None


def canonical_uuid(value: Any) -> Union[str, None]:
    """Canonical lowercase-hyphenated UUID, or None if `value` is not one.

    Normalises braced and `urn:uuid:` forms without inventing a casing policy
    for the event service, which receives this string as `query["taskId"]`.
    """
    try:
        return str(uuid.UUID(str(value).strip()))
    except (ValueError, AttributeError, TypeError):
        return None


# -- inclusion allowlist ----------------------------------------------------


def truncate_for_log(values: list[str], cap: int = LIST_LOG_CAP) -> str:
    shown = list(values)[:cap]
    suffix = f" (+{len(values) - cap} more)" if len(values) > cap else ""
    return f"{shown}{suffix}"


def canonicalise_calculator_ids(raw_values: Any, source_label: str) -> list[str]:
    """Canonicalise a list of calculator UUIDs, naming *every* bad value at once.

    This is the highest-value guard in the redesign: it turns the most likely
    rollout mistake - a config value still holding calculator class names, which
    is exactly what the old exclusion list held - from "silently matches nothing"
    into an immediate error, before any fetch or POST.

    Deduped preserving first-occurrence order, so log sections appear in the
    order the operator wrote them.
    """
    if not isinstance(raw_values, list) or not all(isinstance(c, str) for c in raw_values):
        raise ValueError(f"{source_label} must be a JSON array of strings")

    canonical: list[str] = []
    invalid: list[str] = []

    for raw in raw_values:
        text = raw.strip()
        if not text:
            continue
        as_uuid = canonical_uuid(text)
        if as_uuid is None:
            invalid.append(raw)
        else:
            canonical.append(as_uuid)

    if invalid:
        raise ValueError(
            f"{source_label} must contain calculator UUIDs. "
            f"Not valid UUIDs: {truncate_for_log(invalid)}"
        )

    return list(dict.fromkeys(canonical))


def resolve_inclusion_allowlist(
    env_raw: Any,
    override_raw: Any,
    variable_name: str = INCLUSION_VARIABLE,
) -> Tuple[list[str], list[str], list[str], list[str]]:
    """Resolve the calculators to replay as `env AND override`.

    The Variable is the environment's allowlist; the param can only narrow it.
    A calculator absent from the Variable is unreachable by param by design -
    that is the fail-closed property, and widening it means editing the Variable.

    Returns (resolved, env_ids, override_ids, dropped_not_in_env).
    Raises ValueError with a distinct message for "nothing configured" versus
    "override does not overlap", so the two are not confused during an incident.
    """
    env_ids = canonicalise_calculator_ids(env_raw or [], variable_name)
    override_ids = canonicalise_calculator_ids(override_raw or [], "included_calculator_ids")

    if not env_ids:
        raise ValueError(
            f"No inclusions configured for this environment: {variable_name} is empty. "
            "The backfill is fail-closed and will not replay anything until it is populated."
        )

    if not override_ids:
        return list(env_ids), env_ids, override_ids, []

    env_set = set(env_ids)
    override_set = set(override_ids)
    resolved = [i for i in env_ids if i in override_set]
    dropped = [i for i in override_ids if i not in env_set]

    if dropped:
        logger.warning(
            "included_calculator_ids entries are not in %s and were dropped: %s",
            variable_name,
            truncate_for_log(dropped),
        )

    if not resolved:
        raise ValueError(
            "included_calculator_ids has no overlap with the environment allowlist. "
            f"override={truncate_for_log(override_ids)} "
            f"{variable_name}={truncate_for_log(env_ids)}"
        )

    return resolved, env_ids, override_ids, dropped


def event_ts_for_sort(event: Dict[str, Any]) -> str:
    event_block = event.get("event", {}) if isinstance(event, dict) else {}
    return str(
        event_block.get("eventTimestamp")
        or event_block.get("publicationTimestamp")
        or event.get("context", {}).get("publicationTimestamp")
        or ""
    )


def index_latest_by_run_id(
    events: list[dict[str, Any]],
    extractor: Callable[[dict[str, Any]], str | None],
) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for event in events:
        run_id = extractor(event)
        if not run_id:
            continue
        existing = indexed.get(run_id)
        if existing is None or event_ts_for_sort(event) >= event_ts_for_sort(existing):
            indexed[run_id] = event
    return indexed


def group_completions_by_calculator(
    events: Iterable[dict[str, Any]],
    allowed_calculator_ids: Iterable[str],
) -> Tuple[dict[str, list[dict[str, Any]]], int, int]:
    """Bucket unscoped completion events by calculator UUID, keeping only the allowlist.

    Returns (grouped, unmatched_count, unparseable_count).

    `unmatched` counts completions whose taskId is a valid UUID belonging to some
    other calculator - entirely normal, since the fetch is unscoped. `unparseable`
    counts completions with no taskId, or one that is not a UUID at all; that is
    the signature of a field rename or a malformed identifier upstream, and it is
    what the caller's tripwire warns on.
    """
    allowed = set(allowed_calculator_ids)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    unmatched = 0
    unparseable = 0

    for event in events:
        task_id = canonical_uuid(extract_task_id(event))
        if task_id is None:
            unparseable += 1
            continue
        if task_id not in allowed:
            unmatched += 1
            continue
        grouped[task_id].append(event)

    return dict(grouped), unmatched, unparseable


def assign_run_numbers(
    starts_by_run: dict[str, dict[str, Any]],
    success_run_ids: set[str],
) -> dict[str, int]:
    """
    Tag each started run as run_number 1 or 2 (two buckets only).

    Within each (calculator_id, reporting-date, frequency) group, runs are ordered by the START
    event's publicationTimestamp (tiebreak by runId for determinism). The first successful pair
    (a started run whose complete is a FINISHED event, i.e. its runId is in `success_run_ids`) and
    every run before it are run_number=1; every run after it is run_number=2. A group with no
    successful pair is entirely run_number=2.

    This is the *fallback* path. Where the platform states `context.data.run_number` on the START
    event, that value wins - see `resolve_run_number`.
    """
    groups: dict[tuple[Any, Any, Any], list[tuple[str, str]]] = defaultdict(list)

    for run_id, event in starts_by_run.items():
        event_block = event.get("event", {}) if isinstance(event, dict) else {}
        ctx = event.get("context", {}).get("data", {})
        key = (
            extract_task_id(event),
            extract_reporting_date(event),
            normalised_frequency(ctx.get("frequency")),
        )
        pub_ts = event_block.get("publicationTimestamp", "")
        groups[key].append((pub_ts, run_id))

    run_numbers: dict[str, int] = {}

    for items in groups.values():
        items.sort(key=lambda t: (t[0], t[1]))  # START publicationTimestamp, then runId

        first_success = next((i for i, (_, run_id) in enumerate(items) if run_id in success_run_ids), None)

        for i, (_, run_id) in enumerate(items):
            run_numbers[run_id] = 1 if (first_success is not None and i <= first_success) else 2

    return run_numbers


def resolve_run_number(
    start_event: Dict[str, Any],
    inferred_run_number: int | None,
) -> Tuple[int, str]:
    """Prefer the platform-stated run number; fall back to the timestamp heuristic.

    Returns (run_number, source) where source is "event" or "inferred".

    The fallback is not defensive padding: a long backfill window can straddle the
    date `run_number` was introduced upstream, so historical STARTs legitimately
    lack it while recent ones carry it.
    """
    stated = _dig(start_event, "context", "data", "run_number")

    if stated is not None:
        try:
            stated_int = int(str(stated).strip())
        except (ValueError, AttributeError, TypeError):
            logger.warning(
                "run_number on event is not an integer, falling back to inference: value=%r run_id=%s",
                stated,
                extract_start_run_id(start_event),
            )
        else:
            if stated_int in (1, 2):
                if inferred_run_number is not None and inferred_run_number != stated_int:
                    logger.warning(
                        "run_number disagreement: event=%d inferred=%d run_id=%s "
                        "(event value used; evidence toward retiring assign_run_numbers)",
                        stated_int,
                        inferred_run_number,
                        extract_start_run_id(start_event),
                    )
                return stated_int, "event"

            logger.warning(
                "run_number outside the two-bucket model, falling back to inference: value=%r run_id=%s",
                stated,
                extract_start_run_id(start_event),
            )

    return (inferred_run_number or 2), "inferred"


def resolve_sla_time(
    calculator_name: str | None,
    frequency: str | None,
    run_number: int,
) -> str | None:
    """Resolve SLA time for a calculator run using CalculatorCatalogueProvider.

    The frequency is normalised to its short code first, so a lookup does not
    depend on which vocabulary the caller happened to read it from.

    Args:
        calculator_name: The class/name of the calculator (e.g., 'capitalcalc')
        frequency: The frequency of the run, in either vocabulary ('DAILY' or 'D')
        run_number: The run number (1 or 2)

    Returns:
        ISO-8601 SLA duration string (e.g., 'PT2H30M') or None if not found/invalid.
    """
    from orchestration.common.calculator_metadata import calculator_catalogue_provider

    frequency_code = normalised_frequency(frequency)

    if not calculator_name or not frequency_code or run_number < 1:
        return None

    metadata = calculator_catalogue_provider.get_metadata(calculator_name)
    return metadata.sla_for(frequency_code, run_number)
