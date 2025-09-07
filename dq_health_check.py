# dq_health_check.py
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Set, Tuple
import pandas as pd

OK = "✅"
FAIL = "🛑"
INFO = "📝"

# --------- Models ---------
@dataclass(frozen=True)
class DataQualityRule:
    data_quality_control_name: str
    header: str  # comma-separated list, or single header

    def normalized_name(self) -> str:
        return self.data_quality_control_name.strip().lower()

    def header_set(self) -> Set[str]:
        if not self.header:
            return set()
        return {h.strip().lower() for h in str(self.header).split(",") if h.strip()}

@dataclass
class DataSet:
    name: str
    dataframe: pd.DataFrame                     # the rules table for this source
    dq_rules: List[DataQualityRule] = field(default_factory=list)
    fields: Dict[str, Dict] = field(default_factory=dict)

    def rule_names(self) -> Set[str]:
        return {r.normalized_name() for r in self.dq_rules}

    def rule_by_name(self) -> Dict[str, DataQualityRule]:
        return {r.normalized_name(): r for r in self.dq_rules}

# --------- Helpers ---------
def rules_from_df(df: pd.DataFrame) -> List[DataQualityRule]:
    required = {"data_quality_control_name", "header"}
    if not required.issubset(df.columns):
        missing = list(required - set(df.columns))
        raise ValueError(f"Rules sheet missing columns: {missing}. "
                         f"Expected: {sorted(required)}")
    return [
        DataQualityRule(
            data_quality_control_name=str(row["data_quality_control_name"]),
            header="" if pd.isna(row["header"]) else str(row["header"]),
        )
        for _, row in df.iterrows()
    ]

def find_missing_headers(sources: Dict[str, DataSet]) -> Dict[str, List[str]]:
    # Example: check that each field's declared header exists in dataframe
    result: Dict[str, List[str]] = {}
    for _, src in sources.items():
        missing: List[str] = []
        for field_name, meta in (src.fields or {}).items():
            header = meta.get("header")
            if not header or header not in src.dataframe.columns:
                missing.append(str(field_name))
        if missing:
            result[src.name] = missing
    return result

def compare_rule_counts(main: DataSet, others: Dict[str, DataSet]) -> Tuple[bool, str]:
    m = len(main.dataframe)
    for _, s in others.items():
        n = len(s.dataframe)
        if n != m:
            return False, f"{FAIL} '{main.name}' has {m} rules; '{s.name}' has {n}."
    return True, f"{OK} All sources have the same number of rules as '{main.name}'."

def exclusive_rule_names(main: DataSet, others: Dict[str, DataSet]) -> Dict[str, List[str]]:
    exclusives: Dict[str, List[str]] = {}
    main_names = main.rule_names()
    for _, s in others.items():
        only_there = s.rule_names() - main_names
        if only_there:
            exclusives[s.name] = sorted(only_there)
    return exclusives

def sync_check(main: DataSet, others: Dict[str, DataSet]) -> Dict[str, List[str]]:
    mismatches: Dict[str, List[str]] = {}
    main_index = main.rule_by_name()
    for _, s in others.items():
        for r in s.dq_rules:
            mr = main_index.get(r.normalized_name())
            if not mr:
                continue
            if r.header_set() != mr.header_set():
                mismatches.setdefault(s.name, []).append(r.data_quality_control_name)
    return mismatches

# --------- Orchestration that RETURNS a result dict ---------
def run_health_check(sources: Dict[str, DataSet], main_key: str | None = None):
    """
    Returns a dict you can render to HTML/JSON. If main_key is None,
    the first source (in dict order) is used as main.
    """
    if not sources:
        raise ValueError("No sources uploaded.")

    missing = find_missing_headers(sources)
    if main_key is None:
        main_key = next(iter(sources))
    main = sources[main_key]
    others = {k: v for k, v in sources.items() if k != main_key}

    same_count, rule_msg = compare_rule_counts(main, others)
    exclusives = exclusive_rule_names(main, others) if same_count else {}
    mismatches = sync_check(main, others) if same_count else {}

    return {
        "ok": (not missing) and same_count and (not exclusives) and (not mismatches),
        "missing_headers": missing,          # {source_name: [fields]}
        "main_source": main.name,
        "rule_count_ok": same_count,
        "rule_count_msg": rule_msg,
        "exclusives": exclusives,            # {source_name: [rule_names]}
        "mismatches": mismatches,            # {source_name: [rule_names]}
        "emoji": {"OK": OK, "FAIL": FAIL, "INFO": INFO},
    }
