from __future__ import annotations

import argparse
import hashlib
import json
import mimetypes
import os
import shutil
import zipfile
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

ROLES = ("red", "blue")
SOURCE_GLOBAL_FILES = (
    "bank-manifest.json",
    "research-manifest.json",
    "director-experiments.jsonl",
)
TABLE_NAMES = (
    "episodes",
    "traces",
    "messages",
    "events",
    "director_records",
    "artifacts",
)
def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_relation(selected: Path, candidate: Path) -> str:
    """Describe whether a duplicate capture is byte-identical or a strict prefix."""
    if selected.stat().st_size == candidate.stat().st_size:
        return "equal" if sha256_file(selected) == sha256_file(candidate) else "divergent"
    if candidate.stat().st_size > selected.stat().st_size:
        return "extends_selected" if file_relation(candidate, selected) == "candidate_prefix" else "divergent"
    remaining = candidate.stat().st_size
    with selected.open("rb") as selected_handle, candidate.open("rb") as candidate_handle:
        while remaining:
            candidate_chunk = candidate_handle.read(min(1024 * 1024, remaining))
            selected_chunk = selected_handle.read(len(candidate_chunk))
            if selected_chunk != candidate_chunk:
                return "divergent"
            remaining -= len(candidate_chunk)
    return "candidate_prefix"


def json_compact(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def optional_json(path: Path) -> dict[str, Any] | None:
    return read_json(path) if path.exists() else None


def iso_to_ms(value: str | None) -> int | None:
    if not value:
        return None
    try:
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)
    except ValueError:
        return None


def scalar_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json_compact(value)


def flatten_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                if isinstance(item.get("text"), str):
                    parts.append(item["text"])
                elif isinstance(item.get("content"), str):
                    parts.append(item["content"])
                else:
                    parts.append(json_compact(item))
            else:
                parts.append(str(item))
        return "\n".join(parts)
    if value is None:
        return ""
    return json_compact(value)


def discover_episodes(archive_root: Path) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for episode_path in archive_root.rglob("episode.json"):
        episode = read_json(episode_path)
        episode_id = episode.get("episode_id") or episode_path.parent.name
        raw_paths = {role: episode_path.parent / role / "trace.jsonl" for role in ROLES}
        record = {
            "episode_id": str(episode_id),
            "directory": episode_path.parent,
            "archive_relative_path": episode_path.parent.relative_to(archive_root).as_posix(),
            "episode": episode,
            "complete_roles": sum(path.is_file() for path in raw_paths.values()),
            "raw_bytes": sum(path.stat().st_size for path in raw_paths.values() if path.is_file()),
            "depth": len(episode_path.parent.relative_to(archive_root).parts),
            "raw": {
                role: {
                    "bytes": path.stat().st_size,
                    "sha256": sha256_file(path),
                }
                for role, path in raw_paths.items()
                if path.is_file()
            },
        }
        grouped[str(episode_id)].append(record)

    canonical: list[dict[str, Any]] = []
    alternates: dict[str, list[dict[str, Any]]] = {}
    for episode_id, records in sorted(grouped.items()):
        ordered = sorted(
            records,
            key=lambda item: (
                -item["complete_roles"],
                -item["raw_bytes"],
                item["depth"],
                item["archive_relative_path"],
            ),
        )
        chosen = ordered[0]
        if chosen["complete_roles"] != 2:
            raise ValueError(f"episode lacks paired traces: {episode_id}")
        canonical.append(chosen)
        alternates[episode_id] = ordered
    return canonical, alternates


def load_collection_labels(archive_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    bank: dict[str, Any] = {}
    path = archive_root / "bank-manifest.json"
    if path.exists():
        for entry in read_json(path).get("play_order", []):
            if isinstance(entry, dict) and entry.get("episode_id"):
                bank[str(entry["episode_id"])] = entry
    research: dict[str, Any] = {}
    path = archive_root / "research-manifest.json"
    if path.exists():
        for entry in read_json(path).get("attempts", []):
            if isinstance(entry, dict) and entry.get("episode_id"):
                research[str(entry["episode_id"])] = entry
    return bank, research


def candidate_metadata(directory: Path) -> dict[str, Any] | None:
    candidates = sorted(directory.glob("candidate*.json"))
    return read_json(candidates[0]) if candidates else None


def curation_fields(
    episode_id: str,
    directory: Path,
    bank: dict[str, Any],
    research: dict[str, Any],
) -> dict[str, Any]:
    qualification = optional_json(directory / "qualification.json")
    candidate = candidate_metadata(directory)
    bank_entry = bank.get(episode_id)
    research_entry = research.get(episode_id)

    status = None
    source = None
    for candidate_status, candidate_source in (
        ((bank_entry or {}).get("status"), "replay_bank"),
        ((qualification or {}).get("classification"), "qualification"),
        ((qualification or {}).get("status"), "qualification"),
        ((research_entry or {}).get("status"), "research_manifest"),
        ((candidate or {}).get("status"), "candidate_assessment"),
        ((candidate or {}).get("decision"), "candidate_assessment"),
    ):
        if candidate_status:
            status = str(candidate_status)
            source = candidate_source
            break
    return {
        "curation_status": status or "unassessed",
        "curation_source": source or "none",
        "bank_position": (bank_entry or {}).get("position"),
        "qualified_scope": scalar_text((qualification or {}).get("qualified_scope")),
        "variant": scalar_text((qualification or {}).get("variant") or (candidate or {}).get("variant")),
        "decision": scalar_text((qualification or {}).get("decision")
        or (candidate or {}).get("reason")
        or (candidate or {}).get("arc")
        or (research_entry or {}).get("reason")),
        "qualification_json": json_compact(qualification) if qualification else None,
        "candidate_json": json_compact(candidate) if candidate else None,
        "research_json": json_compact(research_entry) if research_entry else None,
        "bank_json": json_compact(bank_entry) if bank_entry else None,
    }


def table_schemas() -> dict[str, pa.Schema]:
    s = pa.schema
    f = pa.field
    return {
        "events": s([
            f("episode_id", pa.string(), False), f("role", pa.string(), False),
            f("sequence", pa.int64(), False), f("timestamp", pa.string()),
            f("kind", pa.string(), False), f("round", pa.int32()),
            f("stream", pa.string()), f("event_type", pa.string()),
            f("event_subtype", pa.string()), f("claude_session_id", pa.string()),
            f("model", pa.string()), f("source_path", pa.string(), False),
            f("source_line", pa.int64(), False), f("source_sha256", pa.string(), False),
            f("raw_json", pa.string(), False),
        ]),
        "messages": s([
            f("episode_id", pa.string(), False), f("role", pa.string(), False),
            f("sequence", pa.int64(), False), f("timestamp", pa.string()),
            f("timestamp_ms", pa.int64()), f("message_role", pa.string(), False),
            f("content_type", pa.string(), False), f("content", pa.string()),
            f("tool_call_id", pa.string()), f("tool_name", pa.string()),
            f("tool_input_json", pa.string()), f("tool_result_json", pa.string()),
            f("model", pa.string()), f("claude_session_id", pa.string()),
            f("claude_message_id", pa.string()), f("parent_tool_use_id", pa.string()),
            f("source_path", pa.string(), False), f("source_line", pa.int64(), False),
            f("source_sha256", pa.string(), False),
        ]),
        "traces": s([
            f("episode_id", pa.string(), False), f("role", pa.string(), False),
            f("trace_path", pa.string(), False), f("trace_sha256", pa.string(), False),
            f("trace_bytes", pa.int64(), False), f("trace_lines", pa.int64(), False),
            f("started_at", pa.string()), f("ended_at", pa.string()),
            f("duration_seconds", pa.float64()), f("model", pa.string()),
            f("claude_version", pa.string()), f("prompt_path", pa.string()),
            f("prompt_sha256", pa.string()), f("round_count", pa.int32()),
            f("tool_call_count", pa.int64()), f("tool_result_count", pa.int64()),
            f("assistant_text_count", pa.int64()), f("reasoning_block_count", pa.int64()),
            f("raw_metadata_json", pa.string()),
        ]),
        "episodes": s([
            f("episode_id", pa.string(), False), f("range_id", pa.int64()),
            f("started_at", pa.string()), f("ended_at", pa.string()),
            f("duration_seconds", pa.float64()), f("container_count", pa.int32()),
            f("telemetry_cleanup_deleted", pa.int64()),
            f("telemetry_cleanup_remaining", pa.int64()),
            f("telemetry_cleanup_json", pa.string()),
            f("curation_status", pa.string(), False), f("curation_source", pa.string(), False),
            f("qualified_scope", pa.string()), f("bank_position", pa.int32()),
            f("variant", pa.string()), f("decision", pa.string()),
            f("canonical_archive_path", pa.string(), False),
            f("alternate_copy_count", pa.int32(), False),
            f("raw_bytes", pa.int64(), False), f("raw_event_count", pa.int64(), False),
            f("message_count", pa.int64(), False), f("tool_call_count", pa.int64(), False),
            f("tool_result_count", pa.int64(), False),
            f("red_prompt_sha256", pa.string()), f("blue_prompt_sha256", pa.string()),
            f("qualification_json", pa.string()), f("candidate_json", pa.string()),
            f("research_json", pa.string()), f("bank_json", pa.string()),
            f("raw_episode_json", pa.string(), False),
        ]),
        "director_records": s([
            f("sequence", pa.int64(), False), f("timestamp", pa.string()),
            f("episode_id", pa.string()), f("range_id", pa.int64()),
            f("role", pa.string()), f("kind", pa.string()), f("variant", pa.string()),
            f("status", pa.string()), f("outcome", pa.string()),
            f("reason", pa.string()), f("guidance", pa.string()),
            f("notes", pa.string()), f("visibility", pa.string()),
            f("source_path", pa.string(), False), f("source_line", pa.int64(), False),
            f("raw_json", pa.string(), False),
        ]),
        "artifacts": s([
            f("path", pa.string(), False), f("bytes", pa.int64(), False),
            f("sha256", pa.string(), False), f("media_type", pa.string()),
            f("artifact_class", pa.string(), False), f("episode_id", pa.string()),
            f("role", pa.string()),
        ]),
    }


class BatchWriter:
    def __init__(self, path: Path, schema: pa.Schema, compression: str, row_group_size: int):
        self.path = path
        self.schema = schema
        self.compression = compression
        self.row_group_size = row_group_size
        self.rows: list[dict[str, Any]] = []
        self.count = 0
        self.writer = pq.ParquetWriter(
            path,
            schema,
            compression=compression,
            write_page_index=True,
        )

    def add(self, row: dict[str, Any]) -> None:
        self.rows.append(row)
        if len(self.rows) >= self.row_group_size:
            self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        table = pa.Table.from_pylist(self.rows, schema=self.schema)
        self.writer.write_table(table, row_group_size=self.row_group_size)
        self.count += len(self.rows)
        self.rows.clear()

    def close(self) -> int:
        self.flush()
        self.writer.close()
        return self.count


def write_small_table(path: Path, rows: list[dict[str, Any]], schema: pa.Schema, compression: str, row_group_size: int) -> int:
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(
        table,
        path,
        compression=compression,
        row_group_size=row_group_size,
        write_page_index=True,
    )
    return len(rows)


def message_rows(
    episode_id: str,
    role: str,
    event: dict[str, Any],
    timestamp: str | None,
    source_path: str,
    source_line: int,
    source_sha256: str,
    start_sequence: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    message = event.get("message")
    if not isinstance(message, dict):
        return [], [], start_sequence
    message_role = str(message.get("role") or event.get("type") or "unknown")
    content = message.get("content")
    blocks = content if isinstance(content, list) else [{"type": "text", "text": content}] if content is not None else []
    rows: list[dict[str, Any]] = []
    sts: list[dict[str, Any]] = []
    sequence = start_sequence
    model = message.get("model") or event.get("model")
    message_id = message.get("id")
    session_id = event.get("session_id")
    parent_tool_use_id = event.get("parent_tool_use_id")
    timestamp_ms = iso_to_ms(timestamp or event.get("timestamp"))

    for block in blocks:
        if not isinstance(block, dict):
            block = {"type": "text", "text": str(block)}
        block_type = str(block.get("type") or "unknown")
        row_role = message_role
        text = None
        tool_call_id = None
        tool_name = None
        tool_input_json = None
        tool_result_json = None
        sts_message: dict[str, Any] | None = None

        if block_type == "thinking":
            text = flatten_text(block.get("thinking"))
            sts_message = {"role": "assistant", "content": "", "reasoningContent": text}
        elif block_type == "text":
            text = flatten_text(block.get("text"))
            sts_message = {"role": row_role if row_role in {"user", "assistant", "system"} else "assistant", "content": text}
        elif block_type == "tool_use":
            row_role = "assistant"
            tool_call_id = str(block.get("id") or "") or None
            tool_name = str(block.get("name") or "") or None
            tool_input_json = json_compact(block.get("input", {}))
            sts_message = {
                "role": "assistant",
                "content": "",
                "toolCalls": [{
                    "id": tool_call_id or f"tool-{episode_id}-{role}-{sequence}",
                    "function": {"name": tool_name or "unknown", "arguments": tool_input_json},
                }],
            }
        elif block_type == "tool_result":
            row_role = "tool"
            tool_call_id = str(block.get("tool_use_id") or "") or None
            text = flatten_text(block.get("content"))
            tool_result_json = json_compact(block.get("content"))
            sts_message = {"role": "tool", "toolCallId": tool_call_id or "unknown", "content": text}
        else:
            text = json_compact(block)
            sts_message = {"role": row_role if row_role in {"user", "assistant", "system"} else "assistant", "content": text}

        sequence += 1
        rows.append({
            "episode_id": episode_id,
            "role": role,
            "sequence": sequence,
            "timestamp": timestamp or event.get("timestamp"),
            "timestamp_ms": timestamp_ms,
            "message_role": row_role,
            "content_type": block_type,
            "content": text,
            "tool_call_id": tool_call_id,
            "tool_name": tool_name,
            "tool_input_json": tool_input_json,
            "tool_result_json": tool_result_json,
            "model": model,
            "claude_session_id": session_id,
            "claude_message_id": message_id,
            "parent_tool_use_id": parent_tool_use_id,
            "source_path": source_path,
            "source_line": source_line,
            "source_sha256": source_sha256,
        })
        if sts_message is not None:
            if timestamp_ms is not None:
                sts_message["timestamp"] = timestamp_ms
            if model:
                sts_message["model"] = model
            sts.append({"type": "message", "message": sts_message})
    return rows, sts, sequence


def copy_episode_source(source: Path, destination: Path) -> None:
    def ignore(_directory: str, names: list[str]) -> set[str]:
        return {name for name in names if name == "__pycache__" or name.endswith(".pyc")}
    shutil.copytree(source, destination, copy_function=shutil.copy2, ignore=ignore)


def render_dataset_card(config: dict[str, Any], counts: dict[str, int]) -> str:
    dataset = config["dataset"]
    data_license = dataset.get("data_license")
    hub_license = str(data_license).lower() if data_license and not str(data_license).startswith("http") else data_license
    front = {
        "pretty_name": dataset["title"],
        "license": hub_license or "other",
        "language": dataset.get("language", ["en"]),
        "task_categories": ["text-generation"],
        "tags": [
            "agent-traces",
            "agents",
            "cybersecurity",
            "red-team",
            "blue-team",
            "offensive-ai",
            "defensive-ai",
            "tool-use",
            "openrae",
        ],
        "configs": [
            {
                "config_name": name,
                **({"default": True} if name == "episodes" else {}),
                "data_files": [{"split": "full", "path": f"data/{name}.parquet"}],
            }
            for name in TABLE_NAMES
        ],
    }
    frontmatter = yaml.safe_dump(front, sort_keys=False, allow_unicode=True).rstrip()
    return f"""---\n{frontmatter}\n---\n\n# {dataset['title']}\n\n{dataset['description']}\n\nThis release contains **{counts.get('episodes', 0)} paired duels** and **{counts.get('traces', 0)} role traces**. It preserves the collector recordings byte for byte under `source/episodes/`, provides trace-viewer derivatives under `viewer/`, and exposes linked Parquet tables under `data/`.\n\n## Structure\n\n- `episodes`: one row per paired duel.\n- `traces`: one row per Red or Blue trace.\n- `messages`: semantic reasoning, text, tool calls, and tool results.\n- `events`: every recorded collector-envelope event, including lifecycle and token telemetry.\n- `director_records`: contemporaneous experimental notes and interventions.\n- `artifacts`: hashes and roles for package artifacts.\n\nUse the `full` split. Curation status is a column because this observational corpus includes qualified, partial, research-only, and unassessed sessions.\n\n## Source and provenance\n\nThe agents ran inside isolated OpenRÆ cyber ranges using the public [lilrae platform]({config['sources']['platform']['url']}) and public [TechVault scenario]({config['sources']['scenario']['url']}). See `docs/PROVENANCE.md`, `docs/COLLECTION.md`, and `docs/SCHEMA.md`.\n\n## Citation and license\n\n- Version: `{dataset['version']}`\n- DOI: {f"https://doi.org/{dataset['doi']}" if dataset.get('doi') else 'pending'}\n- Data license: `{dataset.get('data_license') or 'pending'}`\n"""


def render_docs(
    config: dict[str, Any],
    inventory: dict[str, Any],
    schemas: dict[str, pa.Schema],
    counts: dict[str, int],
) -> dict[str, str]:
    dataset = config["dataset"]
    platform = config["sources"]["platform"]
    scenario = config["sources"]["scenario"]
    platform_evidence = ", ".join(f"`{path}`" for path in platform.get("evidence", [])) or "not recorded"
    scenario_evidence = ", ".join(f"`{path}`" for path in scenario.get("evidence", [])) or "not recorded"
    collection = f"""# Collection method\n\nThe corpus was collected on 2026-10-17 from paired Red and Blue coding-agent sessions running concurrently in isolated OpenRÆ TechVault cyber ranges. Each role wrote newline-delimited collector envelopes with receipt timestamps around Claude Code stream-JSON events. Human direction, prompt variants, qualification decisions, and replay-window decisions were recorded separately where available.\n\nThe source archive contained {inventory['episode_count']} distinct episode identifiers and {inventory['episode_file_copy_count']} episode-directory copies. All distinct episodes are included. No qualification label controls inclusion.\n\nThe exact source files remain under `source/`. The Parquet and Session Trace Simple Format files are deterministic derived views.\n"""
    provenance = f"""# Provenance\n\n- Platform: [{platform['name']}]({platform['url']})\n- Event-time platform revision: `{platform.get('event_revision') or 'not recovered'}`\n- Platform evidence: {platform_evidence}\n- Scenario: [{scenario['name']}]({scenario['url']})\n- Event-time scenario revision: `{scenario.get('event_revision') or 'not recovered'}`\n- Event-time scenario pack version: `{scenario.get('pack_version') or 'not recovered'}`\n- Event-time scenario set digest: `{scenario.get('set_digest') or 'not recovered'}`\n- Scenario evidence: {scenario_evidence}\n- Harness: {config['sources']['harness']['name']} {config['sources']['harness']['observed_version']}\n- Dataset version: {dataset['version']}\n\nThe scenario artifact digest identifies the deployed content directly even though its corresponding Git revision was not recorded. Evidence paths point into the byte-preserved source layer. `source/collection/canonical-inventory.json` records deterministic selection among duplicate snapshots. `manifest.json` and `SHA256SUMS` bind the released bytes.\n"""
    labels = """# Curation labels\n\nCuration fields reproduce contemporaneous event-operation records. They are observations about replay suitability and director judgment, not ground-truth security outcome labels.\n\nPrecedence for the convenience `curation_status` field is: replay-bank entry, episode qualification record, research manifest, candidate assessment, then `unassessed`. Original records remain embedded as JSON columns and copied into `source/`.\n\nQualified prefixes do not alter raw traces. Full source traces remain available; any selected replay prefix is an additional contemporaneous artifact.\n"""
    datasheet = f"""# Datasheet\n\n## Motivation\n\nThis corpus records paired Red and Blue coding agents operating concurrently in isolated OpenRÆ cyber ranges. The interactions support trace analysis, reproducibility work, teaching, and study of agent attack, defense, tool use, and human direction.\n\n## Composition\n\nThe release contains {counts['episodes']} paired episodes, {counts['traces']} role traces, {counts['messages']:,} semantic message/tool records, {counts['events']:,} raw collector events, and {counts['director_records']} director records. Every distinct observed episode is included. Qualification, research, partial-window, and unassessed states are retained as filterable labels.\n\nEach episode has byte-preserved Red and Blue source traces. Six duplicate episode snapshots were found in the source archive; `source/collection/canonical-inventory.json` records every copy, its hashes, the deterministic canonical choice, and whether alternates are byte-equal or prefixes of that choice.\n\n## Collection process\n\nThe traces were collected on 2026-10-17 during a live public demonstration using concurrent coding-agent sessions in isolated instances of the public OpenRÆ TechVault scenario. The harness wrote newline-delimited collector envelopes around Claude Code stream-JSON events. Receipt timestamps, prompts, tool calls and results, session metadata, and available human direction records were retained. See `COLLECTION.md` and `PROVENANCE.md` for the event-time platform revision and scenario artifact digest.\n\n## Processing\n\nThe source layer is copied byte for byte. Deterministic processing selects duplicate snapshots, derives typed Parquet tables, and converts semantic messages to Hugging Face Session Trace Simple Format for viewing. `manifest.json` and `SHA256SUMS` cover the released files. No outcome labels or train/test splits are assigned.\n\n## Uses and limits\n\nThe corpus supports observational analysis of paired agent behavior, tool-use sequences, attack/defense dynamics, SOC workflow, director interventions, replay/display systems, and educational examples. It is one scenario, one collection event, and one observed harness version; prompt variants and live direction differ between some episodes. Curation labels describe display or research suitability and are not security ground truth. Task-specific outcome measures require validation against the traces and range evidence.\n\n## Distribution and maintenance\n\n- Version: `{dataset['version']}`\n- Data license: `{dataset.get('data_license') or 'pending'}`\n- DOI: `{dataset.get('doi') or 'pending'}`\n- Hugging Face: `{dataset.get('huggingface_url') or 'pending'}`\n- Zenodo: `{dataset.get('zenodo_url') or 'pending'}`\n- Builder and issue tracker: `{dataset.get('repository_url') or 'pending'}`\n\nChanges to released data files receive a new dataset version and Zenodo record version. Metadata-only corrections may update the existing record where the hosting service permits it.\n"""
    sections = ["# Table schemas", ""]
    for name in TABLE_NAMES:
        sections += [f"## `{name}`", "", "| Field | Arrow type | Nullable |", "|---|---|---|"]
        for field in schemas[name]:
            sections.append(f"| `{field.name}` | `{field.type}` | {'yes' if field.nullable else 'no'} |")
        sections.append("")
    return {
        "COLLECTION.md": collection,
        "DATASHEET.md": datasheet,
        "PROVENANCE.md": provenance,
        "LABELS.md": labels,
        "SCHEMA.md": "\n".join(sections),
    }


def render_citation(config: dict[str, Any]) -> str:
    dataset = config["dataset"]
    lines = [
        "cff-version: 1.2.0",
        'message: "If you use this dataset, please cite the versioned Zenodo record."',
        "type: dataset",
        f"title: {json.dumps(dataset['title'], ensure_ascii=False)}",
        f"version: {json.dumps(dataset['version'])}",
        f"license: {json.dumps(dataset['data_license'])}",
        "authors:",
    ]
    for creator in dataset["creators"]:
        if creator.get("family_names"):
            lines.append(f"  - family-names: {json.dumps(creator['family_names'], ensure_ascii=False)}")
            lines.append(f"    given-names: {json.dumps(creator.get('given_names', ''), ensure_ascii=False)}")
            if creator.get("orcid"):
                lines.append(f"    orcid: {json.dumps(creator['orcid'])}")
        else:
            lines.append(f"  - name: {json.dumps(creator['name'], ensure_ascii=False)}")
    if dataset.get("release_date"):
        lines.append(f"date-released: {dataset['release_date']}")
    if dataset.get("doi"):
        lines.append(f"doi: {json.dumps(dataset['doi'])}")
    if dataset.get("repository_url"):
        lines.append(f"url: {json.dumps(dataset['repository_url'])}")
    return "\n".join(lines) + "\n"


def render_croissant(config: dict[str, Any], files: dict[str, dict[str, Any]]) -> dict[str, Any]:
    dataset = config["dataset"]
    license_value = dataset["data_license"]
    license_url = license_value if str(license_value).startswith("http") else f"https://spdx.org/licenses/{license_value}.html"
    creators = []
    for creator in dataset["creators"]:
        if creator.get("family_names"):
            name = " ".join(filter(None, [creator.get("given_names"), creator.get("family_names")]))
            item = {"@type": "Person", "name": name}
        else:
            item = {"@type": "Organization", "name": creator["name"]}
        if creator.get("orcid"):
            item["sameAs"] = creator["orcid"]
        creators.append(item)
    distributions = []
    for name in ("episodes", "traces", "messages", "events", "director_records", "artifacts"):
        info = files[f"data/{name}.parquet"]
        distributions.append({
            "@type": "cr:FileObject",
            "@id": f"{name}-parquet",
            "name": f"{name}.parquet",
            "contentUrl": f"data/{name}.parquet",
            "encodingFormat": "application/vnd.apache.parquet",
            "sha256": info["sha256"],
        })
    return {
        "@context": {
            "@language": "en",
            "@vocab": "https://schema.org/",
            "arrayShape": "cr:arrayShape",
            "citeAs": "cr:citeAs",
            "column": "cr:column",
            "conformsTo": "dct:conformsTo",
            "containedIn": "cr:containedIn",
            "cr": "http://mlcommons.org/croissant/",
            "rai": "http://mlcommons.org/croissant/RAI/",
            "data": {"@id": "cr:data", "@type": "@json"},
            "dataType": {"@id": "cr:dataType", "@type": "@vocab"},
            "dct": "http://purl.org/dc/terms/",
            "description": {"@container": "@language"},
            "equivalentProperty": "cr:equivalentProperty",
            "examples": {"@id": "cr:examples", "@type": "@json"},
            "extract": "cr:extract",
            "field": "cr:field",
            "fileProperty": "cr:fileProperty",
            "fileObject": "cr:fileObject",
            "fileSet": "cr:fileSet",
            "format": "cr:format",
            "includes": "cr:includes",
            "isArray": "cr:isArray",
            "isLiveDataset": "cr:isLiveDataset",
            "jsonPath": "cr:jsonPath",
            "key": "cr:key",
            "md5": "cr:md5",
            "name": {"@container": "@language"},
            "parentField": "cr:parentField",
            "path": "cr:path",
            "recordSet": "cr:recordSet",
            "references": "cr:references",
            "regex": "cr:regex",
            "repeated": "cr:repeated",
            "replace": "cr:replace",
            "samplingRate": "cr:samplingRate",
            "sc": "https://schema.org/",
            "separator": "cr:separator",
            "source": "cr:source",
            "subField": "cr:subField",
            "transform": "cr:transform",
        },
        "@type": "Dataset",
        "name": dataset["id"],
        "alternateName": dataset["title"],
        "description": dataset["description"],
        "license": license_url,
        "creator": creators,
        "publisher": {"@type": "Organization", "name": dataset["publisher"]},
        "datePublished": dataset["release_date"],
        "inLanguage": dataset.get("language", []),
        "keywords": dataset.get("keywords", []),
        "version": dataset["version"],
        "url": dataset["huggingface_url"],
        "sameAs": [dataset["zenodo_url"], dataset["repository_url"]],
        "citeAs": f"https://doi.org/{dataset['doi']}",
        "conformsTo": "http://mlcommons.org/croissant/1.1",
        "distribution": distributions,
    }


def write_zenodo_metadata(config: dict[str, Any], path: Path) -> None:
    dataset = config["dataset"]
    creators = []
    for creator in dataset.get("creators", []):
        if creator.get("family_names"):
            entry = {"name": ", ".join(filter(None, [creator["family_names"], creator.get("given_names")]))}
        else:
            entry = {"name": creator["name"]}
        if creator.get("orcid"):
            entry["orcid"] = creator["orcid"].rsplit("/", 1)[-1]
        if creator.get("affiliation"):
            entry["affiliation"] = creator["affiliation"]
        creators.append(entry)
    value = {
        "metadata": {
            "upload_type": "dataset",
            "title": dataset["title"],
            "publication_date": dataset["release_date"],
            "creators": creators,
            "description": dataset["description"],
            "access_right": "open",
            "license": str(dataset["data_license"]).lower(),
            "keywords": dataset.get("keywords", []),
            "related_identifiers": [
                {"scheme": "url", "identifier": dataset["huggingface_url"], "relation": "isIdenticalTo"},
                {"scheme": "url", "identifier": dataset["repository_url"], "relation": "isSupplementedBy"},
            ],
        }
    }
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def validate_release_metadata(config: dict[str, Any], release: bool) -> list[str]:
    dataset = config["dataset"]
    required = {
        "dataset.release_date": dataset.get("release_date"),
        "dataset.data_license": dataset.get("data_license"),
        "dataset.doi": dataset.get("doi"),
        "dataset.creators": dataset.get("creators"),
        "dataset.publisher": dataset.get("publisher"),
        "dataset.repository_url": dataset.get("repository_url"),
        "dataset.huggingface_url": dataset.get("huggingface_url"),
        "dataset.zenodo_url": dataset.get("zenodo_url"),
        "sources.platform.event_revision": config["sources"]["platform"].get("event_revision"),
    }
    missing = [name for name, value in required.items() if not value]
    scenario = config["sources"]["scenario"]
    if not (scenario.get("event_revision") or scenario.get("set_digest")):
        missing.append("sources.scenario.event_revision or sources.scenario.set_digest")
    if release and missing:
        raise ValueError("release metadata incomplete: " + ", ".join(missing))
    if release and "draft" in str(dataset.get("version", "")).lower():
        raise ValueError("release version must not contain 'draft'")
    return missing


def build(
    config_path: Path,
    output: Path,
    release: bool = False,
    archive_root_override: Path | None = None,
) -> dict[str, Any]:
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    missing_release_metadata = validate_release_metadata(config, release)
    archive_value = archive_root_override or config["sources"].get("archive_root")
    if not archive_value:
        raise ValueError("archive root is required; pass --archive-root or set sources.archive_root")
    archive_root = Path(archive_value).resolve()
    if not archive_root.is_dir():
        raise ValueError(f"archive root does not exist: {archive_root}")

    if output.exists():
        shutil.rmtree(output)
    hf_root = output / "huggingface"
    data_dir = hf_root / "data"
    source_dir = hf_root / "source"
    viewer_dir = hf_root / "viewer"
    docs_dir = hf_root / "docs"
    schema_dir = hf_root / "schema"
    for directory in (data_dir, source_dir / "episodes", source_dir / "collection", viewer_dir, docs_dir, schema_dir):
        directory.mkdir(parents=True, exist_ok=True)

    canonical, alternates = discover_episodes(archive_root)
    bank, research = load_collection_labels(archive_root)
    schemas = table_schemas()
    compression = config["release"].get("parquet_compression", "zstd")
    row_group_size = int(config["release"].get("parquet_row_group_size", 8192))

    inventory_entries = []
    episode_file_copy_count = 0
    for item in canonical:
        episode_id = item["episode_id"]
        copies = alternates[episode_id]
        episode_file_copy_count += len(copies)
        copy_inventory = []
        for copy in copies:
            relations = {}
            for role in ROLES:
                selected_trace = item["directory"] / role / "trace.jsonl"
                candidate_trace = copy["directory"] / role / "trace.jsonl"
                if selected_trace.is_file() and candidate_trace.is_file():
                    relations[role] = file_relation(selected_trace, candidate_trace)
            copy_inventory.append({
                "archive_path": copy["archive_relative_path"],
                "selected": copy["archive_relative_path"] == item["archive_relative_path"],
                "complete_roles": copy["complete_roles"],
                "raw_bytes": copy["raw_bytes"],
                "raw": copy["raw"],
                "relation_to_selected": relations,
            })
        inventory_entries.append({
            "episode_id": episode_id,
            "canonical_archive_path": item["archive_relative_path"],
            "selection_rule": "paired traces; greatest combined raw bytes; shallowest path; lexical path",
            "copies": copy_inventory,
        })
        copy_episode_source(item["directory"], source_dir / "episodes" / episode_id)

    for name in SOURCE_GLOBAL_FILES:
        source = archive_root / name
        if source.exists():
            shutil.copy2(source, source_dir / "collection" / name)

    inventory = {
        "schema": "openrae-duels-canonical-inventory/v1",
        "episode_count": len(canonical),
        "episode_file_copy_count": episode_file_copy_count,
        "duplicate_episode_id_count": sum(len(values) > 1 for values in alternates.values()),
        "episodes": inventory_entries,
    }
    (source_dir / "collection" / "canonical-inventory.json").write_text(
        json.dumps(inventory, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    event_writer = BatchWriter(data_dir / "events.parquet", schemas["events"], compression, row_group_size)
    message_writer = BatchWriter(data_dir / "messages.parquet", schemas["messages"], compression, row_group_size)
    trace_rows: list[dict[str, Any]] = []
    episode_rows: list[dict[str, Any]] = []
    trace_stats_by_episode: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for item in canonical:
        episode_id = item["episode_id"]
        package_episode_dir = source_dir / "episodes" / episode_id
        for role in ROLES:
            trace_path = item["directory"] / role / "trace.jsonl"
            trace_sha = sha256_file(trace_path)
            package_trace_path = f"source/episodes/{episode_id}/{role}/trace.jsonl"
            prompt_path = item["directory"] / role / "prompt.md"
            metadata_path = item["directory"] / role / "metadata.json"
            metadata = optional_json(metadata_path) or {}
            viewer_path = viewer_dir / episode_id / f"{role}.jsonl"
            viewer_path.parent.mkdir(parents=True, exist_ok=True)
            prompt_text = prompt_path.read_text(encoding="utf-8") if prompt_path.exists() else None

            stats = {
                "lines": 0, "messages": 0, "tool_calls": 0, "tool_results": 0,
                "assistant_text": 0, "reasoning": 0, "rounds": 0,
                "first_ts": None, "last_ts": None, "model": None,
            }
            message_sequence = 0
            header = {
                "type": "session",
                "harness": "openrae-duels",
                "id": f"{episode_id}-{role}",
                "name": f"{episode_id} — {role.title()}",
                "episode_id": episode_id,
                "side": role,
                "source_sha256": trace_sha,
                "source_format": "bsides-duel-trace/v1 collector envelope",
                "derived_format": "session-trace-simple/v1",
            }
            with viewer_path.open("w", encoding="utf-8", newline="\n") as sts_handle:
                sts_handle.write(json_compact(header) + "\n")
                if prompt_text is not None:
                    prompt_message = {
                        "type": "message",
                        "message": {"role": "user", "content": prompt_text},
                    }
                    prompt_ts = iso_to_ms(metadata.get("started_at"))
                    if prompt_ts is not None:
                        prompt_message["message"]["timestamp"] = prompt_ts
                    sts_handle.write(json_compact(prompt_message) + "\n")

                with trace_path.open("r", encoding="utf-8") as trace_handle:
                    for line_no, line in enumerate(trace_handle, 1):
                        raw_line = line.rstrip("\n")
                        outer = json.loads(raw_line)
                        timestamp = outer.get("ts")
                        stats["lines"] += 1
                        if timestamp:
                            stats["first_ts"] = stats["first_ts"] or timestamp
                            stats["last_ts"] = timestamp
                        if outer.get("kind") == "round_start":
                            stats["rounds"] += 1
                        event = outer.get("event") if isinstance(outer.get("event"), dict) else {}
                        message = event.get("message") if isinstance(event.get("message"), dict) else {}
                        model = message.get("model") or event.get("model")
                        stats["model"] = stats["model"] or model
                        event_writer.add({
                            "episode_id": episode_id,
                            "role": role,
                            "sequence": line_no,
                            "timestamp": timestamp,
                            "kind": str(outer.get("kind") or "unknown"),
                            "round": outer.get("round"),
                            "stream": outer.get("stream"),
                            "event_type": event.get("type"),
                            "event_subtype": event.get("subtype"),
                            "claude_session_id": event.get("session_id"),
                            "model": model,
                            "source_path": package_trace_path,
                            "source_line": line_no,
                            "source_sha256": trace_sha,
                            "raw_json": raw_line,
                        })
                        rows, sts_rows, message_sequence = message_rows(
                            episode_id, role, event, timestamp, package_trace_path,
                            line_no, trace_sha, message_sequence,
                        )
                        for row in rows:
                            stats["messages"] += 1
                            if row["content_type"] == "tool_use": stats["tool_calls"] += 1
                            if row["content_type"] == "tool_result": stats["tool_results"] += 1
                            if row["content_type"] == "text" and row["message_role"] == "assistant": stats["assistant_text"] += 1
                            if row["content_type"] == "thinking": stats["reasoning"] += 1
                            message_writer.add(row)
                        for sts_row in sts_rows:
                            sts_handle.write(json_compact(sts_row) + "\n")

            duration = None
            if stats["first_ts"] and stats["last_ts"]:
                first_ms, last_ms = iso_to_ms(stats["first_ts"]), iso_to_ms(stats["last_ts"])
                if first_ms is not None and last_ms is not None:
                    duration = (last_ms - first_ms) / 1000
            trace_row = {
                "episode_id": episode_id,
                "role": role,
                "trace_path": package_trace_path,
                "trace_sha256": trace_sha,
                "trace_bytes": trace_path.stat().st_size,
                "trace_lines": stats["lines"],
                "started_at": stats["first_ts"] or metadata.get("started_at"),
                "ended_at": stats["last_ts"],
                "duration_seconds": duration,
                "model": stats["model"],
                "claude_version": metadata.get("claude_version"),
                "prompt_path": f"source/episodes/{episode_id}/{role}/prompt.md" if prompt_path.exists() else None,
                "prompt_sha256": sha256_file(prompt_path) if prompt_path.exists() else metadata.get("prompt_sha256"),
                "round_count": stats["rounds"],
                "tool_call_count": stats["tool_calls"],
                "tool_result_count": stats["tool_results"],
                "assistant_text_count": stats["assistant_text"],
                "reasoning_block_count": stats["reasoning"],
                "raw_metadata_json": json_compact(metadata) if metadata else None,
            }
            trace_rows.append(trace_row)
            trace_stats_by_episode[episode_id].append({**trace_row, "message_count": stats["messages"]})

    event_count = event_writer.close()
    message_count = message_writer.close()

    for item in canonical:
        episode_id = item["episode_id"]
        episode = item["episode"]
        roles = trace_stats_by_episode[episode_id]
        starts = [row["started_at"] for row in roles if row["started_at"]]
        ends = [row["ended_at"] for row in roles if row["ended_at"]]
        start = min(starts) if starts else episode.get("started_at")
        end = max(ends) if ends else None
        duration = None
        if start and end:
            first_ms, last_ms = iso_to_ms(start), iso_to_ms(end)
            if first_ms is not None and last_ms is not None:
                duration = (last_ms - first_ms) / 1000
        cleanup = episode.get("pre_bout_telemetry_cleanup") or {}
        curation = curation_fields(episode_id, item["directory"], bank, research)
        prompt_by_role = {row["role"]: row["prompt_sha256"] for row in roles}
        episode_rows.append({
            "episode_id": episode_id,
            "range_id": episode.get("range_id"),
            "started_at": start,
            "ended_at": end,
            "duration_seconds": duration,
            "container_count": episode.get("container_count"),
            "telemetry_cleanup_deleted": cleanup.get("deleted") if isinstance(cleanup.get("deleted"), int) else None,
            "telemetry_cleanup_remaining": cleanup.get("remaining") if isinstance(cleanup.get("remaining"), int) else None,
            "telemetry_cleanup_json": json_compact(cleanup) if cleanup else None,
            **curation,
            "canonical_archive_path": item["archive_relative_path"],
            "alternate_copy_count": len(alternates[episode_id]) - 1,
            "raw_bytes": sum(row["trace_bytes"] for row in roles),
            "raw_event_count": sum(row["trace_lines"] for row in roles),
            "message_count": sum(row["message_count"] for row in roles),
            "tool_call_count": sum(row["tool_call_count"] for row in roles),
            "tool_result_count": sum(row["tool_result_count"] for row in roles),
            "red_prompt_sha256": prompt_by_role.get("red"),
            "blue_prompt_sha256": prompt_by_role.get("blue"),
            "raw_episode_json": json_compact(episode),
        })

    counts = {
        "episodes": write_small_table(data_dir / "episodes.parquet", episode_rows, schemas["episodes"], compression, row_group_size),
        "traces": write_small_table(data_dir / "traces.parquet", trace_rows, schemas["traces"], compression, row_group_size),
        "messages": message_count,
        "events": event_count,
    }

    director_rows: list[dict[str, Any]] = []
    director_path = archive_root / "director-experiments.jsonl"
    if director_path.exists():
        with director_path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, 1):
                raw_line = line.rstrip("\n")
                value = json.loads(raw_line)
                director_rows.append({
                    "sequence": len(director_rows) + 1,
                    "timestamp": value.get("ts") or value.get("recorded_at"),
                    "episode_id": value.get("episode_id"),
                    "range_id": value.get("range_id"),
                    "role": value.get("role"),
                    "kind": value.get("kind") or "director_note",
                    "variant": value.get("variant"),
                    "status": value.get("status"),
                    "outcome": value.get("outcome"),
                    "reason": value.get("reason"),
                    "guidance": value.get("guidance") or value.get("direction") or value.get("instruction"),
                    "notes": value.get("notes") or value.get("lesson") or value.get("change"),
                    "visibility": value.get("visibility"),
                    "source_path": "source/collection/director-experiments.jsonl",
                    "source_line": line_no,
                    "raw_json": raw_line,
                })
    counts["director_records"] = write_small_table(
        data_dir / "director_records.parquet", director_rows, schemas["director_records"], compression, row_group_size
    )

    for name, schema in schemas.items():
        value = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": f"https://openrae.dev/datasets/duels/schema/{name}.schema.json",
            "title": f"OpenRÆ Duels {name} row",
            "type": "object",
            "properties": {
                field.name: {"description": f"Arrow type: {field.type}"}
                for field in schema
            },
            "required": [field.name for field in schema if not field.nullable],
            "additionalProperties": False,
        }
        (schema_dir / f"{name}.schema.json").write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")

    docs = render_docs(config, inventory, schemas, counts)
    for name, content in docs.items():
        (docs_dir / name).write_text(content, encoding="utf-8")

    (hf_root / "README.md").write_text(render_dataset_card(config, counts), encoding="utf-8")
    (hf_root / "LICENSE-CODE").write_text(
        "MIT License\n\nCopyright (c) 2026 OpenRÆ contributors\n\nPermission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the \"Software\"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:\n\nThe above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.\n\nTHE SOFTWARE IS PROVIDED \"AS IS\", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.\n",
        encoding="utf-8",
    )
    if config["dataset"].get("creators") and config["dataset"].get("data_license"):
        (hf_root / "CITATION.cff").write_text(render_citation(config), encoding="utf-8")
    else:
        (hf_root / "CITATION.cff.pending").write_text("Citation metadata pending.\n", encoding="utf-8")
    if config["dataset"].get("data_license"):
        data_license = str(config["dataset"]["data_license"])
        if data_license.upper() == "CC-BY-4.0":
            license_text = (
                "Creative Commons Attribution 4.0 International (CC BY 4.0)\n"
                "https://creativecommons.org/licenses/by/4.0/\n"
            )
        else:
            license_text = f"Dataset license: {data_license}\n"
        (hf_root / "LICENSE-DATA").write_text(
            license_text, encoding="utf-8"
        )
    else:
        (hf_root / "LICENSE-DATA.pending").write_text("Data license pending.\n", encoding="utf-8")

    # Inventory all source, viewer, docs, schemas, and completed data tables except artifacts itself.
    artifact_rows: list[dict[str, Any]] = []
    for path in sorted(hf_root.rglob("*")):
        if not path.is_file() or path.name in {"artifacts.parquet", "manifest.json", "SHA256SUMS", "croissant.json"}:
            continue
        relative = path.relative_to(hf_root).as_posix()
        parts = relative.split("/")
        episode_id = parts[2] if len(parts) >= 3 and parts[:2] == ["source", "episodes"] else None
        role = parts[3] if episode_id and len(parts) >= 4 and parts[3] in ROLES else None
        if relative.startswith("source/"): artifact_class = "source"
        elif relative.startswith("viewer/"): artifact_class = "viewer"
        elif relative.startswith("data/"): artifact_class = "derived_table"
        else: artifact_class = "documentation"
        artifact_rows.append({
            "path": relative,
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
            "media_type": mimetypes.guess_type(path.name)[0] or "application/octet-stream",
            "artifact_class": artifact_class,
            "episode_id": episode_id,
            "role": role,
        })
    counts["artifacts"] = write_small_table(
        data_dir / "artifacts.parquet", artifact_rows, schemas["artifacts"], compression, row_group_size
    )

    pre_manifest_files: dict[str, dict[str, Any]] = {}
    for path in sorted(hf_root.rglob("*")):
        if path.is_file() and path.name not in {"manifest.json", "SHA256SUMS"}:
            relative = path.relative_to(hf_root).as_posix()
            pre_manifest_files[relative] = {"bytes": path.stat().st_size, "sha256": sha256_file(path)}

    if release:
        croissant = render_croissant(config, pre_manifest_files)
        (hf_root / "croissant.json").write_text(json.dumps(croissant, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        write_zenodo_metadata(config, hf_root / "zenodo-metadata.json")

    manifest_files: dict[str, dict[str, Any]] = {}
    for path in sorted(hf_root.rglob("*")):
        if path.is_file() and path.name not in {"manifest.json", "SHA256SUMS"}:
            relative = path.relative_to(hf_root).as_posix()
            manifest_files[relative] = {"bytes": path.stat().st_size, "sha256": sha256_file(path)}
    manifest = {
        "schema": "openrae-duels-release-manifest/v1",
        "dataset_id": config["dataset"]["id"],
        "version": config["dataset"]["version"],
        "doi": config["dataset"].get("doi"),
        "draft": not release,
        "counts": counts,
        "source_inventory": {
            "episode_count": inventory["episode_count"],
            "episode_file_copy_count": inventory["episode_file_copy_count"],
            "duplicate_episode_id_count": inventory["duplicate_episode_id_count"],
        },
        "files": manifest_files,
    }
    (hf_root / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    checksum_lines = []
    for path in sorted(hf_root.rglob("*")):
        if path.is_file() and path.name != "SHA256SUMS":
            checksum_lines.append(f"{sha256_file(path)}  {path.relative_to(hf_root).as_posix()}")
    (hf_root / "SHA256SUMS").write_text("\n".join(checksum_lines) + "\n", encoding="utf-8")

    version_slug = str(config["dataset"]["version"]).replace("/", "-")
    archive_name = f"{config['dataset']['id']}-v{version_slug}.zip"
    zenodo_dir = output / "zenodo"
    github_dir = output / "github-release"
    zenodo_dir.mkdir(parents=True, exist_ok=True)
    github_dir.mkdir(parents=True, exist_ok=True)
    zip_path = zenodo_dir / archive_name
    archive_prefix = f"{config['dataset']['id']}-v{version_slug}"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as archive:
        for path in sorted(hf_root.rglob("*")):
            if path.is_file():
                archive.write(path, f"{archive_prefix}/{path.relative_to(hf_root).as_posix()}")
    zip_sha = sha256_file(zip_path)
    checksum_path = zenodo_dir / f"{archive_name}.sha256"
    checksum_path.write_text(f"{zip_sha}  {archive_name}\n", encoding="utf-8")
    shutil.copy2(hf_root / "README.md", zenodo_dir / "README.md")
    shutil.copy2(hf_root / "manifest.json", zenodo_dir / "manifest.json")
    os.link(zip_path, github_dir / archive_name)
    shutil.copy2(checksum_path, github_dir / checksum_path.name)

    report = {
        "dataset_id": config["dataset"]["id"],
        "version": config["dataset"]["version"],
        "draft": not release,
        "missing_release_metadata": missing_release_metadata,
        "counts": counts,
        "huggingface_directory": str(hf_root),
        "zenodo_archive": str(zip_path),
        "github_release_asset": str(github_dir / archive_name),
        "archive_bytes": zip_path.stat().st_size,
        "archive_sha256": zip_sha,
    }
    (output / "release-report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, default=Path("dataset.yaml"))
    parser.add_argument("--archive-root", type=Path, help="path to the immutable source archive")
    parser.add_argument("--output", type=Path, default=Path("dist/build"))
    parser.add_argument("--release", action="store_true", help="require complete publication metadata")
    args = parser.parse_args()
    report = build(
        args.config.resolve(),
        args.output.resolve(),
        args.release,
        args.archive_root.resolve() if args.archive_root else None,
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
