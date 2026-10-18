from __future__ import annotations

import argparse
import hashlib
import json
import zipfile
from pathlib import Path

import pyarrow.parquet as pq

EXPECTED_TABLES = {
    "episodes": 40,
    "traces": 80,
    "messages": None,
    "events": None,
    "director_records": None,
    "artifacts": None,
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate(build_root: Path) -> dict[str, object]:
    report = json.loads((build_root / "release-report.json").read_text(encoding="utf-8"))
    hf_root = build_root / "huggingface"
    manifest = json.loads((hf_root / "manifest.json").read_text(encoding="utf-8"))
    failures: list[str] = []

    for relative, metadata in manifest["files"].items():
        path = hf_root / relative
        if not path.is_file():
            failures.append(f"missing manifest file: {relative}")
            continue
        if path.stat().st_size != metadata["bytes"]:
            failures.append(f"size mismatch: {relative}")
        if sha256_file(path) != metadata["sha256"]:
            failures.append(f"hash mismatch: {relative}")

    for line in (hf_root / "SHA256SUMS").read_text(encoding="utf-8").splitlines():
        expected, relative = line.split("  ", 1)
        path = hf_root / relative
        if not path.is_file() or sha256_file(path) != expected:
            failures.append(f"SHA256SUMS mismatch: {relative}")

    table_counts: dict[str, int] = {}
    for name, exact in EXPECTED_TABLES.items():
        path = hf_root / "data" / f"{name}.parquet"
        if not path.is_file():
            failures.append(f"missing table: {name}")
            continue
        count = pq.ParquetFile(path).metadata.num_rows
        table_counts[name] = count
        if exact is not None and count != exact:
            failures.append(f"{name} rows: expected {exact}, got {count}")
        manifest_count = manifest["counts"].get(name)
        if manifest_count != count:
            failures.append(f"{name} manifest count: expected {count}, got {manifest_count}")

    viewer_files = sorted((hf_root / "viewer").glob("*/*.jsonl"))
    if len(viewer_files) != 80:
        failures.append(f"viewer file count: expected 80, got {len(viewer_files)}")
    viewer_messages = 0
    for path in viewer_files:
        with path.open("r", encoding="utf-8") as handle:
            first = json.loads(next(handle))
            if first.get("type") != "session" or not first.get("harness") or not first.get("id"):
                failures.append(f"invalid STS header: {path.relative_to(hf_root)}")
            for line_no, line in enumerate(handle, 2):
                value = json.loads(line)
                message = value.get("message")
                if value.get("type") != "message" or not isinstance(message, dict):
                    failures.append(f"invalid STS message: {path.relative_to(hf_root)}:{line_no}")
                    continue
                if message.get("role") not in {"user", "assistant", "system", "tool"}:
                    failures.append(f"invalid STS role: {path.relative_to(hf_root)}:{line_no}")
                if "content" not in message:
                    failures.append(f"missing STS content: {path.relative_to(hf_root)}:{line_no}")
                if message.get("role") == "tool" and not message.get("toolCallId"):
                    failures.append(f"missing STS toolCallId: {path.relative_to(hf_root)}:{line_no}")
                for call in message.get("toolCalls", []):
                    function = call.get("function", {}) if isinstance(call, dict) else {}
                    if not call.get("id") or not function.get("name") or not isinstance(function.get("arguments"), str):
                        failures.append(f"invalid STS tool call: {path.relative_to(hf_root)}:{line_no}")
                viewer_messages += 1

    inventory = json.loads(
        (hf_root / "source" / "collection" / "canonical-inventory.json").read_text(encoding="utf-8")
    )
    if inventory.get("episode_count") != 40:
        failures.append(f"inventory episode count: {inventory.get('episode_count')}")
    source_traces: dict[str, dict[str, object]] = {}
    for episode in inventory["episodes"]:
        episode_id = episode["episode_id"]
        selected = [copy for copy in episode["copies"] if copy["selected"]]
        if len(selected) != 1:
            failures.append(f"canonical selection count for {episode_id}: {len(selected)}")
            continue
        if set(selected[0]["raw"]) != {"red", "blue"}:
            failures.append(f"canonical roles for {episode_id}: {sorted(selected[0]['raw'])}")
        for role, metadata in selected[0]["raw"].items():
            path = hf_root / "source" / "episodes" / episode_id / role / "trace.jsonl"
            if not path.is_file() or sha256_file(path) != metadata["sha256"]:
                failures.append(f"source trace mismatch: {episode_id}/{role}")
                continue
            line_count = 0
            with path.open("r", encoding="utf-8") as handle:
                for line_no, line in enumerate(handle, 1):
                    try:
                        json.loads(line)
                    except json.JSONDecodeError as exc:
                        failures.append(f"invalid source JSONL: {path.relative_to(hf_root)}:{line_no}: {exc}")
                    line_count = line_no
            relative = path.relative_to(hf_root).as_posix()
            source_traces[relative] = {"sha256": metadata["sha256"], "lines": line_count}

    for table_name in ("messages", "events"):
        parquet = pq.ParquetFile(hf_root / "data" / f"{table_name}.parquet")
        for batch in parquet.iter_batches(columns=["source_path", "source_line", "source_sha256"]):
            for row in batch.to_pylist():
                source = source_traces.get(row["source_path"])
                if source is None:
                    failures.append(f"{table_name} unresolved source: {row['source_path']}")
                    continue
                if row["source_sha256"] != source["sha256"]:
                    failures.append(f"{table_name} source hash mismatch: {row['source_path']}")
                if not 1 <= row["source_line"] <= source["lines"]:
                    failures.append(f"{table_name} source line out of range: {row['source_path']}:{row['source_line']}")

    public_metadata = [
        path for path in hf_root.rglob("*")
        if path.is_file() and not path.relative_to(hf_root).parts[0] in {"source", "viewer", "data"}
    ]
    absolute_markers = ("/home/", "/Users/", "file://", "C:\\Users\\")
    for path in public_metadata:
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        if any(marker in text for marker in absolute_markers):
            failures.append(f"absolute workstation path in public metadata: {path.relative_to(hf_root)}")

    if not report["draft"]:
        for required in ("CITATION.cff", "LICENSE-DATA", "croissant.json", "zenodo-metadata.json"):
            if not (hf_root / required).is_file():
                failures.append(f"missing release metadata file: {required}")

    zip_path = Path(report["zenodo_archive"])
    if not zip_path.is_file() or sha256_file(zip_path) != report["archive_sha256"]:
        failures.append("Zenodo ZIP hash mismatch")
    else:
        with zipfile.ZipFile(zip_path) as archive:
            bad = archive.testzip()
            if bad:
                failures.append(f"corrupt ZIP entry: {bad}")

    github_asset = Path(report["github_release_asset"])
    if not github_asset.is_file() or sha256_file(github_asset) != report["archive_sha256"]:
        failures.append("GitHub release asset differs from Zenodo ZIP")
    expected_checksum = f"{report['archive_sha256']}  {zip_path.name}\n"
    for checksum_path in (
        zip_path.with_suffix(zip_path.suffix + ".sha256"),
        github_asset.with_suffix(github_asset.suffix + ".sha256"),
    ):
        if not checksum_path.is_file() or checksum_path.read_text(encoding="utf-8") != expected_checksum:
            failures.append(f"archive checksum sidecar mismatch: {checksum_path}")

    result = {
        "ok": not failures,
        "failures": failures,
        "table_counts": table_counts,
        "viewer_files": len(viewer_files),
        "viewer_messages": viewer_messages,
        "archive_bytes": report["archive_bytes"],
        "archive_sha256": report["archive_sha256"],
        "draft": report["draft"],
        "missing_release_metadata": report["missing_release_metadata"],
    }
    if failures:
        raise SystemExit(json.dumps(result, indent=2))
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("build_root", type=Path)
    args = parser.parse_args()
    print(json.dumps(validate(args.build_root.resolve()), indent=2))


if __name__ == "__main__":
    main()
