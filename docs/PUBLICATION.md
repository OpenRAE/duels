# Publication architecture and runbook

## Decision

Publish the same versioned content through three complementary surfaces:

- Hugging Face is the interactive home for tabular exploration and rendered
  agent traces.
- Zenodo is the archival record and sole DOI authority.
- GitHub holds the reproducible builder, schemas, documentation, and a release
  asset matching the Zenodo ZIP byte for byte.

Do not mint a second DOI on Hugging Face. Link the Zenodo version DOI from the
Hugging Face card and `CITATION.cff`. A later change to data files becomes a new
semantic dataset version and a new Zenodo version; documentation-only changes
may update repository metadata without rebuilding source evidence.

## Canonical package

```text
openrae-duels-vX.Y.Z/
├── README.md
├── CITATION.cff
├── LICENSE-DATA
├── LICENSE-CODE
├── manifest.json
├── SHA256SUMS
├── croissant.json
├── data/
│   ├── episodes.parquet
│   ├── traces.parquet
│   ├── messages.parquet
│   ├── events.parquet
│   ├── director_records.parquet
│   └── artifacts.parquet
├── viewer/
│   └── <episode-id>/<red|blue>.jsonl
├── source/
│   ├── episodes/<episode-id>/...
│   └── collection/...
├── schema/
│   └── *.schema.json
└── docs/
    ├── COLLECTION.md
    ├── DATASHEET.md
    ├── PROVENANCE.md
    ├── SCHEMA.md
    └── LABELS.md
```

`source/` is the evidence layer. Its files are copied byte for byte from the
selected archive copy. `data/` and `viewer/` are deterministic derivatives.
Every derived record carries an episode identifier and source location; table
rows also carry the source trace digest where applicable.

The archive contains 40 distinct episode IDs and duplicate snapshots for six
of them. Canonical selection requires both role traces, then chooses the copy
with the greatest combined raw-trace length. Ties prefer the shallower archive
path and then lexical order. Every rejected copy and its hashes remain in the
inventory so the choice is auditable.

## Tables

- `episodes`: one row per paired duel; timing, range metadata, cleanup state,
  curation labels, prompt variants, source selection, and aggregate counts.
- `traces`: one row per episode and role; model/harness metadata, prompt and
  trace digests, line count, timing, and package paths.
- `messages`: semantic assistant text/reasoning, tool calls, and tool results.
- `events`: every collector-envelope line, including high-frequency token and
  lifecycle events. `raw_json` preserves the exact JSON object for analysis.
- `director_records`: contemporaneous experimental notes and interventions.
- `artifacts`: path, size, digest, media type, and source/derived role.

This is an observational corpus, so the public tables use one `full` split.
There is no invented train/test split. `qualified`, `research`, partial-window,
and unassessed states are labels that users can filter.

## Hugging Face staging

Upload an unpacked tree containing the card, Parquet tables, source files,
viewer traces, schemas, and documentation. The dataset card defines one config
per table and makes `episodes` the default. Parquet uses small row groups and a
page index so the Dataset Viewer can scan selectively. The 80 viewer files use
Hugging Face Session Trace Simple Format because the recorded files wrap Claude
Code events in a collector envelope and therefore are not native Claude Code
session files.

After a private draft upload:

1. Confirm all six configs and the `full` split appear.
2. Open several Red and Blue files under `viewer/` in the trace viewer.
3. Verify tool calls join to results and timestamps are ordered.
4. Compare source hashes against the local manifest.
5. Make the dataset public only after the Zenodo DOI is reserved and inserted.

## Zenodo staging

Zenodo recommends a ZIP for more than 20 files. Upload:

- `openrae-duels-vX.Y.Z.zip`
- `openrae-duels-vX.Y.Z.zip.sha256`
- `README.md`
- `manifest.json`

Create a **Dataset** draft, reserve the DOI, insert it into `dataset.yaml`, and
rebuild. Fill creators in citation order, ORCIDs and affiliations where
available, publication date, description, keywords, funding if applicable,
and the chosen data license. Preview and validate the draft before publishing.
Use Zenodo versioning whenever released data files change.

The generated `zenodo-metadata.json` is an update body for an existing Zenodo
deposit. Create the draft and reserve its DOI first; the reserved DOI belongs to
the deposit and is therefore not sent as an externally assigned DOI field.
Confirm the semantic version in the deposit form as well; it remains in the
package, citation, and Croissant metadata but is omitted from the API body so
that body validates against Zenodo's linked legacy deposit JSON Schema.

## GitHub staging

Keep generated tables, source traces, and ZIP files out of normal Git history.
Track the builder, lock file, schemas, docs, metadata source, and tests. Add a
root `CITATION.cff` with `type: dataset` and the Zenodo version DOI. Draft a
versioned GitHub release, attach the exact Zenodo ZIP and checksum, then publish
the release after the Zenodo record resolves. GitHub permits release assets
under 2 GiB; this collection is well below that limit.

## Release gates

The release validator must establish:

- exactly one canonical entry per episode ID and both Red and Blue traces;
- every JSONL line parses;
- source file bytes match their manifest hashes;
- viewer files satisfy the STS header/message shape;
- Parquet schemas and counts match the manifest;
- every message/event source reference resolves;
- no absolute local paths occur in public metadata;
- version, DOI, license, creator, repository, and platform/scenario provenance
  fields are complete in release mode;
- the Zenodo ZIP checksum matches the GitHub release asset and the version
  advertised on Hugging Face.

## Publication sequence

1. Create the destination GitHub repository, a private Hugging Face dataset
   repository, and an empty Zenodo Dataset draft.
2. Reserve the Zenodo DOI. Record the DOI and the three stable URLs in
   `dataset.yaml`, select the data license, add creators in citation order, and
   set the release date and version. Zenodo remains the publisher unless this
   dataset was previously published elsewhere.
3. Run the release build and validator:

   ```bash
   uv run openrae-duels-build --config dataset.yaml --archive-root /path/to/archive --output dist/build --release
   uv run openrae-duels-validate dist/build
   ```

4. Upload `dist/build/huggingface/` to the private Hugging Face dataset and
   verify all six table configs plus several `viewer/` traces.
5. Upload the four files in `dist/build/zenodo/` to the Zenodo draft, apply the
   generated metadata, preview the record, and compare its archive checksum to
   `release-report.json`.
6. Push this builder repository and create a draft GitHub release. Attach the
   ZIP and checksum from `dist/build/github-release/`.
7. Publish the Zenodo record, make the Hugging Face dataset public, and publish
   the GitHub release. Verify that all three surfaces cite the version DOI and
   advertise the same version and archive checksum.

## Official references checked

- Hugging Face dataset cards: https://huggingface.co/docs/hub/datasets-cards
- Hugging Face data-file configuration: https://huggingface.co/docs/hub/datasets-data-files-configuration
- Hugging Face agent traces: https://huggingface.co/docs/hub/agent-traces
- Hugging Face STS format: https://huggingface.co/docs/hub/session-traces-format
- Hugging Face storage guidance: https://huggingface.co/docs/hub/storage-limits
- Zenodo uploads: https://help.zenodo.org/docs/deposit/create-new-upload/
- Zenodo file packaging: https://help.zenodo.org/docs/deposit/manage-files/
- Zenodo DOI reservation: https://help.zenodo.org/docs/deposit/describe-records/reserve-doi/
- Zenodo versioning: https://help.zenodo.org/docs/deposit/manage-versions/
- Zenodo REST deposit metadata: https://developers.zenodo.org/
- GitHub release assets: https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases
- GitHub citation files: https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-citation-files
- MLCommons Croissant: https://docs.mlcommons.org/croissant/
