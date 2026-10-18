# OpenRÆ Duels

OpenRÆ Duels is a corpus of paired Red and Blue coding-agent traces collected
during live, supervised cyber-range duels at the BSidesNYC 2026 AI Security
Village. The agents operated concurrently in isolated instances of the public
[OpenRÆ lilrae](https://github.com/OpenRAE/lilrae) platform and
[TechVault](https://github.com/OpenRAE/env-packs/tree/main/packs/techvault)
scenario.

Version 1.0.0 contains 40 paired episodes, 80 role traces, 29,699 semantic
message and tool records, 536,771 raw collector events, 46 director records,
and 551 indexed artifacts. The source traces are preserved byte for byte. The
release also provides linked Parquet tables and Session Trace Simple Format
views for analysis and inspection.

## Data access

Versioned archives and their SHA-256 checksums are distributed through
[GitHub Releases](https://github.com/OpenRAE/duels/releases). The same archive
is prepared for preservation on Zenodo, while unpacked tables and trace views
are prepared for the [Hugging Face dataset](https://huggingface.co/datasets/OpenRAE/duels).

Each archive contains:

- exact collector recordings under `source/episodes/`;
- typed Parquet tables under `data/`;
- trace-viewer files under `viewer/`;
- provenance, collection, schema, label, and datasheet documentation;
- a file manifest and SHA-256 checksums.

The `episodes` table carries curation and variant fields. Qualification is a
filterable observation and does not control inclusion in the corpus.

## Reproducible build

The repository tracks the builder, metadata source, schemas, and publication
runbook. Generated data stays out of Git history.

```bash
uv sync
uv run openrae-duels-build \
  --config dataset.yaml \
  --archive-root /path/to/archive \
  --output dist/build
uv run openrae-duels-validate dist/build
```

See [docs/PUBLICATION.md](docs/PUBLICATION.md) for the package layout, release
gates, and publication sequence.

## License and citation

The dataset is licensed under [CC BY 4.0](LICENSE-DATA). The builder and its
documentation are licensed under the [MIT License](LICENSE). Citation metadata
is available in [CITATION.cff](CITATION.cff).
