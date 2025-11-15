# PRU-DB — Precomputed Relational Universe Database

PRU-DB is a small, segment-backed engine for storing precomputed relationships. It keeps
resolver segments fast while providing a friendlier façade for atoms and facts. The core
ideas are:

- Append-only resolver segments with precomputed postings and filters for O(1)-style
  lookups.
- Simple, ergonomic APIs for adding entities, predicates, literals, and facts.
- A CLI that can initialize a data directory, append resolver segments, and now manage
  atoms/facts directly.

## Building

```bash
cargo build -p pru_cli
```

## Quickstart

```bash
# Initialize a data directory
pru_cli init --dir ./data

# Add atoms
pru_cli entity add --dir ./data --name "Earth"
pru_cli entity add --dir ./data --name "Moon"
pru_cli predicate add --dir ./data --name "orbits"
pru_cli literal add --dir ./data --value "Luna"

# Add a fact (Moon orbits Earth)
pru_cli fact add --dir ./data --subject-id 2 --predicate-id 3 --object-id 1

# List facts for a subject
pru_cli fact list --dir ./data --subject-id 2
```

Existing resolver commands continue to work:

- `pru_cli add-resolver` to append resolver segments.
- `pru_cli resolve` for lookup, with modes (union/dedup/intersect).
- `pru_cli verify`, `pru_cli compact`, `pru_cli promote`, and `pru_cli info` for
  maintenance and inspection.
