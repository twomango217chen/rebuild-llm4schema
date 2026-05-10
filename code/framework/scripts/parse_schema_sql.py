from __future__ import annotations

import argparse
import json
from pathlib import Path

from schema_tuning.collectors.metadata import _collect_schema_from_schema_sql


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse schema.sql and emit schema_state JSON")
    parser.add_argument("--schema", required=True, help="Path to schema.sql")
    args = parser.parse_args()

    schema_state = _collect_schema_from_schema_sql(Path(args.schema))
    print(json.dumps(schema_state, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
