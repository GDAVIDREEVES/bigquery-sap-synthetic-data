"""CLI entry: `python -m acdoca_generator.dash_app.app --data flows.json`"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, List, Optional

from acdoca_generator.dash_app.callbacks import register_callbacks
from acdoca_generator.dash_app.layout import build_layout


def load_records(path: Optional[str]) -> List[dict[str, Any]]:
    if not path:
        return []
    p = Path(path).expanduser().resolve()
    if not p.is_file():
        return []
    suf = p.suffix.lower()
    if suf == ".parquet":
        try:
            import pandas as pd
        except ImportError as e:
            raise SystemExit("pandas is required to read Parquet. pip install pandas pyarrow") from e
        return pd.read_parquet(p).to_dict(orient="records")
    if suf == ".json":
        with p.open(encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return data
        if isinstance(data, dict) and "records" in data:
            return list(data["records"])
        raise SystemExit("JSON must be a list of records or {\"records\": [...]}")
    raise SystemExit(f"Unsupported file type: {suf} (use .json or .parquet)")


def run_dash_app(app: Any, *, host: str, port: int, debug: bool) -> None:
    """Dash 3 uses ``app.run()``; Dash 2 uses ``app.run_server()``."""
    run = getattr(app, "run", None)
    if callable(run):
        run(debug=debug, host=host, port=port)
        return
    run_server = getattr(app, "run_server", None)
    if callable(run_server):
        run_server(debug=debug, host=host, port=port)
        return
    raise RuntimeError("Dash app has neither .run nor .run_server — check dash install.")


def create_dash_app(data_path: Optional[str] = None) -> Any:
    try:
        from dash import Dash
    except ImportError as e:
        raise SystemExit(
            "Dash dependencies missing. Install with: pip install 'acdoca-generator[viz]' "
            "or pip install -r requirements-viz.txt"
        ) from e

    records = load_records(data_path)
    app = Dash(__name__, suppress_callback_exceptions=True)
    app.title = "Financial supply chain"
    app.layout = build_layout(records)
    register_callbacks(app)
    return app


def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Supply chain network viewer (Dash + Cytoscape)")
    p.add_argument("--data", type=str, default=None, help="Path to supply chain flows JSON or Parquet")
    p.add_argument("--port", type=int, default=8050)
    p.add_argument("--host", type=str, default="127.0.0.1")
    p.add_argument("--debug", action="store_true")
    args = p.parse_args(argv)

    app = create_dash_app(args.data)
    url_host = "127.0.0.1" if args.host in ("0.0.0.0", "::") else args.host
    print(f"Starting server — open http://{url_host}:{args.port}/ (leave this terminal open).")
    if args.host == "0.0.0.0":
        print(f"  From this machine you can also use http://127.0.0.1:{args.port}/")
    try:
        run_dash_app(app, host=args.host, port=args.port, debug=args.debug)
    except OSError as e:
        err = str(e).lower()
        if "address already in use" in err or "address in use" in err:
            raise SystemExit(
                f"Port {args.port} is in use. Try: python -m acdoca_generator.dash_app.app --data <file> --port 8051"
            ) from e
        raise
    return 0


if __name__ == "__main__":
    sys.exit(main())
