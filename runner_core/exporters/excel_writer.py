from datetime import datetime
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd

from runner_core.console import ANSI_YELLOW, colorize
from runner_core.io_utils import ensure_dir, sanitize_filename


def save_excel(output_root: Path, job: dict, raw_df: Any, pivot_df: Any, sheet_name: str) -> Path:
    today = datetime.today().strftime("%Y%m%d")

    subdir = job.get("output_subdir", "")
    out_dir = output_root / subdir if subdir else output_root
    ensure_dir(out_dir)

    prefix = job.get("output_prefix", "export")
    prefix = sanitize_filename(prefix)

    def normalize_sheet_name(name: str) -> str:
        n = str(name)
        for ch in "[]:*?/\\":  # Excel sheet forbidden characters
            n = n.replace(ch, "_")
        n = n.strip() or "RAW"
        return n[:31]

    def write_df(writer: pd.ExcelWriter, df: pd.DataFrame, target_sheet: str) -> None:
        use_index = isinstance(df.columns, pd.MultiIndex)
        df.to_excel(writer, sheet_name=normalize_sheet_name(target_sheet), index=use_index, merge_cells=True)

    def candidate_paths() -> List[Path]:
        base = out_dir / f"{prefix}_{today}.xlsx"
        paths = [base]
        for i in range(1, 100):
            paths.append(out_dir / f"{prefix}_{today}_{i:02d}.xlsx")
        return paths

    last_error: Optional[Exception] = None
    for out_path in candidate_paths():
        try:
            with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
                if isinstance(raw_df, dict):
                    wrote = False
                    for k, v in raw_df.items():
                        if isinstance(v, pd.DataFrame):
                            write_df(writer, v, k)
                            wrote = True
                    if not wrote and pivot_df is None:
                        pd.DataFrame().to_excel(writer, sheet_name="RAW", index=False)
                elif isinstance(raw_df, pd.DataFrame):
                    write_df(writer, raw_df, "RAW")
                else:
                    pd.DataFrame().to_excel(writer, sheet_name="RAW", index=False)

                if isinstance(pivot_df, dict):
                    for k, v in pivot_df.items():
                        if isinstance(v, pd.DataFrame):
                            write_df(writer, v, k)
                elif isinstance(pivot_df, pd.DataFrame):
                    write_df(writer, pivot_df, sheet_name[:31])
            if last_error is not None:
                print(colorize(f"[WARN] 기존 파일이 열려 있어 다른 이름으로 저장했습니다: {out_path.name}", ANSI_YELLOW))
            return out_path
        except PermissionError as e:
            last_error = e
            print(colorize(f"[WARN] 파일이 열려 있습니다. 해당 파일을 닫아주세요: {out_path}", ANSI_YELLOW))
            continue

    raise last_error if last_error else RuntimeError("Failed to save Excel output")
