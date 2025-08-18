from dataclasses import dataclass
from typing import Callable, Optional, Tuple, List, Dict
import pandas as pd
import re
from pathlib import Path
import os
import difflib
import json

def pivot_iontech(df: pd.DataFrame) -> pd.DataFrame:
    df = df.pivot(index='TimeString', columns='VarName', values='VarValue')
    df.rename(columns={"Analog_PC_CIT101.Analog_out": "Salt Conductivity (ms/cm)",
                         "Analog_PC_CIT102.Analog_out": "Acid Conductivity (ms/cm)",
                         "Analog_PC_CIT103.Analog_out": "Base Conductivity (ms/cm)",
                         "Analog_PC_HFK101_II.Analog_out": "PS Current (Amps)",
                         "Analog_PC_HFK101_UI.Analog_out": "PS Voltage (Volts)",
                         "Analog_PC_TI101.Analog_out": "Acid? Temperature (C)",
                         "Analog_PC_TI102.Analog_out": "Base? Temperature (C)",
                         "Analog_PC_TI103.Analog_out": "Salt? Temperature (C)",
                         "Analog_PC_TI104.Analog_out": "Electrolyte? Temperature (C)"},
                        inplace=True)
    df.drop(columns=["$RT_DIS$"], inplace=True)
    df.reset_index(names="Timestamp", inplace=True)
    return df

def _read_file(path: str | Path) -> pd.DataFrame:
    """
    Try CSV/TSV (with sep inference), then Excel. Everything as string to validate formats.
    """
    p = str(path)
    root, ext = os.path.splitext(p)
    if ext == '.txt':
        try:
            df = pd.read_csv(p, nrows=4)
            return df
        except Exception as e:
            pass
        
        try:
            df = pd.read_csv(p, delimiter="\t",encoding='UTF-16', quotechar='"', engine='python', skipfooter=1)
            return df
        except Exception as e:
            raise e
        
    if ext == '.csv':
        try:
            df = pd.read_csv(p)
            return df
        except Exception as e:
            raise e
            
# ---------- Schema definition

@dataclass
class Schema:
    name: str
    predicate: Callable[[pd.DataFrame], bool]     # Does the dataframe match this schema?
    normalizer: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None  # Optional: return standardized df

# ---------- Your three example schemas

def _schema_1_pred(df: pd.DataFrame) -> bool:
    if df.columns[0] != 'VarName': return False
    if df.columns[1] != 'TimeString': return False
    return True

def _schema_2_pred(df: pd.DataFrame) -> bool:
    date_format = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d$")
    if not date_format.match(df['Timestamp'][0]):
        return False
    return True

def _schema_1_normalize(df: pd.DataFrame) -> pd.DataFrame:
    df = pivot_iontech(df)
    df.columns.name = None
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    return df

def _schema_2_normalize(df: pd.DataFrame) -> pd.DataFrame:
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])
    return df

SCHEMA_REGISTRY: List[Schema] = [
    Schema(name="altasea iontech bl3t-25", predicate=_schema_1_pred, normalizer=_schema_1_normalize),
    Schema(name="degas system control datalog", predicate=_schema_2_pred, normalizer=_schema_2_normalize)
]

# ---------- Engine

def detect_schema_from_df(df: pd.DataFrame) -> Optional[Schema]:
    for schema in SCHEMA_REGISTRY:
        try:
            if schema.predicate(df):
                return schema
        except Exception:
            # Defensive: if a predicate has a bug, skip it
            continue
        
    return None

FILENAME_LOG = Path("filenames.txt")

def _read_logged_filenames() -> List[str]:
    if not FILENAME_LOG.exists():
        return []
    # normalize whitespace and ignore blank lines
    return [line.strip() for line in FILENAME_LOG.read_text(encoding="utf-8").splitlines() if line.strip()]

def _write_logged_filenames(files_list: List[str]) -> None:
    # Write atomically-ish
    tmp = FILENAME_LOG.with_suffix(".tmp")
    tmp.write_text("\n".join(files_list) + "\n", encoding="utf-8")
    tmp.replace(FILENAME_LOG)

def _detect_update(files_list: List[str]) -> Tuple[bool, str]:
    prev = _read_logged_filenames()
    curr = list(files_list)  # ensure list
    if prev == curr:
        return False, "No change, reading from feather..."

    # Better diff: line-by-line filenames
    diff_lines = difflib.unified_diff(prev, curr, fromfile="previous", tofile="current", lineterm="")
    return True, "\n".join(diff_lines)

def _reload_from_source(folder: str, files_list: str) -> pd.DataFrame:
    _write_logged_filenames(files_list)

    df_list = []
    for file in files_list:
        if '.DS_Store' in file: continue
        df = _read_file(folder + '/' + file)
        if df.empty: continue
        schema = detect_schema_from_df(df)
        if schema:
            if schema.normalizer:
                try:
                    df = schema.normalizer(df)
                
                except Exception as e:
                    # Fallback to raw if normalization fails
                    raise e
        else:
            print("Failed to match " + file)
            continue
        df_list.append(df)

    df = pd.concat(df_list, join='outer')
    df.sort_values(by='Timestamp', inplace=True)
    df.drop_duplicates(inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.to_feather('data.feather')
    return df

def load_files(folder: str) -> pd.DataFrame:
    files_list = os.listdir(folder)
    updated, message = _detect_update(files_list)
    print(message)
    if updated: df = _reload_from_source(folder, files_list)
    else: df = pd.read_feather('data.feather')
    #df = _reload_from_source(folder, files_list)
    return df

# ---------- How to add new schemas
# 1) Write a predicate(df) -> bool
# 2) (Optional) Write a normalizer(df) -> df
# 3) Append Schema(name, predicate, normalizer) to SCHEMA_REGISTRY