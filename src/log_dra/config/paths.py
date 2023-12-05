from pathlib import Path

DATA_FOLDER = Path(r"c:\users\pecen\projects\log_dra\data")
# noinspection SpellCheckingInspection
STATS_FOLDER = Path(r"c:\users\pecen\SZ\OEMT\Diagnostika ETCS\data_provoz\mesicni_prehledy")

RAW_FOLDER = DATA_FOLDER / "raw"
XLSX_FOLDER = RAW_FOLDER / "xlsx"
PARQUET_FOLDER = RAW_FOLDER / "parquet"
CONVERTED_FOLDER = PARQUET_FOLDER / "converted"
MERGED_FOLDER = PARQUET_FOLDER / "merged"

PROCESSED_FOLDER = DATA_FOLDER / "processed"

LOGS_FOLDER = Path(r"c:\users\pecen\projects\log_dra\logs")
