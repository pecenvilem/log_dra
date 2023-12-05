import pandas as pd

from pathlib import Path

from log_dra.config.paths import PROCESSED_FOLDER
from log_dra.pipeline.analyse import load_file
from log_dra.constants.column_names import *

RELEVANT_COLUMNS = [
    TIME, RBC_ID, OBU, MODE, INCIDENT, CONNECTION_LOSS, ILLEGAL_SR, CONN, DISC
]


def compare_files(compare: Path, reference: Path) -> pd.DataFrame:
    """
    Compare contents of 'compare' file with the 'reference' file.
    Returns pandas.DataFrame with selected columns from both loaded DataFrames.
    Selected columns:
        TIME, RBC_ID, OBU, MODE, INCIDENT, CONNECTION_LOSS, ILLEGAL_SR, CONN, DISC
    Column indicating difference in INCIDENT column called DIFF is added.
    """
    data = load_file(compare)
    data = data.rename({"Mód": MODE}, axis="columns")
    reference = load_file(reference)

    data = data[RELEVANT_COLUMNS]
    reference = reference[RELEVANT_COLUMNS]

    data = data.sort_values(by=[TIME, OBU, RBC_ID, MODE])
    reference = reference.sort_values(by=[TIME, OBU, RBC_ID, MODE])

    data = data.reset_index(drop=True)
    reference = reference.reset_index(drop=True)

    joined = pd.merge(reference, data, on=[TIME, OBU, MODE], how="left", indicator=True, suffixes=("_ref", None))

    selector = (joined[INCIDENT] != joined[INCIDENT + "_ref"]) & (joined["_merge"] == "both")
    joined.loc[selector, "DIFF"] = True
    joined["DIFF"] = joined["DIFF"].fillna(False)

    return joined


def get_neighbours(data: pd.DataFrame, label: int, radius: int = 5) -> pd.DataFrame:
    """
    Select 'radius' number of rows around a row given by 'label' from a subset of rows only taking into account rows
    with the same OBU as the row given by 'label'.
    Returns pandas DataFrame created by this slice.
    """
    obu = data.loc[label, OBU]
    group = data.groupby(by=OBU).get_group(obu)
    idx = group.index.get_loc(label)
    return group.iloc[idx - radius: idx + radius]


def main() -> None:
    differences = compare_files(
        PROCESSED_FOLDER / "legacy" / "202311.parquet",
        PROCESSED_FOLDER / "20240117082435.parquet"
    )

    for label in differences[differences["DIFF"]].iloc[0:5].index:
        neighbours = get_neighbours(differences, label)
        pass


if __name__ == "__main__":
    main()
