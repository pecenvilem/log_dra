import re
from pathlib import Path
from time import perf_counter

import pandas as pd
import numpy as np

from log_dra.config.paths import MERGED_FOLDER, PROCESSED_FOLDER
from log_dra.constants.column_names import *
from log_dra.constants.modes import *
from log_dra.config.parameters import *
from log_dra.config.value_replacement import replacement

import logging
from logging import getLogger

logger = getLogger(__name__)


def configure_logger() -> None:
    logger.setLevel(logging.DEBUG)
    # noinspection SpellCheckingInspection
    formatter = logging.Formatter('%(levelname)s\t%(asctime)s (%(name)s): %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def load_file(file_name: Path) -> pd.DataFrame:
    """
    Load dat from .xlsx or .parquet file. Convert data types.
    Returns loaded data as pandas.DataFrame.
    """
    if file_name.suffix == ".xlsx":
        data = pd.read_excel(file_name, engine="openpyxl", header=1)
    elif file_name.suffix == ".parquet":
        data = pd.read_parquet(file_name)
    else:
        raise ValueError(f"File type '{file_name.suffix}' can't be loaded.")
    data = data.astype({
        TRAIN_NUMBER: pd.Int64Dtype(), LENGTH: pd.Int64Dtype(),
        MAX_SPEED: pd.Int64Dtype(), AXLE_LOAD: pd.Int64Dtype(),
    })
    return data


def parse_events(events_raw: pd.Series) -> pd.DataFrame:
    """
    Parse given series of string event descriptions using regular expression.
    Extracted fields are: RBC_ID, RBC_NAME, MODE, OBU (OBU ID), CON (ESTABLISHED CONNECTION), DISC (DISCONNECTION)
    """
    regex = re.compile(
        r"RBC *(?P<rbc_id>\d+) *(?P<name>.*) *- *(?P<obu>\d*) *:.*"
        r"(?:[mM][oóOÓ][dD] (?P<mode>\w{2})|(?P<conn>vznik vlaku)|(?P<disc>zánik vlaku))")
    renaming_rules = {"rbc_id": RBC_ID, "name": RBC_NAME, "mode": MODE,
                      "obu": OBU, "conn": CONN, "disc": DISC}
    result = events_raw.str.extract(regex)
    result = result.rename(renaming_rules, axis="columns")
    result[[CONN, DISC]] = ~result[[CONN, DISC]].isna()
    result = result.astype({RBC_ID: pd.Int64Dtype()})
    logger.debug(f"Parsed events on {len(events_raw)} rows.")
    empty_values = result.isna().apply("sum", axis="index")
    logger.debug(
        f"Missing values - RBC_ID: {empty_values[RBC_ID]}, RBC_NAME: {empty_values[RBC_NAME]}, "
        f"OBU: {empty_values[OBU]},  MODE: {empty_values[MODE]}, CONN: {empty_values[CONN]}, DISC: {empty_values[DISC]}"
    )
    return result


def find_previous_non_empty(series: pd.Series, start_from_label: pd.Index) -> pd.Index:
    """
    DEPRECATED

    Get index label of the nearest previous non-empty value from given series starting at 'start_from_label'
    """
    return series.iloc[0:series.index.get_loc(start_from_label)].last_valid_index()


def find_next_non_empty(series: pd.Series, start_from_label: pd.Index) -> pd.Index:
    """
    DEPRECATED

    Returns scalar!
    Get index label of the nearest next non-empty value from given series starting at 'start_from_label'
    """
    return series.iloc[series.index.get_loc(start_from_label) + 1:].first_valid_index()


def get_values_on_indices(data: pd.DataFrame, indices: pd.Index) -> pd.DataFrame:
    """
    DEPRECATED

    Get values from given 'data' DataFrame from columns MODE, TIME, RBC_ID and TRAIN_NUMBER for each row in 'indices'.
    """
    result = data.loc[data.index.intersection(indices), [MODE, TIME, RBC_ID, TRAIN_NUMBER]].reindex(indices)
    return result


def find_adjacent_modes(data: pd.DataFrame) -> pd.DataFrame:
    """
    DEPRECATED

    !!! Meaning of created columns is different from the new implementation (find_adjacent_modes2) !!!

    For each line in 'data' (pandas.DataFrame) get information about modes in the following way:
        LAST_REPORTED_MODE: fill empty values based on the nearest PREVIOUS existing value
        MODE_TRANSITION: True for all rows - due to compatibility with new implementation
        NEXT_INDEX: Index of the next mode transition
        PREV_INDEX: Index of the previous mode transition
        PREV_MODE, PREV_TIME, PREV_NUMBER, PREV_RBC - data from corresponding transition at PREV_INDEX
        NEXT_MODE, NEXT_TIME, NEXT_NUMBER, NEXT_RBC - data from corresponding transition at NEXT_INDEX
    """
    result_columns = [
        PREV_INDEX, PREV_MODE, PREV_TIME, PREV_NUMBER, PREV_RBC,
        NEXT_INDEX, NEXT_MODE, NEXT_TIME, NEXT_NUMBER, NEXT_RBC,
    ]
    result = pd.DataFrame(columns=result_columns)
    for obu_number, unsorted_subframe in data.groupby(by=OBU, dropna=False):
        subframe = unsorted_subframe.sort_values(by=TIME, ascending=True)

        # for each row find index of the closest following non-empty value in MODE column
        prev_mode_indices = subframe.apply(
            lambda row: find_previous_non_empty(subframe[MODE], row.name), axis="columns", result_type="expand").astype(
            pd.Int64Dtype()
        )

        # for each row find index of the closest previous non-empty value in MODE column
        next_mode_indices = subframe.apply(
            lambda row: find_next_non_empty(subframe[MODE], row.name), axis="columns", result_type="expand").astype(
            pd.Int64Dtype()
        )

        # get MODE, TIME, TRAIN_NUMBER and RBC for previous mode index
        prev_values = get_values_on_indices(subframe, prev_mode_indices)

        # get MODE, TIME, TRAIN_NUMBER and RBC for next mode index
        next_values = get_values_on_indices(subframe, next_mode_indices)

        prev_values[PREV_INDEX] = prev_values.index
        prev_values.index = subframe.index
        prev_values.rename({MODE: PREV_MODE, TIME: PREV_TIME, TRAIN_NUMBER: PREV_NUMBER, RBC_ID: PREV_RBC},
                           inplace=True, axis="columns")

        next_values[NEXT_INDEX] = next_values.index
        next_values.index = subframe.index
        next_values.rename({MODE: NEXT_MODE, TIME: NEXT_TIME, TRAIN_NUMBER: NEXT_NUMBER, RBC_ID: NEXT_RBC},
                           inplace=True, axis="columns")

        # merge value for previous and next mode
        result_subframe = prev_values.join(next_values)

        # add all calculated values for current vehicle to the final data
        result = pd.concat([result, result_subframe])

    # added for compatibility with the new implementation
    result[MODE_TRANSITION] = True
    data[LAST_REPORTED_MODE] = data.groupby(by=OBU)[MODE].ffill()

    return result


# noinspection SpellCheckingInspection
def find_adjacent_modes_2(data: pd.DataFrame) -> pd.DataFrame:
    """
    For each line in 'data' (pandas.DataFrame) get information about modes in the following way:
        LAST_REPORTED_MODE: fill empty values based on the nearest PREVIOUS existing value
        NEXT_REPORTED_MODE: fill empty values based on the nearest NEXT existing value
        MODE_TRANSITION: True if currently reported mode is different from the previous one
        NEXT_INDEX: Index of the next mode transition
        PREV_INDEX: Index of the previous mode transition
        PREV_MODE, PREV_TIME, PREV_NUMBER, PREV_RBC - data from corresponding transition at PREV_INDEX
        NEXT_MODE, NEXT_TIME, NEXT_NUMBER, NEXT_RBC - data from corresponding transition at NEXT_INDEX
    """
    result_columns = [
        LAST_REPORTED_MODE, NEXT_REPORTED_MODE, MODE_TRANSITION,
        PREV_INDEX, PREV_MODE, PREV_TIME, PREV_NUMBER, PREV_RBC,
        NEXT_INDEX, NEXT_MODE, NEXT_TIME, NEXT_NUMBER, NEXT_RBC,
    ]
    local_data = data.copy().sort_values(by=TIME)[[OBU, TIME, MODE, TRAIN_NUMBER, RBC_ID]]
    local_data["idx"] = local_data.index

    #  0,  1,  2,  3,  4,  5,  6,  7,  8,  9 >>  0,  1,  2,  3,  4,  5,  6,  7,  8,  9  < index
    # - , FS, - , FS, SR, - , SR, - , SB, -  >> - , FS, FS, FS, SR, SR, SR, SR, SB, SB  < MODE >> LAST_REPORTED_MODE
    #                                           ^^                          ^^      ^^  < diff to NEXT_REPORTED_MODE
    local_data[LAST_REPORTED_MODE] = local_data.groupby(by=OBU)[MODE].ffill()

    #  0,  1,  2,  3,  4,  5,  6,  7,  8,  9 >>  0,  1,  2,  3,  4,  5,  6,  7,  8,  9  < index
    # - , FS, - , FS, SR, - , SR, - , SB, -  >> FS, FS, FS, FS, SR, SR, SR, SB, SB, -   < MODE >> NEXT_REPORTED_MODE
    #                                           ^^                          ^^      ^^  < diff to LAST_REPORTED_MODE
    local_data[NEXT_REPORTED_MODE] = local_data.groupby(by=OBU)[MODE].bfill()

    #  0,  1,  2,  3,  4,  5,  6,  7,  8,  9    < index
    # - , FS, - , FS, SR, - , SR, - , SB, -     < MODE
    # - , FS, FS, FS, SR, SR, SR, SR, SB, SB    < LAST_REPORTED_MODE
    # - , - , FS, FS, FS, SR, SR, SR, SR, SB    < mode_shifted_forwards
    mode_shifted_forwards = local_data.groupby(by=OBU)[LAST_REPORTED_MODE].shift(+1)

    #  0,  1,  2,  3,  4,  5,  6,  7,  8,  9    < index
    # - , FS, - , FS, SR, - , SR, - , SB, -     < MODE
    # - , FS, FS, FS, SR, SR, SR, SR, SB, SB    < LAST_REPORTED_MODE
    # FS, FS, FS, SR, SR, SR, SR, SB, SB, -     < mode_shifted_backwards
    mode_shifted_backwards = local_data.groupby(by=OBU)[LAST_REPORTED_MODE].shift(-1)

    #  0,  1,  2,  3,  4,  5,  6,  7,  8,  9    < index
    # - , FS, - , FS, SR, - , SR, - , SB, -     < MODE
    # - , FS, FS, FS, SR, SR, SR, SR, SB, SB    < LAST_REPORTED_MODE
    # - , - , FS, FS, FS, SR, SR, SR, SR, SB    < mode_shifted_forwards
    #  1   1           1               1        < different_from_prev
    different_from_prev = local_data[LAST_REPORTED_MODE] != mode_shifted_forwards

    #  0,  1,  2,  3,  4,  5,  6,  7,  8,  9    < index
    # - , FS, - , FS, SR, - , SR, - , SB, -     < MODE
    # - , FS, FS, FS, SR, SR, SR, SR, SB, SB    < LAST_REPORTED_MODE
    # FS, FS, FS, SR, SR, SR, SR, SB, SB, -     < mode_shifted_backwards
    #  1           1               1       1    < different_from_next
    different_from_next = local_data[LAST_REPORTED_MODE] != mode_shifted_backwards

    #  0,  1,  2,  3,  4,  5,  6,  7,  8,  9    < index
    # - , FS, - , FS, SR, - , SR, - , SB, -     < MODE
    #  1   1           1               1        < different_from_prev
    #  0,  1,  4,  8    < local_data[different_from_prev]["idx"]
    #  1,  4,  8, -     < next_transition_index
    next_transition_index = local_data[different_from_prev].groupby(by=OBU)["idx"].shift(-1)

    #  0,  1,  2,  3,  4,  5,  6,  7,  8,  9    < index
    # - , FS, - , FS, SR, - , SR, - , SB, -     < MODE
    #  1           1               1       1    < different_from_next
    #  0,  3,  7,  9    < local_data[different_from_next]["idx"]
    # - ,  0,  3,  7    < prev_transition_index
    prev_transition_index = local_data[different_from_next].groupby(by=OBU)["idx"].shift(+1)

    #  0,  1,  2,  3,  4,  5,  6,  7,  8,  9    < index
    # - , FS, - , FS, SR, - , SR, - , SB, -     < MODE
    #  1   1           1               1        < MODE_TRANSITION
    local_data[MODE_TRANSITION] = False
    local_data.loc[different_from_prev, MODE_TRANSITION] = True

    #  0,  1,  2,  3,  4,  5,  6,  7,  8,  9    < index
    # - , FS, - , FS, SR, - , SR, - , SB, -     < MODE
    #  0,  1,  4,  8    < next_transition_index.index
    #  1,  4,  8, -     < next_transition_index
    #  1,  4, - , - ,  8, - , - , - , - , -     < NEXT_INDEX
    local_data.loc[next_transition_index.index, NEXT_INDEX] = next_transition_index.to_numpy()

    #  0,  1,  2,  3,  4,  5,  6,  7,  8,  9    < index
    # - , FS, - , FS, SR, - , SR, - , SB, -     < MODE
    #  0,  3,  7,  9    < prev_transition_index.index
    # - ,  0,  3,  7    < prev_transition_index
    # - , - , - ,  0, - , - , - ,  3, - ,  7    < PREV_INDEX
    local_data.loc[prev_transition_index.index, PREV_INDEX] = prev_transition_index.to_numpy()

    #  0,  1,  2,  3,  4,  5,  6,  7,  8,  9    < index
    # - , FS, - , FS, SR, - , SR, - , SB, -     < MODE
    #  1,  4, - , - ,  8, - , - , - , - , -     < NEXT_INDEX
    #  1,  4,  4,  4,  8,  8,  8,  8,  8,  8    < NEXT_INDEX = NEXT_INDEX.ffill
    local_data[NEXT_INDEX] = local_data.groupby(by=OBU)[NEXT_INDEX].ffill()

    #  0,  1,  2,  3,  4,  5,  6,  7,  8,  9    < index
    # - , FS, - , FS, SR, - , SR, - , SB, -     < MODE
    # - , - , - ,  0, - , - , - ,  3, - ,  7    < PREV_INDEX
    #  0,  0,  0,  0,  3,  3,  3,  3,  7,  7    < PREV_INDEX = PREV_INDEX.bfill
    local_data[PREV_INDEX] = local_data.groupby(by=OBU)[PREV_INDEX].bfill()

    # drop rows for which adjacent modes were not found, because they have not reported more than 1 different mode
    # int the whole dataset
    local_data = local_data.dropna(axis="index", how="any", subset=[NEXT_INDEX, PREV_INDEX])

    # copy values from rows referenced by PREV_INDEX and NEXT_INDEX into corresponding columns
    local_data[
        [NEXT_MODE, NEXT_TIME, NEXT_NUMBER, NEXT_RBC]
    ] = local_data.loc[local_data[NEXT_INDEX], [LAST_REPORTED_MODE, TIME, TRAIN_NUMBER, RBC_ID]].to_numpy()
    local_data[
        [PREV_MODE, PREV_TIME, PREV_NUMBER, PREV_RBC]
    ] = local_data.loc[local_data[PREV_INDEX], [LAST_REPORTED_MODE, TIME, TRAIN_NUMBER, RBC_ID]].to_numpy()

    return local_data[result_columns]


# noinspection PyUnresolvedReferences
def compare_adjacent_modes(data: pd.DataFrame) -> pd.DataFrame:
    """
    Precompute differences between current mode and the following and previous one.
    Also precompute time differences to transitions to these modes.
    """
    return pd.DataFrame({
        PREV_RBC_CHANGE: (data[PREV_RBC] != data[RBC_ID]).fillna(True),
        NEXT_RBC_CHANGE: (data[RBC_ID] != data[NEXT_RBC]).fillna(True),
        PREV_NUMBER_CHANGE: (data[PREV_NUMBER] != data[TRAIN_NUMBER]).fillna(True),
        NEXT_NUMBER_CHANGE: (data[TRAIN_NUMBER] != data[NEXT_NUMBER]).fillna(True),
        PREV_TIME_DIFFERENCE: data[TIME] - data[PREV_TIME],
        NEXT_TIME_DIFFERENCE: data[NEXT_TIME] - data[TIME]
    })


def is_illegal_sr(data: pd.DataFrame, time_limit: int = T_NVOVTRP, rel_eps: float = 0.1) -> pd.Series:
    # noinspection SpellCheckingInspection
    """
        Test if rows should be qualified as unauthorised use of the Override procedure.
        Returns boolean pandas.Series

        True if mode change is detected, entered mode is SR, previous mode was not SR, the next mode is OS and the
        transition to OS took place after T_NVOVTRP seconds after the first transition to SR (tolerance is applied).
        """
    if rel_eps < 0:
        raise ValueError(f"Relative tolerance cannot be negative! {rel_eps} given")
    suspicious_transition = (data[MODE] == SR) & (data[PREV_MODE].isin([FS, OS])) & (data[NEXT_MODE].isin([OS]))
    lower_bound = time_limit * (1 - rel_eps / 2)
    upper_bound = time_limit * (1 + rel_eps / 2)
    return suspicious_transition & (
        (data[NEXT_TIME_DIFFERENCE] / np.timedelta64(1, "s")).between(lower_bound, upper_bound))


def list_train_numbers(data: pd.Series) -> pd.DataFrame:
    """
    Return pandas.DataFrame with a single column of unique values from the passed pandas.Series.
    The passed series is filtered, dropping data outside range 1 to 899 999.
    The column is named accordingly.
    """
    valid_number_filter = (data.between(1, 899999)) & (~data.isna())
    return pd.DataFrame(sorted(data[valid_number_filter].unique()), columns=[TRAIN_NUMBER])


def list_engines(data: pd.Series) -> pd.DataFrame:
    """
    Return pandas.DataFrame with a single column of unique values from the passed pandas.Series.
    The column is named accordingly.
    """
    return pd.DataFrame(sorted(data.unique()), columns=[OBU])


def is_connection_loss(data: pd.DataFrame, smart: bool = False) -> pd.Series:
    """
    Test if rows should be qualified as connection losses.
    Returns boolean pandas.Series

    True if disconnection is detected from the same RBC as the new connection will be established with,
    last reported mode is FS or OS and next reported mode is SR.
    (Connection losses where resulting transition to TR are reported are detected in separate functions.)

    If passed boolean flag 'smart' True, expect differently named columns (reflects changes to column meanings after
    rework of the function for locating mode transitions).
    """
    if smart:
        return (data[DISC]) & (data[RBC_ID] == data[NEXT_RBC]) & \
            (data[LAST_REPORTED_MODE].isin([FS, OS]) & (data[NEXT_REPORTED_MODE].isin([SR])))
    return (data[DISC]) & (data[RBC_ID] == data[NEXT_RBC]) & \
        (data[PREV_MODE].isin([FS, OS]) & (data[NEXT_MODE].isin([SR])))


# noinspection PyTypeChecker
def is_trip(data: pd.DataFrame) -> pd.Series:
    """
    Test if rows should be qualified as transitions to TR mode.
    Returns boolean pandas.Series

    True if mode change is detected, entered mode is TR and previous mode was not TR.
    """
    return (data[MODE] == TR) & (data[PREV_MODE] != TR) & data[MODE_TRANSITION]


def is_post_trip(data: pd.DataFrame) -> pd.Series:
    """
    Test if rows should be qualified as transitions to PT mode.
    Returns boolean pandas.Series

    True if mode change is detected, entered mode is PT and previous mode was not PT or TR.
    """
    return (data[MODE] == PT) & (~data[PREV_MODE].isin([TR, PT])) & data[MODE_TRANSITION]


# noinspection PyTypeChecker
def is_system_failure(data: pd.DataFrame) -> pd.Series:
    """
    Test if rows should be qualified as transitions to SF mode.
    Returns boolean pandas.Series

    True if mode change is detected, entered mode is SF and previous mode was not SF.
    """
    return (data[MODE] == SF) & (data[PREV_MODE] != SF) & data[MODE_TRANSITION]


def is_isolation(data: pd.DataFrame) -> pd.Series:
    """
    Test if rows should be qualified as transitions to IS mode.
    Returns boolean pandas.Series

    True if mode change is detected, entered mode is IS and previous mode was not IS, TR or PT.
    """
    return (data[MODE] == IS) & (~data[PREV_MODE].isin([IS, TR, PT])) & data[MODE_TRANSITION]


def analyse_data(data: pd.DataFrame, smart: bool = False) -> pd.DataFrame:
    """
    Analyse passed data. Return analysed data with computed columns.
    """
    data = data.drop(columns=[OBU, NATIVE_MODE])
    data = data.drop_duplicates()
    data = data.sort_values(by=TIME)
    data = data.reset_index()
    data = data.join(parse_events(data[EVENT]))
    data = data.dropna(axis="index", how="any", subset=[OBU, RBC_ID])
    data = data.drop(columns=[EVENT])
    if smart:
        data = data.join(find_adjacent_modes_2(data))
    else:
        data = data.join(find_adjacent_modes(data))
    data = data.join(compare_adjacent_modes(data))
    data[TRIP] = is_trip(data)
    data[CONNECTION_LOSS] = is_connection_loss(data, smart)
    data[SYSTEM_FAILURE] = is_system_failure(data)
    data[ILLEGAL_SR] = is_illegal_sr(data, T_NVOVTRP)
    data[ISOLATION] = is_isolation(data)
    data[INCIDENT] = data[TRIP] | is_post_trip(data) | data[ISOLATION] | data[SYSTEM_FAILURE] | data[CONNECTION_LOSS]
    data[INCIDENT_NO_IS] = (data[INCIDENT]) & (~data[ISOLATION])
    data = data.replace(replacement, regex=True)
    data[OBU] = data[OBU].astype(pd.Int64Dtype())
    return data


def analyse_file(input_file: Path, output_file: Path, force: bool = False, smart=True) -> None:

    def load_and_analyse() -> pd.DataFrame:
        """
        Load file and analyse contained data. Return analysed data with computed columns.
        """
        return analyse_data(load_file(input_file), smart)

    try:
        if force:
            logger.debug("Forced recomputing.")
            load_and_analyse().to_parquet(output_file)
            return
        if input_file.lstat().st_mtime <= output_file.lstat().st_mtime:
            logger.info(f"Skipping, no change in source file since output was generated.")
            return
    except FileNotFoundError:
        logger.debug(f"Output file {output_file} not found. Creating a new one.")
    load_and_analyse().to_parquet(output_file)


def main():
    configure_logger()
    input_file = MERGED_FOLDER / "20240104095717.parquet"
    output_file = PROCESSED_FOLDER / "20240104095717.parquet"
    t = perf_counter()
    analyse_file(input_file, output_file, force=True)
    logger.info(f"Processed file {input_file} in {perf_counter() - t:.2f}s.")


if __name__ == "__main__":
    main()
