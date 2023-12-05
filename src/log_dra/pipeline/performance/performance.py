import pandas as pd
from log_dra.config.paths import PROCESSED_FOLDER, STATS_FOLDER, DATA_FOLDER

from log_dra.pipeline.performance import sheets
from log_dra.constants.column_names import *
from log_dra.constants.modes import *
from log_dra.pipeline.performance.readme import README


DATE_FOLDER = STATS_FOLDER / "2024" / "01"
DATE_FOLDER.mkdir(parents=True, exist_ok=True)

data = pd.read_parquet(PROCESSED_FOLDER / "20240116090132.parquet")
data[RBC] = data[RBC_NAME] + " (" + data[RBC_ID].astype(str) + ")"

# per-engine statistics
obu = pd.read_excel(DATA_FOLDER / "rolling_stock.xlsx", index_col="UIC").drop(["MSISDN", "EVN", "Datum vydání"], axis="columns") \
    .drop_duplicates()
data = data.merge(obu.reset_index(), how="left", left_on="OBU ETCS ID", right_on="OBU ID") \
    .drop(["OBU ID"], axis="columns")
data = data.astype({"UIC": pd.Int64Dtype()})

unicov_train_number_intervals = [
    [13701, 13730],
    [3621, 3690],
    [12400, 12560]
]

unicov_train_numbers = [
    1438,
    1439,
    81730,
    81731
]

for lower_bound, upper_bound in unicov_train_number_intervals:
    unicov_train_numbers.extend([train_number for train_number in range(lower_bound, upper_bound + 1)])

unicov_train_numbers = pd.Series(unicov_train_numbers)

invalid_train_number_filter = data[TRAIN_NUMBER].isin([100663296, 0, 1])
# test_drive_filter = data[OBU].isin([]) | data[TRAIN_NUMBER].isin([])
test_drive_filter = data[OBU].isin([13001, 1022, 1023]) | data[TRAIN_NUMBER].isin([94101])
incident_filter = data[INCIDENT_NO_IS] & (~test_drive_filter) & (~invalid_train_number_filter)
isolation_filter = data[ISOLATION] & (~test_drive_filter) & (~invalid_train_number_filter)
unicov_filter = (data[TRAIN_NUMBER].isin(unicov_train_numbers)) | (data[RBC_ID] == 101)

incidents = data[incident_filter]
isolations = data[isolation_filter]
unicov_incidents = data[incident_filter & unicov_filter]
unicov_isolations = data[isolation_filter & unicov_filter]


def filter_contents(category: str) -> pd.DataFrame:
    sheets_to_display = [(name, description) for name, description, display_in in sheets.SHEETS if
                         category in display_in]
    names = [name for name, description in sheets_to_display]
    descriptions = [description for name, description in sheets_to_display]
    return pd.DataFrame({"Název listu": names, "Popis dat": descriptions})


with pd.ExcelWriter(str(DATE_FOLDER / "readme.xlsx"), mode="w") as writer:
    pd.Series(README).to_excel(writer, sheet_name="readme", index=False, header=False)

with pd.ExcelWriter(str(DATE_FOLDER / "trackside.xlsx"), mode="w", datetime_format="YYYY-MM-DD") as writer:
    filter_contents(sheets.TRACKSIDE).to_excel(writer, sheet_name="contents", index=False)

    # daily trains in the whole network
    d = data[data[MODE] == FS].groupby([pd.Grouper(freq='D', key=TIME)])[TRAIN_NUMBER].nunique().astype(pd.Int64Dtype())
    d.name = "Počet vlaků"
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name=sheets.TRAINS_DAY_TOTAL)

    # daily trains per RBC
    d = data[data[MODE] == FS].groupby([pd.Grouper(freq='D', key=TIME), RBC])[TRAIN_NUMBER].nunique().unstack() \
        .astype(pd.Int64Dtype())
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name=sheets.TRAINS_DAY_RBC)

    # daily trains at the Olomouc - Unicov line (filtered by train numbers)
    d = data[unicov_filter & (data[MODE] == FS)].groupby([pd.Grouper(freq='D', key=TIME)])[TRAIN_NUMBER].nunique() \
        .astype(pd.Int64Dtype())
    d.name = "Počet vlaků"
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name=sheets.TRAINS_DAY_311C)

    # monthly trains in the whole network
    d = data[data[MODE] == FS].groupby([pd.Grouper(freq='D', key=TIME)])[TRAIN_NUMBER].nunique().resample("MS").sum() \
        .astype(pd.Int64Dtype())
    d.name = "Počet vlaků"
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name=sheets.TRAINS_MONTH_TOTAL)

    # monthly trains per RBC
    d = data[data[MODE] == FS].groupby(
        [pd.Grouper(freq='D', key=TIME), RBC]
    )[TRAIN_NUMBER].nunique().groupby(
        [pd.Grouper(freq='MS', level=TIME), RBC]
    ).sum().unstack().astype(pd.Int64Dtype())
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name=sheets.TRAINS_MONTH_RBC)

    # monthly trains at the Olomouc - Unicov line (filtered by train numbers)
    d = data[unicov_filter & (data[MODE] == FS)].groupby([pd.Grouper(freq='D', key=TIME)])[TRAIN_NUMBER].nunique() \
        .resample("MS").sum().astype(pd.Int64Dtype())
    d.name = "Počet vlaků"
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name=sheets.TRAINS_MONTH_311C)

    # daily connections in the whole network
    d = data.groupby([pd.Grouper(freq='D', key=TIME)])[TRAIN_NUMBER].nunique().astype(pd.Int64Dtype())
    d.name = "Počet vlaků"
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name=sheets.CONNECTIONS_DAY_TOTAL)

    # daily connections per RBC
    d = data.groupby([pd.Grouper(freq='D', key=TIME), RBC])[TRAIN_NUMBER].nunique().unstack().astype(pd.Int64Dtype())
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name=sheets.CONNECTIONS_DAY_RBC)

    # daily connections at the Olomouc - Unicov line (filtered by train numbers)
    d = data[unicov_filter].groupby([pd.Grouper(freq='D', key=TIME)])[TRAIN_NUMBER].nunique().astype(pd.Int64Dtype())
    d.name = "Počet vlaků"
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name=sheets.CONNECTIONS_DAY_311C)

    # monthly connections in the whole network
    d = data.groupby([pd.Grouper(freq='D', key=TIME)])[TRAIN_NUMBER].nunique().resample("MS").sum() \
        .astype(pd.Int64Dtype())
    d.name = "Počet vlaků"
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name=sheets.CONNECTIONS_MONTH_TOTAL)

    # monthly connections per RBC
    d = data.groupby(
        [pd.Grouper(freq='D', key=TIME), RBC]
    )[TRAIN_NUMBER].nunique().groupby(
        [pd.Grouper(freq='MS', level=TIME), RBC]
    ).sum().unstack().astype(pd.Int64Dtype())
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name=sheets.CONNECTIONS_MONTH_RBC)

    # monthly connections at the Olomouc - Unicov line (filtered by train numbers)
    d = data[unicov_filter].groupby([pd.Grouper(freq='D', key=TIME)])[TRAIN_NUMBER].nunique().resample("MS").sum() \
        .astype(pd.Int64Dtype())
    d.name = "Počet vlaků"
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name=sheets.CONNECTIONS_MONTH_311C)

    # daily engines in the whole network
    d = data.groupby([pd.Grouper(freq='D', key=TIME)])[OBU].nunique().astype(pd.Int64Dtype())
    d.name = "Počet HV"
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name="engines_day_total")

    # daily engines per RBC
    d = data.groupby([pd.Grouper(freq='D', key=TIME), RBC])[OBU].nunique().unstack().astype(pd.Int64Dtype())
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name="engines_day_rbc")

    # daily engines at the Olomouc - Unicov line (filtered by train numbers)
    d = data[unicov_filter].groupby([pd.Grouper(freq='D', key=TIME)])[OBU].nunique().astype(pd.Int64Dtype())
    d.name = "Počet HV"
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name="engines_day_311c")

    # monthly engines in the whole network
    d = data.groupby([pd.Grouper(freq='MS', key=TIME)])[OBU].nunique().astype(pd.Int64Dtype())
    d.name = "Počet HV"
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name="engines_month_total")

    # monthly engines per RBC
    d = data.groupby(
        [pd.Grouper(freq='MS', key=TIME), RBC]
    )[OBU].nunique().unstack().astype(pd.Int64Dtype())
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name="engines_month_rbc")

    # monthly engines at the Olomouc - Unicov line (filtered by train numbers)
    d = data[unicov_filter].groupby([pd.Grouper(freq='MS', key=TIME)])[OBU].nunique().astype(pd.Int64Dtype())
    d.name = "Počet HV"
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name="engines_month_311c")

    # monthly IS in the whole network
    d = isolations.groupby(pd.Grouper(freq="MS", key=TIME)).size()
    d.name = "Počet přechodů do IS"
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name=sheets.ISOLATIONS_MONTH_TOTAL)

    # daily IS in the whole network
    d = isolations.groupby(pd.Grouper(freq="D", key=TIME)).size()
    d.name = "Počet přechodů do IS"
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name=sheets.ISOLATIONS_DAY_TOTAL)

    # monthly IS per RBC
    d = isolations.groupby([pd.Grouper(freq="MS", key=TIME), RBC]).size().unstack().astype(pd.Int64Dtype())
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name=sheets.ISOLATIONS_MONTH_RBC)

    # daily IS per RBC
    d = isolations.groupby([pd.Grouper(freq="D", key=TIME), RBC]).size().unstack().astype(pd.Int64Dtype())
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name=sheets.ISOLATIONS_DAY_RBC)

    # monthly IS at the Olomouc - Unicov line (filtered by train numbers)
    d = unicov_isolations.groupby(pd.Grouper(freq="MS", key=TIME)).size()
    d.name = "Počet přechodů do IS"
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name=sheets.ISOLATIONS_MONTH_311C)

    # daily IS at the Olomouc - Unicov line (filtered by train numbers)
    d = unicov_isolations.groupby(pd.Grouper(freq="D", key=TIME)).size()
    d.name = "Počet přechodů do IS"
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name=sheets.ISOLATIONS_DAY_311C)

    # monthly incidents in the whole network
    d = incidents.groupby(pd.Grouper(freq="MS", key=TIME)).size()
    d.name = "Počet incidentů"
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name="incidents_month_total")

    # daily incidents in the whole network
    d = incidents.groupby(pd.Grouper(freq="D", key=TIME)).size()
    d.name = "Počet incidentů"
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name="incidents_day_total")

    # monthly incidents per RBC
    d = incidents.groupby([pd.Grouper(freq="MS", key=TIME), RBC]).size().unstack().astype(pd.Int64Dtype())
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name="incidents_month_rbc")

    # daily incidents per RBC
    d = incidents.groupby([pd.Grouper(freq="D", key=TIME), RBC]).size().unstack().astype(pd.Int64Dtype())
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name="incidents_day_rbc")

    # monthly incidents at the Olomouc - Unicov line (filtered by train numbers)
    d = unicov_incidents.groupby(pd.Grouper(freq="MS", key=TIME)).size()
    d.name = "Počet incidentů"
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name="incidents_month_311c")

    # daily incidents at the Olomouc - Unicov line (filtered by train numbers)
    d = unicov_incidents.groupby(pd.Grouper(freq="D", key=TIME)).size()
    d.name = "Počet incidentů"
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name="incidents_day_311c")

    # monthly affected trains (the whole network)
    d = incidents.groupby(pd.Grouper(freq="D", key=TIME))[TRAIN_NUMBER].nunique().resample("MS").sum()
    d.name = "Počet vlaků"
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name="affected_month_total")

    # daily affected trains (the whole network)
    d = incidents.groupby(pd.Grouper(freq="D", key=TIME))[TRAIN_NUMBER].nunique()
    d.name = "Počet vlaků"
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name="affected_day_total")

    # monthly affected trains per RBC (the whole network)
    d = incidents.groupby([pd.Grouper(freq="D", key=TIME), RBC])[TRAIN_NUMBER].nunique().groupby(
        [pd.Grouper(freq='MS', level=TIME), RBC]
    ).sum().unstack().astype(pd.Int64Dtype())
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name="affected_month_rbc")

    # daily affected trains per RBC (the whole network)
    d = incidents.groupby([pd.Grouper(freq="D", key=TIME), RBC])[TRAIN_NUMBER].nunique().unstack().astype(
        pd.Int64Dtype())
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name="affected_day_rbc")

    # monthly affected trains at the Olomouc - Unicov line (filtered by train numbers)
    d = unicov_incidents.groupby(pd.Grouper(freq="D", key=TIME))[TRAIN_NUMBER].nunique().resample("MS").sum()
    d.name = "Počet vlaků"
    d.index.rename("Měsíc", inplace=True)
    d.to_excel(writer, sheet_name="affected_month_311c")

    # daily affected trains at the Olomouc - Unicov line (filtered by train numbers)
    d = unicov_incidents.groupby(pd.Grouper(freq="D", key=TIME))[TRAIN_NUMBER].nunique()
    d.name = "Počet vlaků"
    d.index.rename("Den", inplace=True)
    d.to_excel(writer, sheet_name="affected_day_311c")

with pd.ExcelWriter(str(DATE_FOLDER / "vehicles.xlsx"), mode="w", datetime_format="YYYY-MM-DD") as writer:
    filter_contents(sheets.VEHICLES).to_excel(writer, sheet_name="sheet_contents", index=False)

    # per engine statistics in the whole network - day / week / month
    for period, \
        trains_sheet_name, \
        cumulative_trains_sheet_name, \
        incidents_sheet_name, \
        cumulative_incidents_sheet_name, \
        affected_sheet_name, \
        cumulative_affected_sheet_name, \
        relative_affected_sheet_name in zip(
            ["D", "W", "MS"],
            [sheets.TRAINS_DAY_PER_ENGINE, sheets.TRAINS_WEEK_PER_ENGINE, sheets.TRAINS_MONTH_PER_ENGINE],
            [sheets.TRAINS_CUMULATIVE_DAY_PER_ENGINE, sheets.TRAINS_CUMULATIVE_WEEK_PER_ENGINE,
             sheets.TRAINS_CUMULATIVE_MONTH_PER_ENGINE],
            [sheets.INCIDENTS_DAY_PER_ENGINE, sheets.INCIDENTS_WEEK_PER_ENGINE, sheets.INCIDENTS_MONTH_PER_ENGINE],
            [sheets.INCIDENTS_CUMULATIVE_DAY_PER_ENGINE, sheets.INCIDENTS_CUMULATIVE_WEEK_PER_ENGINE,
             sheets.INCIDENTS_CUMULATIVE_MONTH_PER_ENGINE],
            [sheets.AFFECTED_DAY_PER_ENGINE, sheets.AFFECTED_WEEK_PER_ENGINE, sheets.AFFECTED_MONTH_PER_ENGINE],
            [sheets.AFFECTED_CUMULATIVE_DAY_PER_ENGINE, sheets.AFFECTED_CUMULATIVE_WEEK_PER_ENGINE,
             sheets.AFFECTED_CUMULATIVE_MONTH_PER_ENGINE],
            [sheets.RELATIVE_AFFECTED_DAY_PER_ENGINE, sheets.RELATIVE_AFFECTED_WEEK_PER_ENGINE,
             sheets.RELATIVE_AFFECTED_MONTH_PER_ENGINE],
            ):
        trains = pd.pivot_table(
            data,
            index=pd.Grouper(freq="D", key=TIME),
            columns=[data["DOPRAVCE"], data[OBU], data["UIC"]],
            aggfunc="nunique",
            values=TRAIN_NUMBER
        ).resample(period).sum().astype(pd.Int64Dtype())
        trains.columns = trains.columns.set_levels(trains.columns.levels[-1].astype(str), level=-1)

        incidents_pivot = pd.pivot_table(
            incidents,
            index=pd.Grouper(freq="D", key=TIME),
            columns=[incidents["DOPRAVCE"], incidents[OBU], incidents["UIC"]],
            aggfunc="size",
            values=[INCIDENT]
        ).resample(period).sum().astype(pd.Int64Dtype())
        incidents_pivot.columns = incidents_pivot.columns.set_levels(incidents_pivot.columns.levels[-1].astype(str), level=-1)
        incidents_pivot = incidents_pivot.reorder_levels(["DOPRAVCE", OBU, "UIC"], axis="columns")

        affected = pd.pivot_table(
            incidents,
            index=pd.Grouper(freq="D", key=TIME),
            columns=[incidents["DOPRAVCE"], incidents[OBU], incidents["UIC"]],
            aggfunc="nunique",
            values=TRAIN_NUMBER
        ).resample(period).sum().astype(pd.Int64Dtype())
        affected.columns = affected.columns.set_levels(affected.columns.levels[-1].astype(str), level=-1)

        trains, affected = trains.align(affected, join="outer", axis="columns")
        _, incidents_pivot = affected.align(incidents_pivot, join="outer", axis="columns")
        relative = affected / trains
        relative = relative.astype(float)
        s = affected.sum().sort_values(ascending=False).index

        trains[s].T.to_excel(writer, sheet_name=trains_sheet_name, merge_cells=False)
        trains[s].cumsum(axis="index").T.to_excel(writer, sheet_name=cumulative_trains_sheet_name, merge_cells=False)
        incidents_pivot[s].T.to_excel(writer, sheet_name=incidents_sheet_name, merge_cells=False)
        incidents_pivot[s].cumsum(axis="index").T.to_excel(writer, sheet_name=cumulative_incidents_sheet_name, merge_cells=False)
        affected[s].T.to_excel(writer, sheet_name=affected_sheet_name, merge_cells=False)
        affected[s].cumsum(axis="index").T.to_excel(writer, sheet_name=cumulative_affected_sheet_name, merge_cells=False)
        relative[s].T.to_excel(writer, sheet_name=relative_affected_sheet_name, merge_cells=False)
