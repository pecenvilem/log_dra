import pandas as pd

import sheets
from column_names import *
from modes import *
from readme import README

frames = []

# for filename in listdir("data/performance_stats"):
#     data = load_data(join("data", "performance_stats", filename))
#     data.loc[data[DLS] == "ETCS Břeclav-Česká Třebová", DLS] = "ETCS Břeclav - Česká Třebová"
#     data.loc[data[DLS] == "ETCS Přerov-Česká Třebová", DLS] = "ETCS Přerov - Česká Třebová"
#     frames.append(data)
#
# main_frame = pd.concat(frames, axis="index", ignore_index=True)
# main_frame = main_frame.drop_duplicates()
#
# data = analyse_from_data(main_frame)
# data[RBC] = data[RBC_NAME] + " (" + data[RBC_ID].astype(str) + ")"

data = pd.read_parquet("data.parquet")

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
test_drive_filter = data[OBU].isin([13001, 1022, 1023]) | data[TRAIN_NUMBER].isin([94101])
incident_filter = data[INCIDENT_NO_IS] & (~test_drive_filter) & (~invalid_train_number_filter)
isolation_filter = data[ISOLATION] & (~test_drive_filter) & (~invalid_train_number_filter)
unicov_filter = (data[TRAIN_NUMBER].isin(unicov_train_numbers)) | (data[RBC_ID] == 101)

incidents = data[incident_filter]
isolations = data[isolation_filter]
unicov_incidents = data[incident_filter & unicov_filter]
unicov_isolations = data[isolation_filter & unicov_filter]

with pd.ExcelWriter("data/data_provoz_etcs_v07.xlsx", mode="w", datetime_format="YYYY-MM-DD") as writer:
    pd.Series(README).to_excel(writer, sheet_name="readme", index=False, header=False)

    pd.DataFrame({
        "Název listu": sheets.DESCRIPTIONS.keys(),
        "Popis dat": sheets.DESCRIPTIONS.values()
    }).to_excel(writer, sheet_name="sheet_contents", index=False)

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
