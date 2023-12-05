TRAINS_DAY_TOTAL = "trains_day_total"
TRAINS_DAY_RBC = "trains_day_rbc"
TRAINS_DAY_311C = "trains_day_311c"
TRAINS_MONTH_TOTAL = "trains_month_total"
TRAINS_MONTH_RBC = "trains_month_rbc"
TRAINS_MONTH_311C = "trains_month_311c"
CONNECTIONS_DAY_TOTAL = "connections_day_total"
CONNECTIONS_DAY_RBC = "connections_day_rbc"
CONNECTIONS_DAY_311C = "connections_day_311c"
CONNECTIONS_MONTH_TOTAL = "connections_month_total"
CONNECTIONS_MONTH_RBC = "connections_month_rbc"
CONNECTIONS_MONTH_311C = "connections_month_311c"
ENGINES_DAY_TOTAL = "engs_day_total"
ENGINES_DAY_RBC = "engs_day_rbc"
ENGINES_DAY_311C = "engs_day_311c"
ENGINES_MONTH_TOTAL = "engs_month_total"
ENGINES_MONTH_RBC = "engs_month_rbc"
ENGINES_MONTH_311C = "engs_month_311c"
ISOLATIONS_MONTH_TOTAL = "isolations_month_total"
ISOLATIONS_DAY_TOTAL = "isolations_day_total"
ISOLATIONS_MONTH_RBC = "isolations_month_rbc"
ISOLATIONS_DAY_RBC = "isolations_day_rbc"
ISOLATIONS_MONTH_311C = "isolations_month_311c"
ISOLATIONS_DAY_311C = "isolations_day_311c"
INCIDENTS_MONTH_TOTAL = "incidents_month_total"
INCIDENTS_DAY_TOTAL = "incidents_day_total"
INCIDENTS_MONTH_RBC = "incidents_month_rbc"
INCIDENTS_DAY_RBC = "incidents_day_rbc"
INCIDENTS_MONTH_311C = "incidents_month_311c"
INCIDENTS_DAY_311C = "incidents_day_311c"
AFFECTED_MONTH_TOTAL = "affected_month_total"
AFFECTED_DAY_TOTAL = "affected_day_total"
AFFECTED_MONTH_RBC = "affected_month_rbc"
AFFECTED_DAY_RBC = "affected_day_rbc"
AFFECTED_MONTH_311C = "affected_month_311c"
AFFECTED_DAY_311C = "affected_day_311c"
TRAINS_DAY_PER_ENGINE = "trains_day_per_eng"
TRAINS_WEEK_PER_ENGINE = "trains_week_per_eng"
TRAINS_MONTH_PER_ENGINE = "trains_month_per_eng"
TRAINS_CUMULATIVE_DAY_PER_ENGINE = "trains_cumul_day_per_eng"
TRAINS_CUMULATIVE_WEEK_PER_ENGINE = "trains_cumul_week_per_eng"
TRAINS_CUMULATIVE_MONTH_PER_ENGINE = "trains_cumul_month_per_eng"
AFFECTED_DAY_PER_ENGINE = "affected_day_per_eng"
AFFECTED_WEEK_PER_ENGINE = "affected_week_per_eng"
AFFECTED_MONTH_PER_ENGINE = "affected_month_per_eng"
AFFECTED_CUMULATIVE_DAY_PER_ENGINE = "affected_cumul_day_per_eng"
AFFECTED_CUMULATIVE_WEEK_PER_ENGINE = "affected_cumul_week_per_eng"
AFFECTED_CUMULATIVE_MONTH_PER_ENGINE = "affected_cumul_month_per_eng"
RELATIVE_AFFECTED_DAY_PER_ENGINE = "rel_affected_day_per_eng"
RELATIVE_AFFECTED_WEEK_PER_ENGINE = "rel_affected_week_per_eng"
RELATIVE_AFFECTED_MONTH_PER_ENGINE = "rel_affected_month_per_eng"
RELATIVE_AFFECTED_CUMULATIVE_DAY_PER_ENGINE = "rel_affected_cumul_day_per_eng"
RELATIVE_AFFECTED_CUMULATIVE_WEEK_PER_ENGINE = "rel_affected_cumul_week_per_eng"
RELATIVE_AFFECTED_CUMULATIVE_MONTH_PER_ENGINE = "rel_affected_cumul_month_per_eng"
INCIDENTS_DAY_PER_ENGINE = "incidents_day_per_eng"
INCIDENTS_CUMULATIVE_DAY_PER_ENGINE = "incidents_cumul_day_per_eng"
INCIDENTS_WEEK_PER_ENGINE = "incidents_week_per_eng"
INCIDENTS_CUMULATIVE_WEEK_PER_ENGINE = "incidents_cumul_week_per_eng"
INCIDENTS_MONTH_PER_ENGINE = "incidents_month_per_eng"
INCIDENTS_CUMULATIVE_MONTH_PER_ENGINE = "incidents_cumul_month_per_eng"

TRACKSIDE, VEHICLES = "TRACKSIDE", "VEHICLES"

SHEETS = [
    [
        TRAINS_DAY_TOTAL,
        "Počty vlaků jedoucích ve FS v jednotlivých dnech (celá síť)",
        [TRACKSIDE]
    ],
    [
        TRAINS_DAY_RBC,
        "Počty vlaků jedoucích ve FS v jednotlivých dnech podle RBC (celá síť)",
        [TRACKSIDE]
    ],
    [
        TRAINS_DAY_311C,
        "Počty vlaků jedoucích ve FS v jednotlivých dnech (trať Olomouc - Uničov)",
        [TRACKSIDE]
    ],
    [
        TRAINS_MONTH_TOTAL,
        "Počty vlaků jedoucích ve FS v jednotlivých měsících (celá síť)",
        [TRACKSIDE]
    ],
    [
        TRAINS_MONTH_RBC,
        "Počty vlaků jedoucích ve FS v jednotlivých měsících podle RBC (celá síť)",
        [TRACKSIDE]
    ],
    [
        TRAINS_MONTH_311C,
        "Počty vlaků jedoucích ve FS v jednotlivých měsících (trať Olomouc - Uničov)",
        [TRACKSIDE]
    ],
    [
        CONNECTIONS_DAY_TOTAL,
        "Počty připojených vlaků v jednotlivých dnech (celá síť)",
        [TRACKSIDE]
    ],
    [
        CONNECTIONS_DAY_RBC,
        "Počty připojených vlaků v jednotlivých dnech podle RBC (celá síť)",
        [TRACKSIDE]
    ],
    [
        CONNECTIONS_DAY_311C,
        "Počty připojených vlaků v jednotlivých dnech (trať Olomouc - Uničov)",
        [TRACKSIDE]
    ],
    [
        CONNECTIONS_MONTH_TOTAL,
        "Počty připojených vlaků v jednotlivých měsících (celá síť)",
        [TRACKSIDE]
    ],
    [
        CONNECTIONS_MONTH_RBC,
        "Počty připojených vlaků v jednotlivých měsících podle RBC (celá síť)",
        [TRACKSIDE]
    ],
    [
        CONNECTIONS_MONTH_311C,
        "Počty připojených vlaků v jednotlivých měsících (trať Olomouc - Uničov)",
        [TRACKSIDE]
    ],
    [
        ENGINES_DAY_TOTAL,
        "Počty vozidel v jednotlivých dnech (celá síť)",
        [TRACKSIDE]
    ],
    [
        ENGINES_DAY_RBC,
        "Počty vozidel v jednotlivých dnech podle RBC (celá síť)",
        [TRACKSIDE]
    ],
    [
        ENGINES_DAY_311C,
        "Počty vozidel v jednotlivých dnech (trať Olomouc - Uničov)",
        [TRACKSIDE]
    ],
    [
        ENGINES_MONTH_TOTAL,
        "Počty vozidel v jednotlivých měsících (celá síť)",
        [TRACKSIDE]
    ],
    [
        ENGINES_MONTH_RBC,
        "Počty vozidel v jednotlivých měsících podle RBC (celá síť)",
        [TRACKSIDE]
    ],
    [
        ENGINES_MONTH_311C,
        "Počty vozidel v jednotlivých měsících (trať Olomouc - Uničov)",
        [TRACKSIDE]
    ],
    [
        ISOLATIONS_MONTH_TOTAL,
        "Počty přechodů do módu IS v jednotlivých měsících (celá síť)",
        [TRACKSIDE]
    ],
    [
        ISOLATIONS_DAY_TOTAL,
        "Počty přechodů do módu IS v jednotlivých dnech (celá síť)",
        [TRACKSIDE]
    ],
    [
        ISOLATIONS_MONTH_RBC,
        "Počty přechodů do módu IS v jednotlivých měsících podle RBC (celá síť)",
        [TRACKSIDE]
    ],
    [
        ISOLATIONS_DAY_RBC,
        "Počty přechodů do módu IS v jednotlivých dnech podle RBC (celá síť)",
        [TRACKSIDE]
    ],
    [
        ISOLATIONS_MONTH_311C,
        "Počty přechodů do módu IS v jednotlivých měsících (trať Olomouc - Uničov)",
        [TRACKSIDE]
    ],
    [
        ISOLATIONS_DAY_311C,
        "Počty přechodů do módu IS v jednotlivých dnech (trať Olomouc - Uničov)",
        [TRACKSIDE]
    ],
    [
        INCIDENTS_MONTH_TOTAL,
        "Počty incidentů v jednotlivých měsících (celá síť)",
        [TRACKSIDE]
    ],
    [
        INCIDENTS_DAY_TOTAL,
        "Počty incidentů v jednotlivých dnech (celá síť)",
        [TRACKSIDE]
    ],
    [
        INCIDENTS_MONTH_RBC,
        "Počty incidentů v jednotlivých měsících podle RBC (celá síť)",
        [TRACKSIDE]
    ],
    [
        INCIDENTS_DAY_RBC,
        "Počty incidentů v jednotlivých dnech podle RBC (celá síť)",
        [TRACKSIDE]
    ],
    [
        INCIDENTS_MONTH_311C,
        "Počty incidentů v jednotlivých měsících (trať Olomouc - Uničov)",
        [TRACKSIDE]
    ],
    [
        INCIDENTS_DAY_311C,
        "Počty incidentů v jednotlivých dnech (trať Olomouc - Uničov)",
        [TRACKSIDE]
    ],
    [
        AFFECTED_MONTH_TOTAL,
        "Počty vlaků ovlivněných incidentem v jednotlivých měsících (celá síť)",
        [TRACKSIDE]
    ],
    [
        AFFECTED_DAY_TOTAL,
        "Počty vlaků ovlivněných incidentem v jednotlivých dnech (celá síť)",
        [TRACKSIDE]
    ],
    [
        AFFECTED_MONTH_RBC,
        "Počty vlaků ovlivněných incidentem v jednotlivých měsících podle RBC (celá síť)",
        [TRACKSIDE]
    ],
    [
        AFFECTED_DAY_RBC,
        "Počty vlaků ovlivněných incidentem v jednotlivých dnech podle RBC (celá síť)",
        [TRACKSIDE]
    ],
    [
        AFFECTED_MONTH_311C,
        "Počty vlaků ovlivněných incidentem v jednotlivých měsících (trať Olomouc - Uničov)",
        [TRACKSIDE]
    ],
    [
        AFFECTED_DAY_311C,
        "Počty vlaků ovlivněných incidentem v jednotlivých dnech (trať Olomouc - Uničov)",
        [TRACKSIDE]
    ],
    [
        TRAINS_DAY_PER_ENGINE,
        "Počty vlaků jedoucích ve FS v jednotlivých dnech (celá síť) podle HV",
        [VEHICLES]
    ],
    [
        TRAINS_CUMULATIVE_DAY_PER_ENGINE,
        "Počty vlaků jedoucích ve FS v jednotlivých dnech (celá síť) podle HV - kumulativně",
        [VEHICLES]
    ],
    [
        INCIDENTS_DAY_PER_ENGINE,
        "Počty incidentů v jednotlivých dnech (celá síť) podle HV",
        [VEHICLES]
    ],
    [
        INCIDENTS_CUMULATIVE_DAY_PER_ENGINE,
        "Počty incidentů v jednotlivých dnech (celá síť) podle HV - kumulativně",
        [VEHICLES]
    ],
    [
        AFFECTED_DAY_PER_ENGINE,
        "Počty vlaků ovlivněných incidentem v jednotlivých dnech (celá síť) podle HV",
        [VEHICLES]
    ],
    [
        AFFECTED_CUMULATIVE_DAY_PER_ENGINE,
        "Počty vlaků ovlivněných incidentem v jednotlivých dnech (celá síť) podle HV - kumulativně",
        [VEHICLES]
    ],
    [
        RELATIVE_AFFECTED_DAY_PER_ENGINE,
        "Poměr počtů vlaků ovlivněných incidentem v jednotlivých dnech počtem vlaků ve FS za dané období (celá síť) podle HV",
        [VEHICLES]
    ],
    [
        RELATIVE_AFFECTED_CUMULATIVE_DAY_PER_ENGINE,
        "Poměr počtů vlaků ovlivněných incidentem v jednotlivých dnech počtem vlaků ve FS za dané období (celá síť) podle HV - kumulativně",
        []
    ],
    [
        TRAINS_WEEK_PER_ENGINE,
        "Počty vlaků jedoucích ve FS v jednotlivých týdnech (celá síť) podle HV",
        [VEHICLES]
    ],
    [
        TRAINS_CUMULATIVE_WEEK_PER_ENGINE,
        "Počty vlaků jedoucích ve FS v jednotlivých týdnech (celá síť) podle HV - kumulativně",
        [VEHICLES]
    ],
    [
        INCIDENTS_WEEK_PER_ENGINE,
        "Počty incidentů v jednotlivých týdnech (celá síť) podle HV",
        [VEHICLES]
    ],
    [
        INCIDENTS_CUMULATIVE_WEEK_PER_ENGINE,
        "Počty incidentů v jednotlivých týdnech (celá síť) podle HV - kumulativně",
        [VEHICLES]
    ],
    [
        AFFECTED_WEEK_PER_ENGINE,
        "Počty vlaků ovlivněných incidentem v jednotlivých týdnech (celá síť) podle HV",
        [VEHICLES]
    ],
    [
        AFFECTED_CUMULATIVE_WEEK_PER_ENGINE,
        "Počty vlaků ovlivněných incidentem v jednotlivých týdnech (celá síť) podle HV - kumulativně",
        [VEHICLES]
    ],
    [
        RELATIVE_AFFECTED_WEEK_PER_ENGINE,
        "Poměr počtů vlaků ovlivněných incidentem v jednotlivých týdnech počtem vlaků ve FS za dané období (celá síť) podle HV",
        [VEHICLES]
    ],
    [
        RELATIVE_AFFECTED_CUMULATIVE_WEEK_PER_ENGINE,
        "Poměr počtů vlaků ovlivněných incidentem v jednotlivých týdnech počtem vlaků ve FS za dané období (celá síť) podle HV - kumulativně",
        []
    ],
    [
        TRAINS_MONTH_PER_ENGINE,
        "Počty vlaků jedoucích ve FS v jednotlivých měsících (celá síť) podle HV",
        [VEHICLES]
    ],
    [
        TRAINS_CUMULATIVE_MONTH_PER_ENGINE,
        "Počty vlaků jedoucích ve FS v jednotlivých měsících (celá síť) podle HV - kumulativně",
        [VEHICLES]
    ],
    [
        INCIDENTS_MONTH_PER_ENGINE,
        "Počty incidentů v jednotlivých měsících (celá síť) podle HV",
        [VEHICLES]
    ],
    [
        INCIDENTS_CUMULATIVE_MONTH_PER_ENGINE,
        "Počty incidentů v jednotlivých měsících (celá síť) podle HV - kumulativně",
        [VEHICLES]
    ],
    [
        AFFECTED_MONTH_PER_ENGINE,
        "Počty vlaků ovlivněných incidentem v jednotlivých měsících (celá síť) podle HV",
        [VEHICLES]
    ],
    [
        AFFECTED_CUMULATIVE_MONTH_PER_ENGINE,
        "Počty vlaků ovlivněných incidentem v jednotlivých měsících (celá síť) podle HV - kumulativně",
        [VEHICLES]
    ],
    [
        RELATIVE_AFFECTED_MONTH_PER_ENGINE,
        "Poměr počtů vlaků ovlivněných incidentem v jednotlivých měsících počtem vlaků ve FS za dané období (celá síť) podle HV",
        [VEHICLES]
    ],
    [
        RELATIVE_AFFECTED_CUMULATIVE_MONTH_PER_ENGINE,
        "Poměr počtů vlaků ovlivněných incidentem v jednotlivých měsících počtem vlaků ve FS za dané období (celá síť) podle HV - kumulativně",
        []
    ],
]


