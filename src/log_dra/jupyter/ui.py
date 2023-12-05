from IPython.display import display, clear_output, Markdown
import ipywidgets as widgets
from ipywidgets import HBox, VBox, Box, Dropdown, Output, Layout, Checkbox, SelectMultiple, Button, NaiveDatetimePicker
from map_tools import get_map
from log_dra.pipeline.analyse import list_train_numbers, list_engines, is_trip, is_post_trip

from datetime import date

import pandas as pd
from matplotlib.figure import Figure
from matplotlib.ticker import MaxNLocator
from matplotlib import pyplot as plt

import numpy as np

from typing import Iterable, Optional

from log_dra.constants.column_names import *
from log_dra.constants.modes import *
from errors import *

FIG_WIDTH = 5.0  # inch

class FilterConfigurator(Output):
    def __init__(self, data, filter_callback):
        super().__init__()
        active = Checkbox(description="Filtrovat záznamy", value=False)
        self.dls = SelectMultiple(
            options=data[DLS].drop_duplicates().tolist(),
            value=[],
            description='Filtrovat DLS',
            disabled=True
        )
        self.rbc = SelectMultiple(
            options=list(),
            value=list(),
            description='Filtrovat RBC',
            disabled=True
        )

        select_all_dls_button = Button(description='Vybrat všechna DLS', disabled=True)
        unselect_all_dls_button = Button(description='Zrušit výběr DLS', disabled=True)
        select_all_rbc_button = Button(description='Vybrat všechna RBC', disabled=True)
        unselect_all_rbc_button = Button(description='Zrušit výběr RBC', disabled=True)
        self.start_date_time = NaiveDatetimePicker(
            description="Čas od:", min=data[TIME].min(), max=data[TIME].max(), value=data[TIME].min(), disabled=True
        )
        self.end_date_time = NaiveDatetimePicker(
            description="Čas do:", min=data[TIME].min(), max=data[TIME].max(), value=data[TIME].max(), disabled=True
        )
        select_whole_range_button = Button(description='Vybrat maximální rozsah', disabled=True)
        apply_filter_button = Button(description='Filtrovat', disabled=True)
        
        def select_all_dls():
            self.dls.value = self.dls.options

        def unselect_dls():
            self.dls.value = list()

        def select_all_rbc():
            self.rbc.value = list([number for text, number in self.rbc.options])

        def unselect_rbc():
            self.rbc.value = list()
        
        def select_whole_range():
            self.start_date_time.value = data[TIME].min()
            self.end_date_time.value = data[TIME].max()

        def toggle_active(change_event):
            self.dls.disabled = not change_event["new"]
            self.rbc.disabled = not change_event["new"]
            select_all_dls_button.disabled = not change_event["new"]
            unselect_all_dls_button.disabled = not change_event["new"]
            select_all_rbc_button.disabled = not change_event["new"]
            unselect_all_rbc_button.disabled = not change_event["new"]
            apply_filter_button.disabled = not change_event["new"]
            self.start_date_time.disabled = not change_event["new"]
            self.end_date_time.disabled = not change_event["new"]
            select_whole_range_button.disabled = not change_event["new"]
            if change_event["new"] == False:
                filter_callback({})

        def update_rbc_selector(change_event):
            dls_filter = data[DLS].isin(change_event["new"])
            rbc_options = data.loc[dls_filter, [RBC_ID, RBC_NAME]].drop_duplicates()
            rbc_strings = rbc_options[RBC_ID].astype(str).str.cat(rbc_options[RBC_NAME], sep=": ")
            rbc_ids = rbc_options[RBC_ID]
            self.rbc.options = list(zip(rbc_strings, rbc_ids))

        active.observe(toggle_active, names="value", type="change")
        self.dls.observe(update_rbc_selector, names="value", type="change")
        select_all_dls_button.on_click(lambda _: select_all_dls())
        unselect_all_dls_button.on_click(lambda _: unselect_dls())
        select_all_rbc_button.on_click(lambda _: select_all_rbc())
        unselect_all_rbc_button.on_click(lambda _: unselect_rbc())
        apply_filter_button.on_click(lambda _: filter_callback(self.get_filter_parameters()))
        select_whole_range_button.on_click(lambda _: select_whole_range())

        with self:
            dls_buttons = VBox([select_all_dls_button, unselect_all_dls_button])
            rbc_buttons = VBox([select_all_rbc_button, unselect_all_rbc_button])
            date_times = VBox([self.start_date_time, self.end_date_time])
            display(
                HBox([active, apply_filter_button]),
                HBox([self.dls, dls_buttons, self.rbc, rbc_buttons, date_times, select_whole_range_button])
            )
        
    def get_filter_parameters(self):
        return {
            DLS: self.dls.value,
            RBC_ID: self.rbc.value,
            "START_TIME": self.start_date_time.value,
            "END_TIME": self.end_date_time.value,
        }
        

def mode_highlighter_factory(mode):
    if isinstance(mode, str):
        mode = [mode]
    def highlight_mode(series, color="yellow"):
        s = pd.Series(data=False, index=series.index)
        return [f'background-color: {color}' if series[MODE] in mode else '' for column in s.index]
    return highlight_mode

def highlight_illegal_sr(series, color="yellow"):
    s = pd.Series(data=False, index=series.index)
    return [f'background-color: {color}' if series[ILLEGAL_SR] else '' for column in s.index]

def highlight_connection_loss(series, color="yellow"):
    s = pd.Series(data=False, index=series.index)
    return [f'background-color: {color}' if series[CONNECTION_LOSS] else '' for column in s.index]

def highlight_tr_r(series, color="yellow"):
    s = pd.Series(data=False, index=series.index)
    return [f'background-color: {color}' if series[CONNECTION_LOSS] or series[MODE] in [TR, PT] else '' for column in s.index]

highlighters = {
    TR_EVENT: mode_highlighter_factory([TR, PT]),
    ILLEGAL_SR_EVENT: highlight_illegal_sr,
    CONNECTION_LOSS_EVENT: highlight_connection_loss,
    SYSTEM_FAILURE_EVENT: mode_highlighter_factory(SF),
    ISOLATION_EVENT: mode_highlighter_factory(IS),
    TR_R_EVENT: highlight_tr_r,
}

def get_event_plot(dataset: pd.DataFrame) -> Figure:
    counts = get_event_counts(dataset)
    with plt.ioff():
        fig, ax = plt.subplots()
        fig.set_figwidth(FIG_WIDTH)
        if len(counts) > 0:
            counts.groupby("Datum").sum().plot.bar(ax=ax)
            ax.set_title("Počty událostí podle data")
            ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        else:
            ax.text(.5, .5, "Žádná událost", ha="center", va="center")
        plt.tight_layout()
    return fig.canvas

def get_event_counts(dataset: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame(dataset.groupby([dataset[TIME].dt.date, OBU, TRAIN_NUMBER], dropna=False).size(), columns=[f"Počet"])
    df.index.set_names("Datum", level=TIME, inplace=True)
    return df

def get_total_trains(dataset: pd.DataFrame, filter_parameters: Optional[dict]) -> None:
    dataset = dataset.copy()
    dataset = apply_filter(dataset, filter_parameters)
    dataset[TIME] = dataset[TIME].dt.date
    dataset = dataset[dataset[TRAIN_NUMBER].between(1, 899999) & (~dataset[TRAIN_NUMBER].isna())]
    number_unique = len(dataset[[TRAIN_NUMBER, TIME]].drop_duplicates())
    display(Markdown(f"#### Celkový počet vlaků: {number_unique}"))

def get_total_engines(dataset: pd.DataFrame, filter_parameters: Optional[dict]) -> None:
    dataset = dataset.copy()
    dataset = apply_filter(dataset, filter_parameters)
    number_unique = len(dataset[[OBU]].drop_duplicates())
    display(Markdown(f"#### Celkový počet vozidel: {number_unique}"))
    
def get_rbc_list(data: pd.DataFrame) -> Output:
    rbc_df = data[[RBC_ID, RBC_NAME]].drop_duplicates()
    output = Output()
    with output:
        display(
            Markdown(f"#### RBC v souboru: {len(rbc_df)}"),
            rbc_df.style.hide(axis="index")
        )
    return output

def get_dls_list(data: pd.DataFrame) -> Output:
    dls_df = data[[DLS]].drop_duplicates()
    output = Output()
    with output:
        display(
            Markdown(f"#### DLS v souboru: {len(dls_df)}"),
            dls_df.style.hide(axis="index")
        )
    return output

def get_time_range(data: pd.DataFrame) -> Output:
    time_range = pd.DataFrame({"Od": data[TIME].min(), "Do": data[TIME].max()}, index=pd.Index([0]))
    output = widgets.Output()
    with output:
        display(Markdown("#### Záznamy"))
        display(time_range.T.style.hide(axis="columns"))
    return output

def apply_filter(data, filter_parameters):
    if filter_parameters is not None:
        for column, value in filter_parameters.items():
            if column == "START_TIME":
                data = data[data[TIME] >= value]
            elif column == "END_TIME":
                data = data[data[TIME] <= value]
            else:
                data = data[data[column].isin(value)]
    return data

def get_active_trains(data: pd.DataFrame, filter_parameters) -> None:
    data = apply_filter(data, filter_parameters)
    data = data[data[TRAIN_NUMBER].between(1, 899999) & (~data[TRAIN_NUMBER].isna())]
    with plt.ioff():
        left, right, plot_output = Output(), Output(), Output()
        train_list_output = Output()

        dropdown = Dropdown(
            description="Vyber datum",
            options = sorted(data[TIME].dt.date.unique())
        )

        def redraw_plot(change_event):
            with plot_output:
                clear_output()
                fig, ax = plt.subplots()
                fig.set_figwidth(FIG_WIDTH)
                data.loc[data[TIME].dt.date.isin([change_event["new"]])].resample("H", on=TIME)[TRAIN_NUMBER].nunique().plot(ax=ax)
                ax.set_title(f"Počty přihlášených vlaků {dropdown.value} podle hodin")
                ax.set_ylabel("Počet komunikujících vlaků")
                ax.yaxis.set_major_locator(MaxNLocator(integer=True))
                display(fig.canvas)
            with train_list_output:
                clear_output()
                number_list = list_train_numbers(data.loc[data[TIME].dt.date.isin([change_event["new"]]), TRAIN_NUMBER])
                box_layout = Layout(overflow_y='auto', height='500px')
                output = Output()
                with output:
                    display(number_list.style.hide(axis="index"))
                display(Box(children=[output], layout=box_layout))

        dropdown.observe(redraw_plot, names="value", type="change")

        try:
            redraw_plot({"new": dropdown.options[0]})
        except IndexError:
            pass

        with right:
            display(dropdown)

        fig_left, ax = plt.subplots()
        fig_left.set_figwidth(FIG_WIDTH)
        data.resample("D", on=TIME)[TRAIN_NUMBER].nunique().plot.bar(ax=ax)
        ax.set_title("Počty přihlášených vlaků podle dní (celkově za den)")
        ax.set_ylabel("Počet komunikujících vlaků")
        ax.set_xlabel("Datum")
        ax.set_xticklabels(pd.Series(data.resample("D", on=TIME).groups.keys()).dt.strftime("%Y-%m-%d"))
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        with left: display(fig_left.canvas)
        
        clear_output()
        display(
            Markdown("#### Průběh počtu aktivních vlaků"),
            HBox([left, VBox([right, plot_output]), train_list_output])
        )
        plt.tight_layout()

def get_active_engines(data: pd.DataFrame, filter_parameters) -> None:
    data = apply_filter(data, filter_parameters)
    with plt.ioff():
        left, right, plot_output = Output(), Output(), Output()
        train_list_output = Output()

        dropdown = Dropdown(
            description="Vyber datum",
            options = sorted(data[TIME].dt.date.unique())
        )

        def redraw_plot(change_event):
            with plot_output:
                clear_output()
                fig, ax = plt.subplots()
                data.loc[data[TIME].dt.date.isin([change_event["new"]])].resample("H", on=TIME)[OBU].nunique().plot(ax=ax)
                ax.set_title(f"Počty přihlášených vozidel {dropdown.value} podle hodin")
                ax.set_ylabel("Počet komunikujících vozidel")
                ax.yaxis.set_major_locator(MaxNLocator(integer=True))
                display(fig.canvas)
            with train_list_output:
                clear_output()
                number_list = list_engines(data.loc[data[TIME].dt.date.isin([change_event["new"]]), OBU])
                box_layout = Layout(overflow_y='auto', height='500px')
                output = Output()
                with output:
                    display(number_list.style.hide(axis="index"))
                display(Box(children=[output], layout=box_layout))

        dropdown.observe(redraw_plot, names="value", type="change")

        try:
            redraw_plot({"new": dropdown.options[0]})
        except IndexError:
            pass

        with right:
            display(dropdown)

        fig_left, ax = plt.subplots()
        data.resample("D", on=TIME)[OBU].nunique().plot.bar(ax=ax)
        ax.set_title("Počty přihlášených vozidel podle dní (celkově za den)")
        ax.set_ylabel("Počet komunikujících vozidel")
        ax.set_xlabel("Datum")
        ax.set_xticklabels(pd.Series(data.resample("D", on=TIME).groups.keys()).dt.strftime("%Y-%m-%d"))
        ax.yaxis.set_major_locator(MaxNLocator(integer=True))
        with left: display(fig_left.canvas)
        
        clear_output()
        display(
            Markdown("#### Průběh počtu aktivních vozidel"),
            HBox([left, VBox([right, plot_output]), train_list_output])
        )
        plt.tight_layout()

def get_filter_config(data, callback):
    return FilterConfigurator(data, callback)

def render_summary(dataset: pd.DataFrame, mode: str, _: pd.DataFrame, filter_parameters: Optional[dict]):
    dataset = apply_filter(dataset, filter_parameters)
    counts = get_event_counts(dataset)
    counts = counts.reset_index(TRAIN_NUMBER)
    counts[TRAIN_NUMBER] = counts[TRAIN_NUMBER].astype("object")
    counts[TRAIN_NUMBER] = counts[TRAIN_NUMBER].replace(np.nan, "Nezadáno")
    counts = counts.set_index(TRAIN_NUMBER, append=True)
    plot = get_event_plot(dataset)
    display(Markdown(f"Počet nalezených událostí: {len(dataset)}"))
    display(Markdown(f"Počty událostí podle vlaků"))
    top, bottom = widgets.Output(), widgets.Output()
    with top:
        s = counts.T.style
        if len(counts):
            s.applymap_index(lambda _: "border-width: thin; border-style: solid", axis="columns")
            s.applymap(lambda _: "border-width: thin; border-style: solid")
            display(s)
    with bottom:
        display(plot)
    display(VBox([top, bottom]))
    display(Markdown(f"Celkem postižených vlaků: {counts.index.nunique()}"))

def render_list(dataset: pd.DataFrame, mode: str, data: pd.DataFrame, filter_parameters: Optional[dict]):
    dataset = apply_filter(dataset, filter_parameters).sort_values(by=TIME)
    display(Markdown(f"Výpis všech nalezených událostí: {mode} - celkový počet záznamů: {len(dataset)}"))
    with pd.option_context('display.max_rows', None):
        display(dataset[DISPLAY_COLUMNS])

def render_detail(dataset: pd.DataFrame, mode: str, data: pd.DataFrame, filter_parameters: Optional[dict]):
    dataset = apply_filter(dataset, filter_parameters)
    display(Markdown(f"Výpis všech záznamů pro vybraný vlak"))
    
    def change_selection(selector, new_value):
        selector.value = new_value

    full_selection = dataset[TIME].dt.date.unique().tolist()
    empty_selection = []
    
    date_selector = widgets.SelectMultiple(
        options=full_selection,
        value=full_selection,
        description='Datum',
        disabled=False
    )
    
    train_selector = widgets.Dropdown(description='Číslo vlaku')
    
    select_all_button = widgets.Button(description='Vybrat vše')
    unselect_all_button = widgets.Button(description='Zrušit výběr')
    
    output = widgets.Output()

    select_all_button.on_click(lambda _: change_selection(date_selector, full_selection))
    unselect_all_button.on_click(lambda _: change_selection(date_selector, empty_selection))
    
    def redraw(date_range: Iterable[date], train_number: int):
        record_filter = (data[TIME].dt.date.isin(date_range)) & (data[TRAIN_NUMBER] == train_number)
        subframe = data[record_filter].copy()
        subframe.sort_values(TIME, ascending=True, inplace=True)
        styler = subframe.style.apply(highlighters[mode], axis='columns')
        columns_to_hide = [column for column in subframe.columns if column not in DISPLAY_COLUMNS]
        styler.hide(columns_to_hide, axis="columns")
        return styler
    
    def update_date(change_event):
        date_range = change_event["new"]
        train_selector.value = None
        train_filter = dataset[TIME].dt.date.isin(date_range)
        train_selector.options = sorted(pd.unique(dataset.loc[train_filter, TRAIN_NUMBER]).dropna())
    
    def update_train(change_event):
        with output:
            clear_output()
            display(redraw(date_selector.value, change_event["new"])) 

    date_selector.observe(update_date, names="value", type="change")
    train_selector.observe(update_train, names="value", type="change")
    
    update_date({"new": full_selection})
    
    display(HBox([date_selector, VBox([select_all_button, unselect_all_button])]))
    display(train_selector, output)

def render_ui(loaded_file_name: str, data: pd.DataFrame):
    dls_list = get_dls_list(data)
    rbc_list = get_rbc_list(data)
    time_range = get_time_range(data)
    area_map = get_map(data)
    active_trains = Output()
    total_trains = Output()
    total_engines = Output()
    active_engines = Output()
    with active_trains: get_active_trains(data, None)
    with total_trains: get_total_trains(data, None)
    with active_engines: get_active_engines(data, None)
    with total_engines: get_total_engines(data, None)
    
    datasets = {
        TR_EVENT: data[(is_trip(data)) | (is_post_trip(data))],
        ILLEGAL_SR_EVENT: data[data[ILLEGAL_SR]],
        CONNECTION_LOSS_EVENT: data[data[CONNECTION_LOSS]],
        SYSTEM_FAILURE_EVENT: data[data[SYSTEM_FAILURE]],
        ISOLATION_EVENT: data[data[ISOLATION]],
        TR_R_EVENT: data[data[CONNECTION_LOSS] | ((data[MODE].isin([TR, PT])) & (~data[PREV_MODE].isin([PT, TR])))]
    }
    
    accordion_fields = (
        {"name": "Souhrn", "renderer": render_summary},
        {"name": "Seznam", "renderer": render_list},
        {"name": "Detail", "renderer": render_detail}
    )

    accordions = {
        TR_EVENT: {field["name"]: (widgets.Output(), field["renderer"]) for field in accordion_fields},
        CONNECTION_LOSS_EVENT: {field["name"]: (widgets.Output(), field["renderer"]) for field in accordion_fields},
        ILLEGAL_SR_EVENT: {field["name"]: (widgets.Output(), field["renderer"]) for field in accordion_fields},
        SYSTEM_FAILURE_EVENT: {field["name"]: (widgets.Output(), field["renderer"]) for field in accordion_fields},
        ISOLATION_EVENT: {field["name"]: (widgets.Output(), field["renderer"]) for field in accordion_fields},
        TR_R_EVENT: {field["name"]: (widgets.Output(), field["renderer"]) for field in accordion_fields},
    }

    tab_outputs = {
        TR_EVENT: widgets.Output(),
        CONNECTION_LOSS_EVENT: widgets.Output(),
        ILLEGAL_SR_EVENT: widgets.Output(),
        SYSTEM_FAILURE_EVENT: widgets.Output(),
        ISOLATION_EVENT: widgets.Output(),
        TR_R_EVENT: widgets.Output(),
    }
    
    for mode, tab_output in tab_outputs.items():
        with tab_output:
            clear_output()
            display(Markdown(f"### {mode} - nalezené události"))
            for field, (output, renderer) in accordions[mode].items():
                with output:
                    display(renderer(datasets[mode], mode, data, None))
                display(widgets.Accordion(children=[output], titles=[field]))
    
    def update_filters(filter_parameters):
        with active_trains:
            clear_output()
            get_active_trains(data, filter_parameters)
        
        with total_trains:
            clear_output()
            get_total_trains(data, filter_parameters)
        
        with active_engines:
            clear_output()
            get_active_engines(data, filter_parameters)
        
        for mode, tab_output in tab_outputs.items():
            with tab_output:
                clear_output()
                display(Markdown(f"### {mode} - nalezené události"))
                for field, (output, renderer) in accordions[mode].items():
                    with output:
                        clear_output()
                        display(renderer(datasets[mode], mode, data, filter_parameters))
                    display(widgets.Accordion(children=[output], titles=[field]))
    
    filter_config = get_filter_config(data, update_filters)
    
    tab = widgets.Tab()
    tab.children = list(tab_outputs.values())
    tab.titles = list(tab_outputs.keys())
    output = widgets.Output()
    with output:
        display(
            Markdown(f"### Soubor: '{loaded_file_name}'"),
            HBox([
                dls_list, rbc_list, area_map
            ]),
            HBox([
                filter_config
            ]),
            HBox([
                VBox([time_range, total_trains]), active_trains
            ]),
            HBox([active_engines]),
            total_engines,
            tab
        )
    return output
