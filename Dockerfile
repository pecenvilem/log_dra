FROM jupyter/scipy-notebook
RUN pip install voila ipyfilechooser geopandas pyarrow geopy ipyleaflet
COPY . /home/jovyan/work