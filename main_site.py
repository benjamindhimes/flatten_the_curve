import datetime
import os

import pandas as pd
import plotly
import plotly.express as px
import quart
import requests
import yaml

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))

COUNTIES = yaml.safe_load(
    open(os.path.join(
        ROOT_DIR,
        "config",
        "counties.yaml"
    ))
)

app = quart.Quart(__name__, template_folder=os.path.join(ROOT_DIR, "templates"))


@app.route("/")
async def index_page():
    return await quart.render_template("index.html",
                                       counties=COUNTIES)


@app.route("/death-chart")
async def death_chart():
    county = quart.request.args.get("county")
    if not county:
        return quart.abort(404)
    county_data_ = await get_county_data(county)
    county_data_df = pd.DataFrame([convert_ts_in_obj(x["attributes"]) for x in county_data_["features"]])
    plot = render_plot(county_data_df, county)
    return plot.to_html()


async def get_county_data(county_name: str):
    url = (f"https://services2.arcgis.com/xtuWQvb2YQnp0z3F/arcgis/rest/services/Covid_Deaths_County/FeatureServer/0/"
           f"query?f=json&where=county%3D%27{county_name}%27"
           f"&returnGeometry=false"
           f"&spatialRel=esriSpatialRelIntersects"
           f"&outFields=ObjectId%2Cdeaths%2Cdate&orderByFields=date%20asc"
           f"&resultOffset=0&resultRecordCount=32000&resultType=standard&cacheHint=true")
    r = requests.get(url)
    return r.json()


def timestamp_to_date(ts: int) -> str:
    return datetime.datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d')


def convert_ts_in_obj(obj: dict) -> dict:
    return {**obj, **{"date": timestamp_to_date(obj["date"])}}


def render_plot(county_data: pd.DataFrame, county_name: str) -> plotly.graph_objs.Figure:
    fig = px.line(county_data, y="deaths", x="date", title=f"{county_name} County Death Data")
    return fig


def show_plot(figure: plotly.graph_objs.Figure) -> None:
    figure.show()


async def play_with_me() -> None:
    county = "Allegheny"
    county_data_ = get_county_data(county)
    county_data_df = pd.DataFrame([convert_ts_in_obj(x["attributes"]) for x in county_data_["features"]])
    plot = render_plot(county_data_df, county)
    show_plot(plot)


if __name__ == '__main__':
    app.run("0.0.0.0:8000", debug=False)
