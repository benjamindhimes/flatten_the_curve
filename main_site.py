import asyncio
import datetime
from decimal import Decimal
import os

import pandas as pd
import plotly
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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
    if not county or county not in COUNTIES:
        return quart.abort(404)
    county_death_data, county_cases_data = await asyncio.gather(
        get_county_data(county),
        get_county_data(county, "cases")
    )
    # county_data_df = pd.DataFrame([convert_ts_in_obj(x["attributes"]) for x in county_death_data["features"]])
    death_average = Decimal(
        Decimal(
            sum(x["attributes"]["deaths"] for x in county_death_data["features"][-7:])
        ) / 7
    ).quantize(Decimal("0.01"))
    plot = render_complex_plot(
        [convert_ts_in_obj(x["attributes"]) for x in county_death_data["features"]],
        [convert_ts_in_obj(x["attributes"]) for x in county_cases_data["features"]],
        county
    )
    return await quart.render_template("display.html",
                                       graph=quart.Markup(plot.to_html()),
                                       county=county,
                                       death_average=death_average)


@app.errorhandler(404)
async def not_found(_):
    return "The requested data was not found."


async def get_county_data(county_name: str, data_type: str = "deaths"):
    case = {
        "deaths": (
            "Covid_Deaths_County",
            {"f": "json",
             "where": f"county='{county_name}'",
             "returnGeometry": False,
             "spatialRel": "esriSpatialRelIntersects",
             "outFields": "ObjectId,deaths,date",
             "orderByFields": "date asc",
             "resultOffset": 0,
             "resultRecordCount": 32000,
             "resultType": "standard",
             "cacheHint": True}),
        "cases": (
            "Covid_Cases_County",
            {"f": "json",
             "where": f"county='{county_name}'",
             "returnGeometry": False,
             "spatialRel": "esriSpatialRelIntersects",
             "outFields": "ObjectId,cases,date,county",
             "orderByFields": "date asc",
             "resultOffset": 0,
             "resultRecordCount": 32000,
             "resultType": "standard",
             "cacheHint": True}
        )
    }
    service, params = case.get(data_type)
    url = f"https://services2.arcgis.com/xtuWQvb2YQnp0z3F/arcgis/rest/services/{service}/FeatureServer/0/query"
    r = requests.get(url, params=params)
    return r.json()


def timestamp_to_date(ts: int) -> str:
    return datetime.datetime.fromtimestamp(Decimal(ts) / 1000).strftime('%Y-%m-%d')


def convert_ts_in_obj(obj: dict) -> dict:
    return {**obj, **{"date": timestamp_to_date(obj["date"])}}


def render_plot(county_data: pd.DataFrame, county_name: str) -> plotly.graph_objs.Figure:
    max_single_day_deaths = max(county_data["deaths"])
    fig = px.line(county_data,
                  y="deaths",
                  x="date",
                  title=f"{county_name} County Death Data",
                  range_y=[0, 10] if max_single_day_deaths < 11 else [0, max_single_day_deaths])
    return fig


def render_complex_plot(death_data: list, cases_data: list, name: str):
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(x=[_x["date"] for _x in death_data],
                   y=[_y["deaths"] for _y in death_data],
                   name="Deaths"),
        secondary_y=False
    )
    fig.add_trace(
        go.Scatter(x=[_x["date"] for _x in cases_data],
                   y=[_y["cases"] for _y in cases_data],
                   name="Cases"),
        secondary_y=True
    )
    fig.update_layout(
        title_text=f"{name.capitalize()} County Death/Cases Data",
        yaxis={"tickformat": ",d"}
    )
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Daily Deaths", secondary_y=False)
    fig.update_yaxes(title_text="Daily Cases", secondary_y=True)
    return fig


def show_plot(figure: plotly.graph_objs.Figure) -> None:
    figure.show()


async def play_with_me() -> None:
    county = "Allegheny"
    county_data_ = await get_county_data(county)
    county_data_df = pd.DataFrame([convert_ts_in_obj(x["attributes"]) for x in county_data_["features"]])
    plot = render_plot(county_data_df, county)
    show_plot(plot)


if __name__ == '__main__':
    asyncio.run(play_with_me())
