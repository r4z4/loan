from dagster import asset
from loan.loan import SQL
import pandas as pd

@asset
def population() -> SQL:
    df = pd.read_html("https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)")[0]
    df.columns = [
        "country",
        "population_2022",
        "population_2023",
        "pop_change",
        "continent",
        "subregion",
    ]
    df["pop_change"] = [
        float(str(row).rstrip("%").replace("\u2212", "-")) for row in df["pop_change"]
    ]
    return SQL("select * from $df", df=df)

@asset
def continent_population(population: SQL) -> SQL:
    return SQL(
        "select continent, avg(pop_change) as avg_pop_change from $population group by 1 order by 2 desc",
        population=population,
    )