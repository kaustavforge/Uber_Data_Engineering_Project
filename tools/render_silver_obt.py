"""Render the Jinja template into databricks/silver_obt.sql, and optionally upload it to your pipeline folder.

    uv run python tools/render_silver_obt.py                  # writes databricks/silver_obt.sql on your laptop
    uv run python tools/render_silver_obt.py --upload         # also uploads it into the Databricks pipeline folder

Edit JINJA_CONFIG below (add a lookup table, a column, a join, a where), run the script, and the pipeline
picks the new silver_obt.sql up on its next run. The template is the same idea as Ansh's silver_obt.ipynb:
loop over the config -> SELECT list, FROM + LEFT JOINs, optional WHERE.

--upload needs DATABRICKS_HOST and DATABRICKS_TOKEN in .env. It writes to
/Users/<you>/<project-folder>/<pipeline-folder>/transformations/silver_obt.sql as a plain workspace FILE.
Not tested on Free Edition: if it fails, paste the printed SQL into the pipeline's silver_obt.sql by hand.
If you use --upload, remove the notebook task 02_silver_obt_jinja from the Job (it does the same thing).
"""
import argparse
import io
import os
from pathlib import Path

from dotenv import load_dotenv
from jinja2 import Environment

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

TARGET = "uber.silver.silver_obt"
OUT_FILE = Path(__file__).resolve().parent.parent / "databricks" / "silver_obt.sql"

# One entry per table. The FIRST entry is the streaming fact side, the rest are LEFT JOINed lookups.
JINJA_CONFIG = [
    {
        "table": "STREAM(uber.silver.stg_rides) WATERMARK booking_timestamp DELAY OF INTERVAL 3 MINUTES stg_rides",
        "select": ["stg_rides.*"],
        "where": "",
    },
    {
        "table": "uber.bronze.map_vehicle_makes map_vehicle_makes",
        "select": ["map_vehicle_makes.vehicle_make"],
        "on": "stg_rides.vehicle_make_id = map_vehicle_makes.vehicle_make_id",
        "where": "",
    },
    {
        "table": "uber.bronze.map_vehicle_types map_vehicle_types",
        "select": [
            "map_vehicle_types.vehicle_type",
            "map_vehicle_types.description",
            "map_vehicle_types.base_rate",
            "map_vehicle_types.per_mile",
            "map_vehicle_types.per_minute",
        ],
        "on": "stg_rides.vehicle_type_id = map_vehicle_types.vehicle_type_id",
        "where": "",
    },
    {
        "table": "uber.bronze.map_ride_statuses map_ride_statuses",
        "select": ["map_ride_statuses.ride_status"],
        "on": "stg_rides.ride_status_id = map_ride_statuses.ride_status_id",
        "where": "",
    },
    {
        "table": "uber.bronze.map_payment_methods map_payment_methods",
        "select": [
            "map_payment_methods.payment_method",
            "map_payment_methods.is_card",
            "map_payment_methods.requires_auth",
        ],
        "on": "stg_rides.payment_method_id = map_payment_methods.payment_method_id",
        "where": "",
    },
    {
        "table": "uber.bronze.map_cities map_cities",
        "select": [
            "map_cities.city AS pickup_city",
            "map_cities.state",
            "map_cities.region",
            "map_cities.updated_at AS city_updated_at",
        ],
        "on": "stg_rides.pickup_city_id = map_cities.city_id",
        "where": "",
    },
    {
        "table": "uber.bronze.map_cancellation_reasons map_cancellation_reasons",
        "select": ["map_cancellation_reasons.cancellation_reason"],
        "on": "stg_rides.cancellation_reason_id = map_cancellation_reasons.cancellation_reason_id",
        "where": "",
    },
]

TEMPLATE = """\
CREATE OR REFRESH STREAMING TABLE {{ target }}
AS
SELECT
{% for config in jinja_config %}
    {{ config.select | join(",\\n    ") }}{% if not loop.last %},{% endif %}

{% endfor %}
FROM
{% for config in jinja_config %}
{% if loop.first %}
    {{ config.table }}
{% else %}
LEFT JOIN {{ config.table }}
       ON {{ config.on }}
{% endif %}
{% endfor %}
{% if wheres %}
WHERE {{ wheres | join(" AND ") }}
{% endif %}
;
"""


def render() -> str:
    env = Environment(trim_blocks=True, lstrip_blocks=True)
    wheres = [c["where"] for c in JINJA_CONFIG if c.get("where")]
    return env.from_string(TEMPLATE).render(target=TARGET, jinja_config=JINJA_CONFIG, wheres=wheres)


def upload(sql: str, project_folder: str, pipeline_folder: str) -> None:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.workspace import ImportFormat

    w = WorkspaceClient(host=os.getenv("DATABRICKS_HOST"), token=os.getenv("DATABRICKS_TOKEN"))
    user = w.current_user.me().user_name
    path = f"/Users/{user}/{project_folder}/{pipeline_folder}/transformations/silver_obt.sql"
    # RAW = import as a plain workspace file. (The SDK default, SOURCE, would turn a .sql into a SQL notebook
    # and drop the extension, which the pipeline can't use.)
    w.workspace.upload(path, io.BytesIO(sql.encode("utf-8")), format=ImportFormat.RAW, overwrite=True)
    status = w.workspace.get_status(path)
    kind = getattr(status.object_type, "value", status.object_type)
    print(f"-- uploaded to {path} (object type: {kind})")
    if str(kind) != "FILE":
        print("-- WARNING: expected a FILE. If it shows NOTEBOOK, delete it in the workspace and paste the SQL by hand.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--upload", action="store_true", help="also upload the file into the Databricks pipeline folder")
    ap.add_argument("--project-folder", default="Uber_Project", help="workspace folder that holds the pipeline")
    ap.add_argument("--pipeline-folder", default="uber_rides_ingest", help="the pipeline's folder name")
    args = ap.parse_args()

    sql = render()
    OUT_FILE.write_text(sql, encoding="utf-8")
    print(sql)
    print(f"-- written to {OUT_FILE}")
    if args.upload:
        upload(sql, args.project_folder, args.pipeline_folder)
