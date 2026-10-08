"""Homework 1 from the video: render the Jinja template straight into databricks/silver_obt.sql
(no manual copy-paste).

    uv run python tools/render_silver_obt.py

Edit JINJA_CONFIG below (add a lookup table, a column, a join, a where), run the script,
then re-sync the pipeline source. The template is the same idea as Ansh's silver_obt.ipynb:
loop over the config -> SELECT list, FROM + LEFT JOINs, optional WHERE.
"""
from pathlib import Path

from jinja2 import Environment

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


if __name__ == "__main__":
    sql = render()
    OUT_FILE.write_text(sql, encoding="utf-8")
    print(sql)
    print(f"-- written to {OUT_FILE}")
