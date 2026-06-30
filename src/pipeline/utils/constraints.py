"""Constraint"""

BRONZE_COLUMNS = [
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "pickup_zip",
            "dropoff_zip",
            "trip_distance",
            "fare_amount",
        ]

SILVER_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pickup_zip",
    "dropoff_zip",
    "trip_distance",
    "fare_amount",
    "trip_duration_minutes",
    "avg_speed_mph",
    "pickup_hour",
    "pickup_day_of_week",
]

GOLD_COLUMNS = [
    "pickup_day_of_week",
    "day_name",
    "total_rides",
    "total_fare",
    "avg_distance",
    "avg_fare",
    "avg_speed",
]
