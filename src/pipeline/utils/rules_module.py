"""Rule sets for data quality"""

def get_rules_by_names(name: list):
  """
    loads data quality rules from a table
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  return {
    row['name']: row['constraint']
    for row in get_rules_as_list_of_dict()
    if row['name'] in name
  }

def get_rules_as_list_of_dict():
  return [
    {
        "name": "valid_timestamp_tpep_pickup_datetime",
        "constraint": "CAST(tpep_pickup_datetime AS TIMESTAMP) IS NOT NULL",
        "tag": "nyc-taxi"
    },
    {
        "name": "valid_timestamp_tpep_dropoff_datetime",
        "constraint": "CAST(tpep_dropoff_datetime AS TIMESTAMP) IS NOT NULL",
        "tag": "nyc-taxi"
    },
    {
        "name": "valid_fare_amount",
        "constraint": "fare_amount > 0",
        "tag": "nyc-taxi"
    },
    {
        "name": "valid_distance",
        "constraint": "trip_distance > 0",
        "tag": "nyc-taxi"
    },
    {
        "name": "valid_pickup_day_of_week",
        "constraint": "pickup_day_of_week BETWEEN 1 AND 7",
        "tag": "nyc-taxi"
    },
    {
        "name": "valid_pickup_hour",
        "constraint": "pickup_hour BETWEEN 0 AND 23",
        "tag": "nyc-taxi"
    }
  ]

if __name__ == "__main__":
  print(get_rules_by_names(["valid_pickup_hour"]))
