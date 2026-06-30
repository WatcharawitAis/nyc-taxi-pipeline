"""Gold Pipelines"""
from src.pipeline.utils.transformations import convert_day_number_to_name
from src.pipeline.utils.aggregations import (
    aggregate_by_day_of_week,
    round_metric_columns,
    sort_by_day_of_week,
)

def gold_pipeline(df):
    """Gold Pipeline Logic"""
    df = aggregate_by_day_of_week(df)
    df = convert_day_number_to_name(df)
    df = round_metric_columns(df)
    df = sort_by_day_of_week(df)
    return df
