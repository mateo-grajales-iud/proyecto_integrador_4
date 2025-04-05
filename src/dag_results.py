from collections import namedtuple
from enum import Enum
from typing import Callable, Dict, List

import pandas as pd
from pandas import DataFrame, read_sql
from sqlalchemy import text
from sqlalchemy.engine.base import Engine

from src.config import QUERIES_ROOT_PATH

QueryResult = namedtuple("QueryResult", ["query", "result"])


class QueryEnum(Enum):
    """This class enumerates all the queries that are available"""

    DELIVERY_DATE_DIFFERECE = "delivery_date_difference"
    GLOBAL_AMMOUNT_ORDER_STATUS = "global_ammount_order_status"
    REVENUE_BY_MONTH_YEAR = "revenue_by_month_year"
    REVENUE_PER_STATE = "revenue_per_state"
    TOP_10_LEAST_REVENUE_CATEGORIES = "top_10_least_revenue_categories"
    TOP_10_REVENUE_CATEGORIES = "top_10_revenue_categories"
    REAL_VS_ESTIMATED_DELIVERED_TIME = "real_vs_estimated_delivered_time"
    ORDERS_PER_DAY_AND_HOLIDAYS_2017 = "orders_per_day_and_holidays_2017"
    GET_FREIGHT_VALUE_WEIGHT_RELATIONSHIP = "get_freight_value_weight_relationship"


def query_delivery_date_difference(database: Engine) -> QueryResult:
    query_name = QueryEnum.DELIVERY_DATE_DIFFERECE.value
    query = read_sql("SELECT * FROM delivery_date_difference", database)
    return QueryResult(query=query_name, result=query)


def query_global_ammount_order_status(database: Engine) -> QueryResult:
    query_name = QueryEnum.GLOBAL_AMMOUNT_ORDER_STATUS.value
    query = read_sql("SELECT * FROM global_ammount_order_status", database)
    return QueryResult(query=query_name, result=query)


def query_revenue_by_month_year(database: Engine) -> QueryResult:
    query_name = QueryEnum.REVENUE_BY_MONTH_YEAR.value
    query = read_sql("SELECT * FROM revenue_by_month_year", database)
    return QueryResult(query=query_name, result=query)


def query_revenue_per_state(database: Engine) -> QueryResult:
    query_name = QueryEnum.REVENUE_PER_STATE.value
    query = read_sql("SELECT * FROM revenue_per_state", database)
    return QueryResult(query=query_name, result=query)


def query_top_10_least_revenue_categories(database: Engine) -> QueryResult:
    query_name = QueryEnum.TOP_10_LEAST_REVENUE_CATEGORIES.value
    query = read_sql("SELECT * FROM top_10_least_revenue_categories", database)
    return QueryResult(query=query_name, result=query)


def query_top_10_revenue_categories(database: Engine) -> QueryResult:
    query_name = QueryEnum.TOP_10_REVENUE_CATEGORIES.value
    query = read_sql("SELECT * FROM top_10_revenue_categories", database)
    return QueryResult(query=query_name, result=query)


def query_real_vs_estimated_delivered_time(database: Engine) -> QueryResult:
    query_name = QueryEnum.REAL_VS_ESTIMATED_DELIVERED_TIME.value
    query = read_sql("SELECT * FROM real_vs_estimated_delivered_time", database)
    return QueryResult(query=query_name, result=query)


def query_freight_value_weight_relationship(database: Engine) -> QueryResult:
    query_name = QueryEnum.GET_FREIGHT_VALUE_WEIGHT_RELATIONSHIP.value
    orders = read_sql("SELECT * FROM get_freight_value_weight_relationship", database)
    return QueryResult(query=query_name, result=orders)


def query_orders_per_day_and_holidays_2017(database: Engine) -> QueryResult:
    query_name = QueryEnum.ORDERS_PER_DAY_AND_HOLIDAYS_2017.value
    holidays = read_sql("SELECT * FROM orders_per_day_and_holidays_2017", database)
    return QueryResult(query=query_name, result=holidays)


def get_all_queries() -> List[Callable[[Engine], QueryResult]]:
    """Get all queries.

    Returns:
        List[Callable[[Engine], QueryResult]]: A list of all queries.
    """
    return [
        query_delivery_date_difference,
        query_global_ammount_order_status,
        query_revenue_by_month_year,
        query_revenue_per_state,
        query_top_10_least_revenue_categories,
        query_top_10_revenue_categories,
        query_real_vs_estimated_delivered_time,
        query_orders_per_day_and_holidays_2017,
        query_freight_value_weight_relationship,
    ]


def run_queries_processed(database: Engine) -> Dict[str, DataFrame]:
    """Transform data based on the queries. For each query, the query is executed and
    the result is stored in the dataframe.

    Args:
        database (Engine): Database connection.

    Returns:
        Dict[str, DataFrame]: A dictionary with keys as the query file names and
        values the result of the query as a dataframe.
    """
    query_results = {}
    for query in get_all_queries():
        query_result = query(database)
        query_results[query_result.query] = query_result.result
    return query_results
