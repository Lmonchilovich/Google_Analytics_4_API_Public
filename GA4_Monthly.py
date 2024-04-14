import os
import time
import datetime
import pandas as pd
import calendar
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Filter,
    FilterExpression,
    Metric,
    RunReportRequest,
    FilterExpressionList
)

# Load Credentials
KEY_FILE_LOCATION = os.path.expanduser('~/PATH TO YOUR GOOGLE API KEY JSON FILE')
credentials = service_account.Credentials.from_service_account_file(KEY_FILE_LOCATION)
bigquery_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
analytics_client = BetaAnalyticsDataClient(credentials=credentials)


def fetch_ids():
    """
    Fetches records from a BigQuery table. Each record is expected to contain at least a domain name and
    a corresponding property ID.

    The function collects these entries and returns them as a list of tuples, where each tuple
    contains:
    - domain (str): The netloc domain name associated with the property.
    - property_id (str): The identifier for the view associated with the domain.

    :return: List[Tuple[str, str]]: A list of tuples, each containing the domain and its associated property ID.
    """
    sql = f"""
    SELECT * FROM `YOUR TABLE IN BIGQUERY`
    """

    # Execute the query
    properties_job = bigquery_client.query(sql)

    # Get the results
    properties_results = properties_job.result()

    return [(row.property_id, row.domain) for row in properties_results]


def organic_filter():
    """
    :return: Filter expression for organic traffic only
    """
    return FilterExpression(
        filter=Filter(
            field_name="sessionDefaultChannelGroup",
            string_filter=Filter.StringFilter(value="Organic Search"),
        )
    )


def blog_filter():
    """
    :return: Filter expression to remove blog traffic
    """
    return FilterExpression(not_expression=FilterExpression(filter=Filter(
        field_name="landingPage",
        string_filter=Filter.StringFilter(
            value="blog",
            match_type=Filter.StringFilter.MatchType.CONTAINS)
                )))


def blog_organic_filter():
    """
    :return: Combines the previous 2 filters and returns a single filter that filters on blog traffic, and
    organic traffic returning organic non blog traffic for a particular site. Use this as a base to construct
    other filters https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/FilterExpression
    """
    return FilterExpression(
                and_group=FilterExpressionList(expressions=[
                    FilterExpression(filter=Filter(
                        field_name="sessionDefaultChannelGroup",
                        string_filter=Filter.StringFilter(value="Organic Search")
                    )),
                    FilterExpression(not_expression=FilterExpression(filter=Filter(
                        field_name="landingPage",
                        string_filter=Filter.StringFilter(
                            value="blog",
                            match_type=Filter.StringFilter.MatchType.CONTAINS)
                    )))
                ])
            )


def run_total(property_id, start_date, end_date, max_retries=3):
    """
    Generates a GA4 traffic report for the specified property without any traffic filters.

    :param property_id: GA4 Property ID.
    :param start_date: Start date for the report.
    :param end_date: End date for the report.
    :param max_retries: Maximum number of retry attempts due to API failures.
    :return: Traffic report including active users, total users, and sessions.
    """
    retries = 0
    while retries < max_retries:
        try:
            request = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[],
                metrics=[
                    Metric(name="activeUsers"),
                    Metric(name="totalUsers"),
                    Metric(name="sessions")
                ],
                date_ranges=[DateRange(start_date=f"{start_date}", end_date=f"{end_date}")]
            )
            total_response = analytics_client.run_report(request)
            return total_response

        except Exception as e:
            print(f'Error fetching data for id: {property_id} on {start_date}: {e}. Attempt {retries+1} of {max_retries}')
            time.sleep(5)
            retries += 1

    print(f'Failed to fetch data after {max_retries} attempts.')
    return None


def run_organic(property_id, start_date, end_date, max_retries=3):
    """
    Generates a GA4 traffic report for the specified property filtered for organic traffic.

    :param property_id: GA4 Property ID.
    :param start_date: Start date for the report.
    :param end_date: End date for the report.
    :param max_retries: Maximum number of retry attempts due to API failures.
    :return: Traffic report for organic traffic including active users, total users, and sessions.
    """
    retries = 0
    while retries < max_retries:
        try:
            filter_expression = organic_filter()

            request = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[],
                metrics=[
                    Metric(name="activeUsers"),
                    Metric(name="totalUsers"),
                    Metric(name="sessions")
                ],
                date_ranges=[DateRange(start_date=f"{start_date}", end_date=f"{end_date}")],
                dimension_filter=filter_expression,
            )
            # Execute GA4 request
            organic_response = analytics_client.run_report(request)
            return organic_response

        except Exception as e:
            print(f'Error fetching data for id: {property_id} on {start_date}: {e}. Attempt {retries + 1} of {max_retries}')
            time.sleep(5)
            retries += 1

    print(f'Failed to fetch data after {max_retries} attempts.')
    return None


def run_total_filtered(property_id, start_date, end_date, max_retries=3):
    """
    Generates a GA4 traffic report for the specified property, filtering out blog traffic.

    :param property_id: GA4 Property ID.
    :param start_date: Start date for the report.
    :param end_date: End date for the report.
    :param max_retries: Maximum number of retry attempts due to API failures.
    :return: Traffic report excluding blog traffic, including active users, total users, and sessions.
    """
    retries = 0
    while retries < max_retries:
        try:
            filter_expression = blog_filter()

            request = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[],
                metrics=[
                    Metric(name="activeUsers"),
                    Metric(name="totalUsers"),
                    Metric(name="sessions")
                ],
                date_ranges=[DateRange(start_date=f"{start_date}", end_date=f"{end_date}")],
                dimension_filter=filter_expression,
            )
            # Execute GA4 request
            total_response_filtered = analytics_client.run_report(request)
            return total_response_filtered

        except Exception as e:
            print(f'Error fetching data for id: {property_id} on {start_date}: {e}. Attempt {retries+1} of {max_retries}')
            time.sleep(5)
            retries += 1

    print(f'Failed to fetch data after {max_retries} attempts.')
    return None


def run_organic_filtered(property_id, start_date, end_date, max_retries=3):
    """
    Generates a GA4 traffic report for the specified property, filtering only for organic traffic and excluding blog traffic.

    :param property_id: GA4 Property ID.
    :param start_date: Start date for the report.
    :param end_date: End date for the report.
    :param max_retries: Maximum number of retry attempts due to API failures.
    :return: Organic traffic report excluding blog content, including active users, total users, and sessions.
    """
    retries = 0
    while retries < max_retries:
        try:
            filter_expression = blog_organic_filter()

            request = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[],
                metrics=[
                    Metric(name="activeUsers"),
                    Metric(name="totalUsers"),
                    Metric(name="sessions")
                ],
                date_ranges=[DateRange(start_date=f"{start_date}", end_date=f"{end_date}")],
                dimension_filter=filter_expression,
            )
            # Execute GA4 request
            organic_response_filtered = analytics_client.run_report(request)
            return organic_response_filtered

        except Exception as e:
            print(f'Error fetching data for id: {property_id} on {start_date}: {e}. Attempt {retries+1} of {max_retries}')
            time.sleep(5)
            retries += 1

    print(f'Failed to fetch data after {max_retries} attempts.')
    return None


def transform_response(response):
    """
    Transforms the GA4 API response into a pandas DataFrame.

    :param response: The response data from the GA4 API to convert.
    :return: A DataFrame containing the response data, structured for analysis.
    """
    # Extract GA4 data into pandas DataFrame
    data = []
    for row in response.rows:
        dimension_values = [value.value for value in row.dimension_values]
        metric_values = [value.value for value in row.metric_values]
        data.append(dimension_values + metric_values)
    columns = ([dimension.name for dimension in response.dimension_headers] +
               [metric.name for metric in response.metric_headers])

    return pd.DataFrame(data, columns=columns)


def main():
    # Timing how long the script takes to run
    start_time = time.time()

    # Create a holding frame
    accumulated_df = pd.DataFrame()

    # Will iterate through the dates by Month and return a list
    start_date = datetime.date(2023, 7, 1)
    # Note this is the end date range of the entire pull this is different from the end date within each function
    end_date = datetime.date(2024, 1, 31)
    curr_date = start_date

    while curr_date <= end_date:

        # Grabs the last day of the month for the end date range parameter within the reporting functions
        last_day = calendar.monthrange(curr_date.year, curr_date.month)[1]
        month_end_date = datetime.date(curr_date.year, curr_date.month, last_day)

        for property_id, domain, stream_id in fetch_ids():

            print(f'Running for date range: {curr_date} and {month_end_date} for {domain}')

            start_date_str = curr_date.strftime('%Y-%m-%d')
            end_date_str = month_end_date.strftime('%Y-%m-%d')

            organic_response = run_organic(property_id, start_date_str, end_date_str)
            total_response = run_total(property_id, start_date_str, end_date_str)
            organic_response_filter = run_organic_filtered(property_id, start_date_str, end_date_str)
            total_response_filter = run_total_filtered(property_id, start_date_str, end_date_str)

            if organic_response_filter is not None and total_response_filter is not None:
                df_organic = transform_response(organic_response)
                df_total = transform_response(total_response)
                df_organic_filter = transform_response(organic_response_filter)
                df_total_filter = transform_response(total_response_filter)

                df_organic.rename(columns={"activeUsers": "organicActiveUsers",
                                           "totalUsers": "organicTotalUsers",
                                           "sessions": "organicSessions"}, inplace=True)
                df_total.rename(columns={"activeUsers": "activeUsers",
                                         "totalUsers": "totalUsers",
                                         "sessions": "totalSessions"}, inplace=True)
                df_organic_filter.rename(columns={"activeUsers": "organicActiveUsersFiltered",
                                                  "totalUsers": "organicTotalUsersFiltered",
                                                  "sessions": "organicSessionsFiltered"}, inplace=True)
                df_total_filter.rename(columns={"activeUsers": "activeUsersFiltered",
                                                "totalUsers": "totalUsersFiltered",
                                                "sessions": "totalSessionsFiltered"}, inplace=True)

                df_organic["month"] = start_date_str
                df_total["month"] = start_date_str
                df_organic_filter["month"] = start_date_str
                df_total_filter["month"] = start_date_str

                df_merged = pd.merge(df_total, df_organic, on=["month"], how="left")
                df_merged_filter = pd.merge(df_total_filter, df_organic_filter, on=["month"], how="left")
                df_combined = pd.merge(df_merged, df_merged_filter, on=["month"], how="left")

                df_combined.fillna(0, inplace=True)

                # Add domain column and pageURL
                df_combined['domain'] = domain

                # Append to the accumulated DataFrame
                accumulated_df = pd.concat([accumulated_df, df_combined], ignore_index=True)

        # Increment date
        curr_date += relativedelta(months=1)

    file_path_str = f"~/Downloads/{datetime.date.today} GA4 Monthly.csv"
    file_path = os.path.expanduser(file_path_str)
    accumulated_df.to_csv(file_path, index=False)

    end_time = time.time()
    print(f'Process took {round((end_time - start_time), 2)}')


if __name__ == "__main__":
    main()
