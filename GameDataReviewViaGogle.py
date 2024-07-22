import json

import argparse
import pandas as pd
from time import sleep
from typing import List, Optional, Tuple
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from google_play_scraper import Sort
from google_play_scraper.constants.element import ElementSpecs
from google_play_scraper.constants.regex import Regex
from google_play_scraper.constants.request import Formats
from google_play_scraper.utils.request import post

MAX_COUNT_EACH_FETCH = 4500

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app_reviews_scraper.log"),
        logging.StreamHandler()
    ]
)


class _ContinuationToken:
    __slots__ = (
        "token",
        "lang",
        "country",
        "sort",
        "count",
        "filter_score_with",
        "filter_device_with",
    )

    def __init__(
            self, token, lang, country, sort, count, filter_score_with, filter_device_with
    ):
        self.token = token
        self.lang = lang
        self.country = country
        self.sort = sort
        self.count = count
        self.filter_score_with = filter_score_with
        self.filter_device_with = filter_device_with


def _fetch_review_items(
        url: str,
        app_id: str,
        sort: int,
        count: int,
        filter_score_with: Optional[int],
        filter_device_with: Optional[int],
        pagination_token: Optional[str],
):
    dom = post(
        url,
        Formats.Reviews.build_body(
            app_id,
            sort,
            count,
            "null" if filter_score_with is None else filter_score_with,
            "null" if filter_device_with is None else filter_device_with,
            pagination_token,
        ),
        {"content-type": "application/x-www-form-urlencoded"},
    )
    match = json.loads(Regex.REVIEWS.findall(dom)[0])
    try:
        token = json.loads(match[0][2])[-2][-1]
    except:
        token = None

    results = json.loads(match[0][2])
    if len(results) == 0 or len(results[0]) == 0:
        return [], token
    return results[0], token


def reviews(
        app_id: str,
        lang: str = "en",
        country: str = "us",
        sort: Sort = Sort.NEWEST,
        count: int = 100,
        filter_score_with: int = None,
        filter_device_with: int = None,
        continuation_token: _ContinuationToken = None,
) -> Tuple[List[dict], _ContinuationToken]:
    sort = sort.value

    if continuation_token is not None:
        token = continuation_token.token

        if token is None:
            return (
                [],
                continuation_token,
            )

        lang = continuation_token.lang
        country = continuation_token.country
        sort = continuation_token.sort
        count = continuation_token.count
        filter_score_with = continuation_token.filter_score_with
        filter_device_with = continuation_token.filter_device_with
    else:
        token = None

    url = Formats.Reviews.build(lang=lang, country=country)

    _fetch_count = count

    result = []

    while True:
        if _fetch_count == 0:
            break

        if _fetch_count > MAX_COUNT_EACH_FETCH:
            _fetch_count = MAX_COUNT_EACH_FETCH

        try:
            review_items, token = _fetch_review_items(
                url,
                app_id,
                sort,
                _fetch_count,
                filter_score_with,
                filter_device_with,
                token,
            )
        except Exception as e:
            logging.error(f"Error fetching reviews for app {app_id}: {e}")
            token = None
            break

        for review in review_items:
            review_data = {
                k: spec.extract_content(review)
                for k, spec in ElementSpecs.Review.items()
            }
            result.append(review_data)

        _fetch_count = count - len(result)

        if isinstance(token, list):
            token = None
            break
        if token is None:
            break

    return (
        result,
        _ContinuationToken(
            token, lang, country, sort, count, filter_score_with, filter_device_with
        ),
    )


def reviews_all(app_id: str, sleep_milliseconds: int = 0, **kwargs) -> list:
    kwargs.pop("count", None)
    kwargs.pop("continuation_token", None)

    continuation_token = None

    result = []

    while True:
        _result, continuation_token = reviews(
            app_id,
            count=MAX_COUNT_EACH_FETCH,
            continuation_token=continuation_token,
            **kwargs
        )

        result += _result

        if continuation_token.token is None:
            break

        if sleep_milliseconds:
            sleep(sleep_milliseconds / 1000)

    # Print the total number of reviews fetched
    print(f"Total number of reviews fetched: {len(result)}")

    return result


def save_reviews_to_csv(reviews: List[dict], output_file: str):
    df = pd.DataFrame(reviews)
    df.to_csv(output_file, index=False)
    logging.info(f"Saved {len(reviews)} reviews to {output_file}")


def fetch_reviews_for_app(app_id: str, title: str) -> List[dict]:
    logging.info(f"Fetching reviews for: {app_id} - {title}")
    try:
        reviews_result = reviews_all(app_id=app_id, lang='en', country='us')
        for review in reviews_result:
            review["title"] = title
        return reviews_result
    except Exception as e:
        logging.error(f"Error fetching reviews for app {app_id}: {e}")
        return []


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fine-tune GPT-2 on the KP20k dataset and evaluate the model."
    )
    parser.add_argument("--batch_size", type=int, default=10, help="Batch size for fetching reviews")
    parser.add_argument("--output_dir", type=str, default="./output", help="Output directory for CSV files")
    parser.add_argument("--input_file", type=str, default="./csv/merged_csv_20240617-1.csv",
                        help="Input Excel file with app IDs")
    parser.add_argument("--threads", type=int,
                        help="Number of threads to use for fetching reviews", default=5)
    args = parser.parse_args()
    # Read app_id from Excel sheet
    excel_file = args.input_file
    df_app_ids = pd.read_csv(excel_file)

    batch_size = args.batch_size
    batch_count = 0

    # Assuming the app IDs and game titles are in columns named 'appId' and 'title'
    for batch_start in range(0, len(df_app_ids), batch_size):
        batch_end = min(batch_start + batch_size, len(df_app_ids))
        batch_app_ids = df_app_ids.iloc[batch_start:batch_end]

        all_reviews = []  # Reset for each batch

        # Use ThreadPoolExecutor for parallel fetching
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            futures = {
                executor.submit(fetch_reviews_for_app, row.appId, row.title): row
                for row in batch_app_ids.itertuples(index=False)
            }
            for future in as_completed(futures):
                app_reviews = future.result()
                all_reviews.extend(app_reviews)

        batch_count += 1
        # Define output CSV file name for the current batch
        output_csv_file = f"{args.output_dir}/all_app_reviews_batch_{batch_count}.csv"

        # Save current batch of reviews to a CSV
        save_reviews_to_csv(all_reviews, output_csv_file)

    logging.info("All batches processed.")
