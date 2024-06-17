import json
import time
import pandas as pd
from typing import Any, Dict, List
from urllib.parse import quote
from urllib.error import HTTPError
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from google_play_scraper.constants.element import ElementSpecs
from google_play_scraper.constants.regex import Regex
from google_play_scraper.constants.request import Formats
from google_play_scraper.utils.request import get
from google_play_scraper.features import search, reviews

class GameScraper:
    def __init__(self):
        self.types = ['action/id7001', 'adventure/id7002', 'board/id7004', 'card/id7005', 'casino/id7006', 'casual/id7003',
                      'dice/id7007', 'educational/id7008', 'family/id7009', 'music/id7011', 'puzzle/id7012', 'racing/id7013', 'role-playing/id7014', 'simulation/id7015', 'sports/id7016',
                      'strategy/id7017', 'trivia/id7018', 'word/id7019']
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run headless Chrome
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        self.driver = webdriver.Chrome(options=chrome_options)
        self.base_url = "https://apps.apple.com/us/genre/ios-games-"

    def scrape_games(self, limit: int = 10) -> List[str]:
        game_names = []
        for type in self.types:
            self.driver.get(self.base_url + type)
            time.sleep(3)  # Wait for the page to load
            game_names_column_first = self.driver.find_elements(By.XPATH, ".//div[@class = 'column first']/ul/li")
            for name in game_names_column_first:
                game_names.append(name.text)
                if len(game_names) >= limit:
                    break
            if len(game_names) < limit:
                game_names_column = self.driver.find_elements(By.XPATH, ".//div[@class = 'column']/ul/li")
                for name in game_names_column:
                    game_names.append(name.text)
                    if len(game_names) >= limit:
                        break
            if len(game_names) < limit:
                game_names_column_last = self.driver.find_elements(By.XPATH, ".//div[@class = 'column last']/ul/li")
                for name in game_names_column_last:
                    game_names.append(name.text)
                    if len(game_names) >= limit:
                        break
            if len(game_names) >= limit:
                break
        self.driver.quit()
        return game_names

def search(query: str, n_hits: int = 30, lang: str = "en", country: str = "us") -> List[Dict[str, Any]]:
    if n_hits <= 0:
        return []

    query = quote(query)
    url = Formats.Searchresults.build(query=query, lang=lang, country=country)

    retries = 3
    for _ in range(retries):
        try:
            dom = get(url)
            break
        except HTTPError as e:
            if e.code == 429:
                print("Rate limited. Retrying in 60 seconds...")
                time.sleep(60)
            else:
                raise e
    else:
        print("Failed to retrieve data after retries.")
        return []

    matches = Regex.SCRIPT.findall(dom)  # take out script blocks from dom

    dataset = {}

    for match in matches:
        key_match = Regex.KEY.findall(match)
        value_match = Regex.VALUE.findall(match)

        if key_match and value_match:
            key = key_match[0]
            value = json.loads(value_match[0])
            dataset[key] = value

    try:
        top_result = dataset["ds:4"][0][1][0][23][16]
    except (IndexError, KeyError, TypeError):
        top_result = None

    success = False
    data_index = None
    for idx in range(len(dataset.get("ds:4", [[]])[0][1])):
        try:
            dataset = dataset["ds:4"][0][1][idx][22][0]
            success = True
            data_index = idx
            break
        except (IndexError, KeyError, TypeError):
            pass

    if not success or not dataset:
        return []

    n_apps = min(len(dataset), n_hits)

    search_results = (
        [
            {
                k: spec.extract_content(top_result)
                for k, spec in ElementSpecs.SearchResultOnTop.items()
            }
        ] if top_result else []
    )

    for app_idx in range(n_apps - len(search_results)):
        app = {}
        for k, spec in ElementSpecs.SearchResult.items():
            content = spec.extract_content(dataset[app_idx])
            app[k] = content

        search_results.append(app)

    return search_results

def extract_info_from_search_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    extracted_data = []
    for result in results:
        app_info = {
            "appId": result.get("appId"),
            "title": result.get("title"),
            "score": result.get("score"),
            "genre": result.get("genre"),
            "price": result.get("price"),
            "currency": result.get("currency"),
            "description": result.get("description"),
            "developer": result.get("developer"),
            "installs": result.get("installs")
        }
        extracted_data.append(app_info)
    return extracted_data

def save_to_csv(data: List[Dict[str, Any]], filename: str):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)

def main():
    # Initialize the GameScraper class
    scraper = GameScraper()

    # Scrape game names with a limit of 3000
    game_names = scraper.scrape_games(limit=3000)
    print(f"Scraped game names: {game_names}")

    batch_size = 100
    for i in range(0, len(game_names), batch_size):
        batch = game_names[i:i+batch_size]
        all_search_results = []

        for game_name in batch:
            search_results = search(game_name, n_hits=30)
            extracted_data = extract_info_from_search_results(search_results)
            all_search_results.extend(extracted_data)
            # Delay to avoid rate limiting
            time.sleep(5)

        # Save each batch to a CSV file
        batch_filename = f"game_search_results_batch_{i//batch_size + 1}.csv"
        save_to_csv(all_search_results, batch_filename)
        print(f"Batch {i//batch_size + 1} saved to '{batch_filename}'")

if __name__ == "__main__":
    main()
