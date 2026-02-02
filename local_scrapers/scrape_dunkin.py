# local_scrapers/scrape_dunkin.py

"""
Dunkin Menu Scraper using Selenium
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import json
import time
from datetime import datetime
import os
from typing import List, Dict
import re
from urllib.parse import urlparse


class DunkinScraper:
    """Scraper for Dunkin menu data"""
    
    def __init__(self, headless=True):
        """Initialize scraper"""
        
        print("🔧 Initializing Dunkin scraper...")
        
        chrome_options = Options()
        env_headless = os.getenv("DUNKIN_HEADLESS", "true").lower() != "false"
        if headless and env_headless:
            chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')
        chrome_options.add_argument('--window-size=1440,900')
        chrome_options.set_capability("goog:loggingPrefs", {"performance": "ALL"})
        
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)
        try:
            self.driver.execute_cdp_cmd("Network.enable", {})
        except Exception:
            pass
        
        self.base_url = "https://www.dunkindonuts.com"
        self.drinks = []
    
    def scrape_all(self) -> List[Dict]:
        """Scrape Dunkin menu"""
        
        print("🚀 Starting Dunkin scraping...")

        categories = self.get_menu_categories()
        for category in categories:
            category_drinks = self.scrape_category(category)
            self.drinks.extend(category_drinks)
            print(f"  ✅ Total drinks so far: {len(self.drinks)}")

        print(f"🎉 Scraping complete! Total drinks: {len(self.drinks)}")
        return self.drinks
    
    def close(self):
        """Close browser"""
        self.driver.quit()

    def get_menu_categories(self) -> List[Dict[str, str]]:
        """Get menu category links from Dunkin menu page"""
        print("📋 Fetching Dunkin menu categories...")
        self.driver.get(f"{self.base_url}/en/menu")
        self.wait_for_page_ready()

        categories = []
        try:
            anchors = self.driver.find_elements(By.CSS_SELECTOR, "a[href]")
        except Exception:
            anchors = []

        seen = set()
        for anchor in anchors:
            href = anchor.get_attribute("href") or ""
            if "/en/menu/" not in href:
                continue
            path = urlparse(href).path
            segments = [s for s in path.split("/") if s]
            # category paths look like /en/menu/coffee
            if len(segments) == 3 and segments[1] == "menu":
                if path not in seen and not path.endswith("/menu"):
                    seen.add(path)
                    name = anchor.text.strip() or segments[-1].replace("-", " ").title()
                    categories.append({"name": name, "url": path})

        # Fallback to known categories if none found
        if not categories:
            categories = [
                {"name": "Coffee", "url": "/en/menu/coffee"},
                {"name": "Espresso", "url": "/en/menu/espresso"},
                {"name": "Iced Coffee", "url": "/en/menu/iced-coffee"},
                {"name": "Cold Brew", "url": "/en/menu/cold-brew"},
                {"name": "Tea", "url": "/en/menu/tea"},
                {"name": "Frozen Drinks", "url": "/en/menu/frozen-drinks"},
            ]

        print(f"✅ Found {len(categories)} categories")
        return categories

    def scrape_category(self, category: Dict[str, str]) -> List[Dict]:
        """Scrape all drinks from a category"""
        print(f"\n☕ Scraping category: {category['name']}")
        url = f"{self.base_url}{category['url']}"
        self.driver.get(url)
        self.wait_for_page_ready()
        self.scroll_to_bottom()

        drink_links = self.collect_drink_links()
        print(f"  Found {len(drink_links)} drinks in {category['name']}")

        category_drinks = []
        for drink_url in drink_links:
            try:
                drink_data = self.scrape_drink_detail(drink_url, category["name"])
                if drink_data:
                    category_drinks.append(drink_data)
                time.sleep(1)
            except Exception as e:
                print(f"    ⚠️ Error scraping {drink_url}: {e}")
                continue

        return category_drinks

    def scrape_drink_detail(self, drink_url: str, category: str) -> Dict:
        """Scrape detailed information for a single drink"""
        full_url = f"{self.base_url}{drink_url}" if not drink_url.startswith("http") else drink_url
        self.driver.get(full_url)
        self.wait_for_page_ready()
        time.sleep(1)

        soup = BeautifulSoup(self.driver.page_source, "html.parser")
        name = self.extract_text(soup, "h1") or self.extract_text(soup, "h2")
        description = self.extract_meta_description(soup)

        drink_data = {
            "name": name,
            "category": category,
            "description": description,
            "url": full_url,
            "scraped_date": datetime.now().isoformat(),
        }

        drink_data["sizes"] = self.extract_sizes(soup)
        drink_data["customizations"] = self.default_customizations()
        drink_data["temperature"] = self.infer_temperature(category, name)
        drink_data["has_caffeine"] = self.infer_caffeine(name, description)
        drink_data["is_seasonal"] = self.is_seasonal(name)

        return drink_data

    def collect_drink_links(self) -> List[str]:
        """Collect drink links using DOM, page source, and network logs"""
        links = set()
        try:
            anchors = self.driver.find_elements(By.CSS_SELECTOR, "a[href]")
        except Exception:
            anchors = []

        for anchor in anchors:
            href = anchor.get_attribute("href") or ""
            if "/en/menu/" not in href:
                continue
            path = urlparse(href).path
            segments = [s for s in path.split("/") if s]
            # item paths usually have /en/menu/{category}/{item}
            if len(segments) >= 4 and segments[1] == "menu":
                links.add(path)

        if not links:
            try:
                html = self.driver.page_source or ""
                for match in re.findall(r"/en/menu/[a-z0-9\\-]+/[a-z0-9\\-]+", html, re.IGNORECASE):
                    links.add(match)
            except Exception:
                pass

        if not links:
            links.update(self.collect_links_from_network())

        return sorted(links)

    def collect_links_from_network(self) -> List[str]:
        """Inspect network responses for menu item links"""
        found = set()
        try:
            perf_logs = self.driver.get_log("performance")
        except Exception:
            perf_logs = []

        for entry in perf_logs:
            try:
                message = json.loads(entry.get("message", "{}")).get("message", {})
                if message.get("method") != "Network.responseReceived":
                    continue
                params = message.get("params", {})
                response = params.get("response", {})
                mime_type = response.get("mimeType", "")
                if "application/json" not in mime_type:
                    continue
                request_id = params.get("requestId")
                if not request_id:
                    continue
                body = self.driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
                text = body.get("body", "")
            except Exception:
                continue

            for match in re.findall(r"/en/menu/[a-z0-9\\-]+/[a-z0-9\\-]+", text, re.IGNORECASE):
                found.add(match)

            if not found:
                for url_match in re.findall(r"\"url\"\\s*:\\s*\"(/en/menu/[a-z0-9\\-]+/[a-z0-9\\-]+)\"", text, re.IGNORECASE):
                    found.add(url_match)

        return sorted(found)

    def wait_for_page_ready(self):
        """Wait for the page to finish loading"""
        try:
            self.wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        except Exception:
            time.sleep(2)

    def scroll_to_bottom(self):
        """Scroll page to load all dynamic content"""
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def extract_text(self, soup, tag, class_name=None, multiple=False):
        """Extract text from HTML element"""
        try:
            if multiple:
                elements = soup.find_all(tag, class_=class_name) if class_name else soup.find_all(tag)
                return " ".join([el.get_text(strip=True) for el in elements[:3]])
            element = soup.find(tag, class_=class_name) if class_name else soup.find(tag)
            return element.get_text(strip=True) if element else ""
        except Exception:
            return ""

    def extract_meta_description(self, soup) -> str:
        """Extract meta description if available"""
        try:
            meta = soup.find("meta", attrs={"name": "description"})
            if meta and meta.get("content"):
                return meta.get("content", "").strip()
        except Exception:
            pass
        return ""

    def extract_sizes(self, soup) -> List[str]:
        """Extract sizes from text"""
        sizes = []
        size_keywords = ["Small", "Medium", "Large"]
        text = soup.get_text(" ", strip=True)
        for size in size_keywords:
            if size.lower() in text.lower():
                sizes.append(size)
        return sizes if sizes else ["Small", "Medium", "Large"]

    def default_customizations(self) -> Dict:
        """Default customization options"""
        return {
            "milk_options": ["Whole Milk", "Skim Milk", "Oat Milk", "Almond Milk"],
            "flavors": ["French Vanilla", "Hazelnut", "Caramel", "Mocha"],
            "sweeteners": ["Sugar", "Splenda", "Equal"],
        }

    def infer_temperature(self, category: str, name: str) -> str:
        """Infer drink temperature from category and name"""
        text = f"{category} {name}".lower()
        if "iced" in text or "cold" in text:
            return "cold"
        if "frozen" in text or "coolatta" in text:
            return "frozen"
        return "hot"

    def infer_caffeine(self, name: str, description: str) -> bool:
        """Infer if drink contains caffeine"""
        text = (name + " " + description).lower()
        if "decaf" in text:
            return False
        if any(word in text for word in ["tea", "coffee", "espresso", "latte", "cold brew"]):
            return True
        return True

    def is_seasonal(self, name: str) -> bool:
        """Check if drink is seasonal"""
        seasonal_keywords = ["pumpkin", "peppermint", "gingerbread", "holiday", "winter", "fall", "summer"]
        name_lower = name.lower()
        return any(keyword in name_lower for keyword in seasonal_keywords)


def save_to_json(drinks: List[Dict], cafe_name: str) -> str:
    """Save to JSON"""
    date_str = datetime.now().strftime('%Y-%m-%d')
    output_dir = f'data/raw/{cafe_name}'
    os.makedirs(output_dir, exist_ok=True)
    
    filename = f'{output_dir}/{date_str}.json'
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(drinks, f, indent=2, ensure_ascii=False)
    
    print(f"💾 Saved {len(drinks)} drinks to {filename}")
    return filename


def main():
    """Main execution"""
    scraper = DunkinScraper(headless=True)
    
    try:
        drinks = scraper.scrape_all()
        if drinks:
            save_to_json(drinks, 'dunkin')
    finally:
        scraper.close()
        print("✨ Done!")


if __name__ == "__main__":
    main()