# local_scrapers/scrape_starbucks.py

"""
Comprehensive Starbucks Menu Scraper using Selenium
Targets 100-200 drinks across all categories
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
from tqdm import tqdm
import re


class StarbucksScraper:
    """Scraper for Starbucks menu data"""
    
    def __init__(self, headless=True):
        """Initialize scraper with Selenium driver"""
        
        print("🔧 Initializing Starbucks scraper...")
        
        chrome_options = Options()
        env_headless = os.getenv("STARBUCKS_HEADLESS", "true").lower() != "false"
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
        
        self.base_url = "https://www.starbucks.com"
        self.drinks = []
        
    def get_menu_categories(self) -> List[Dict[str, str]]:
        """Get all drink categories from main menu"""
        
        print("📋 Fetching menu categories...")
        self.driver.get(f"{self.base_url}/menu")
        time.sleep(3)
        
        categories = [
            {'name': 'Hot Coffee', 'url': '/menu/drinks/hot-coffee'},
            {'name': 'Hot Tea', 'url': '/menu/drinks/hot-tea'},
            {'name': 'Hot Drink', 'url': '/menu/drinks/hot-drink'},
            {'name': 'Frappuccino', 'url': '/menu/drinks/frappuccino-blended-beverage'},
            {'name': 'Cold Coffee', 'url': '/menu/drinks/cold-coffee'},
            {'name': 'Iced Tea', 'url': '/menu/drinks/iced-tea'},
            {'name': 'Cold Drink', 'url': '/menu/drinks/cold-drink'},
        ]
        
        print(f"✅ Found {len(categories)} categories")
        return categories
    
    def scrape_category(self, category: Dict[str, str]) -> List[Dict]:
        """Scrape all drinks from a category"""
        
        print(f"\n☕ Scraping category: {category['name']}")
        
        url = f"{self.base_url}{category['url']}"
        self.driver.get(url)
        self.wait_for_page_ready()
        
        # Scroll to load all items
        self.scroll_to_bottom()
        
        drink_links = self.collect_drink_links()
        
        print(f"  Found {len(drink_links)} drinks in {category['name']}")
        
        category_drinks = []
        for drink_url in tqdm(drink_links, desc=f"  Scraping {category['name']}"):
            try:
                drink_data = self.scrape_drink_detail(drink_url, category['name'])
                if drink_data:
                    category_drinks.append(drink_data)
                time.sleep(1)  # Be polite to the server
            except Exception as e:
                print(f"    ⚠️ Error scraping {drink_url}: {e}")
                continue
        
        return category_drinks
    
    def scrape_drink_detail(self, drink_url: str, category: str) -> Dict:
        """Scrape detailed information for a single drink"""
        
        full_url = f"{self.base_url}{drink_url}" if not drink_url.startswith('http') else drink_url
        
        self.driver.get(full_url)
        time.sleep(2)
        
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        
        # Extract drink information (selectors may need adjustment based on actual site)
        drink_data = {
            'name': self.extract_text(soup, 'h1'),
            'category': category,
            'description': self.extract_text(soup, 'p', multiple=True),
            'url': full_url,
            'scraped_date': datetime.now().isoformat()
        }
        
        # Try to extract sizes and calories
        drink_data['sizes'] = self.extract_sizes(soup)
        drink_data['nutrition'] = self.extract_nutrition(soup)
        
        # Extract customization options
        drink_data['customizations'] = self.extract_customizations(soup)
        
        # Infer additional attributes
        drink_data['temperature'] = self.infer_temperature(category, drink_data['name'])
        drink_data['has_caffeine'] = self.infer_caffeine(drink_data['name'], drink_data['description'])
        drink_data['is_seasonal'] = self.is_seasonal(drink_data['name'])
        
        return drink_data
    
    def extract_text(self, soup, tag, class_name=None, multiple=False):
        """Extract text from HTML element"""
        try:
            if multiple:
                elements = soup.find_all(tag, class_=class_name) if class_name else soup.find_all(tag)
                return ' '.join([el.get_text(strip=True) for el in elements[:3]])  # First 3 paragraphs
            else:
                element = soup.find(tag, class_=class_name) if class_name else soup.find(tag)
                return element.get_text(strip=True) if element else ''
        except:
            return ''
    
    def extract_sizes(self, soup) -> List[str]:
        """Extract available sizes"""
        sizes = []
        size_keywords = ['Short', 'Tall', 'Grande', 'Venti', 'Trenta']
        
        text = soup.get_text()
        for size in size_keywords:
            if size.lower() in text.lower():
                sizes.append(size)
        
        return sizes if sizes else ['Tall', 'Grande', 'Venti']  # Default
    
    def extract_nutrition(self, soup) -> Dict:
        """Extract nutritional information"""
        nutrition = {}
        
        # Look for calorie information
        text = soup.get_text()
        
        # Simple regex-like extraction (improve as needed)
        if 'calorie' in text.lower():
            # Extract numbers near 'calorie'
            import re
            cal_match = re.search(r'(\d+)\s*calorie', text, re.IGNORECASE)
            if cal_match:
                nutrition['calories'] = cal_match.group(1)
        
        return nutrition
    
    def extract_customizations(self, soup) -> Dict:
        """Extract customization options"""
        
        customizations = {
            'milk_options': ['Whole Milk', '2% Milk', 'Nonfat Milk', 'Oat Milk', 
                           'Almond Milk', 'Coconut Milk', 'Soy Milk'],
            'espresso_shots': ['1', '2', '3', '4'],
            'syrups': ['Vanilla', 'Caramel', 'Hazelnut', 'Mocha', 'Sugar Free Vanilla'],
            'toppings': ['Whipped Cream', 'Caramel Drizzle', 'Chocolate Drizzle']
        }
        
        # Could enhance this by parsing actual customization options from page
        
        return customizations
    
    def infer_temperature(self, category: str, name: str) -> str:
        """Infer drink temperature from category and name"""
        name_lower = name.lower()
        category_lower = category.lower()
        
        if 'iced' in name_lower or 'cold' in category_lower:
            return 'cold'
        elif 'frappuccino' in category_lower or 'frappuccino' in name_lower:
            return 'frozen'
        elif 'hot' in category_lower:
            return 'hot'
        else:
            return 'hot'  # default
    
    def infer_caffeine(self, name: str, description: str) -> bool:
        """Infer if drink contains caffeine"""
        text = (name + ' ' + description).lower()
        
        if 'decaf' in text:
            return False
        if any(word in text for word in ['tea', 'herbal', 'rooibos']):
            # Some teas don't have caffeine
            return 'herbal' not in text and 'rooibos' not in text
        if any(word in text for word in ['coffee', 'espresso', 'americano', 'latte', 'mocha']):
            return True
        
        return True  # default assumption
    
    def is_seasonal(self, name: str) -> bool:
        """Check if drink is seasonal"""
        seasonal_keywords = [
            'pumpkin', 'peppermint', 'gingerbread', 'eggnog',
            'holiday', 'christmas', 'fall', 'winter', 'summer'
        ]
        name_lower = name.lower()
        return any(keyword in name_lower for keyword in seasonal_keywords)
    
    def scroll_to_bottom(self):
        """Scroll page to load all dynamic content"""
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def wait_for_page_ready(self):
        """Wait for the page to finish loading"""
        try:
            self.wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        except Exception:
            time.sleep(2)

    def collect_drink_links(self) -> List[str]:
        """Collect drink links using DOM and bootstrapped state"""
        links = set()

        # First attempt: DOM anchors with expected menu/product paths
        try:
            anchors = self.driver.find_elements(By.CSS_SELECTOR, "a[href]")
            for anchor in anchors:
                href = anchor.get_attribute("href") or ""
                if "/menu/product/" in href:
                    links.add(href.replace(self.base_url, ""))
        except Exception:
            pass

        # Fallback: regex against full page source
        if not links:
            try:
                html = self.driver.page_source or ""
                for match in re.findall(r"/menu/product/[a-zA-Z0-9\\-]+", html):
                    links.add(match)
            except Exception:
                pass

        # Fallback: look for product links inside bootstrapped state JSON
        if not links:
            state_names = [
                "__BOOTSTRAP",
                "__PRELOADED_STATE__",
                "__INITIAL_STATE__",
                "__APOLLO_STATE__",
            ]
            for state_name in state_names:
                try:
                    state_json = self.driver.execute_script(
                        f"return window.{state_name} ? JSON.stringify(window.{state_name}) : null;"
                    )
                except Exception:
                    state_json = None

                if not state_json:
                    continue

                for match in re.findall(r"/menu/product/[a-zA-Z0-9\\-]+", state_json):
                    links.add(match)

                if links:
                    break

        # Last resort: scan network responses for product links
        if not links:
            links.update(self.collect_links_from_network())

        return sorted(links)

    def collect_links_from_network(self) -> List[str]:
        """Inspect network responses for product links"""
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

            for match in re.findall(r"/menu/product/[a-zA-Z0-9\\-]+", text):
                found.add(match)

            if not found:
                for product_number, form_code in re.findall(
                    r"\"productNumber\"\\s*:\\s*\"([a-zA-Z0-9\\-]+)\".*?\"formCode\"\\s*:\\s*\"([a-zA-Z0-9\\-]+)\"",
                    text,
                    re.DOTALL,
                ):
                    found.add(f"/menu/product/{product_number}/{form_code}")

        return sorted(found)
    
    def scrape_all(self) -> List[Dict]:
        """Scrape all drinks from all categories"""
        
        print("\n🚀 Starting comprehensive Starbucks scraping...")

        self.select_store_if_needed()
        
        categories = self.get_menu_categories()
        
        for category in categories:
            category_drinks = self.scrape_category(category)
            self.drinks.extend(category_drinks)
            print(f"  ✅ Total drinks so far: {len(self.drinks)}")
        
        print(f"\n🎉 Scraping complete! Total drinks: {len(self.drinks)}")
        return self.drinks
    
    def close(self):
        """Close the browser"""
        self.driver.quit()

    def select_store_if_needed(self):
        """Select a store for menu availability if prompted"""
        print("🏬 Ensuring store is selected...")
        try:
            self.driver.get(f"{self.base_url}/menu")
            self.wait_for_page_ready()
        except Exception:
            return

        try:
            self.wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "[data-e2e='select-store-from-crust'], a[href*='store-locator']")
                )
            )
        except Exception:
            return

        query = os.getenv("STARBUCKS_STORE_QUERY", "98109")

        # Try clicking the store selector link
        try:
            store_link = self.driver.find_element(By.CSS_SELECTOR, "[data-e2e='select-store-from-crust']")
        except Exception:
            store_link = None

        if store_link:
            try:
                store_link.click()
            except Exception:
                pass
        else:
            try:
                self.driver.get(f"{self.base_url}/store-locator?source=menu")
                self.wait_for_page_ready()
            except Exception:
                return

        search_input = None
        search_selectors = [
            "input[type='search']",
            "input[placeholder*='ZIP']",
            "input[placeholder*='Zip']",
            "input[placeholder*='Search']",
            "input[aria-label*='store']",
            "input[aria-label*='location']",
        ]
        for selector in search_selectors:
            try:
                search_input = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                if search_input:
                    break
            except Exception:
                continue

        if not search_input:
            return

        try:
            search_input.clear()
            search_input.send_keys(query)
            search_input.send_keys("\n")
        except Exception:
            return

        # Try to select the first store result
        try:
            self.wait.until(
                EC.presence_of_element_located(
                    (By.XPATH, "//button[contains(., 'Select') or contains(., 'Order')]")
                )
            )
        except Exception:
            return

        try:
            buttons = self.driver.find_elements(By.XPATH, "//button[contains(., 'Select') or contains(., 'Order')]")
            for button in buttons:
                if button.is_displayed() and button.is_enabled():
                    button.click()
                    print(f"✅ Store selected for query: {query}")
                    self.debug_store_state()
                    return
        except Exception:
            return

    def debug_store_state(self):
        """Log current URL and store cookies if present"""
        try:
            current_url = self.driver.current_url
            cookies = {c.get("name"): c.get("value") for c in self.driver.get_cookies()}
            store_cookie = {k: v for k, v in cookies.items() if "store" in k.lower()}
            print(f"🔎 Current URL after store selection: {current_url}")
            if store_cookie:
                print(f"🔎 Store cookies: {store_cookie}")
        except Exception:
            pass


def save_to_json(drinks: List[Dict], cafe_name: str) -> str:
    """Save drinks data to JSON file"""
    
    date_str = datetime.now().strftime('%Y-%m-%d')
    output_dir = f'data/raw/{cafe_name}'
    
    os.makedirs(output_dir, exist_ok=True)
    
    filename = f'{output_dir}/{date_str}.json'
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(drinks, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 Saved {len(drinks)} drinks to {filename}")
    return filename


def main():
    """Main execution"""
    
    scraper = StarbucksScraper(headless=True)
    
    try:
        drinks = scraper.scrape_all()
        
        if drinks:
            save_to_json(drinks, 'starbucks')
            
            # Print summary statistics
            print("\n📊 Summary Statistics:")
            print(f"  Total drinks: {len(drinks)}")
            
            categories = {}
            for drink in drinks:
                cat = drink.get('category', 'Unknown')
                categories[cat] = categories.get(cat, 0) + 1
            
            for cat, count in categories.items():
                print(f"  {cat}: {count} drinks")
            
        else:
            print("⚠️ No drinks were scraped!")
        
    finally:
        scraper.close()
        print("\n✨ Done!")


if __name__ == "__main__":
    main()