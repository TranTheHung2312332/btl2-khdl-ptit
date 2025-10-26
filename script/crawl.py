import pandas as pd
import time
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from tqdm import tqdm
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys

BASE_URL = "https://oto.com.vn"
PROVINCES = ["da-nang", "ha-noi", "hcm", "hai-phong", "bac-ninh", "thanh-hoa", "nghe-an", "binh-duong"]

TARGET_FILE = "../data/raw"

USER_AGENT = "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

class OtoCrawlerPERFECT:
    def __init__(self, path):
        self.data = []
        self.base_url = BASE_URL
        self.path = path
        self.setup_driver()

    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(USER_AGENT)
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--headless")

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.wait = WebDriverWait(self.driver, 10)

    def extract_car_info(self, url):
        try:
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            r = requests.get(url, headers=headers, timeout=10)

            soup = BeautifulSoup(r.content.decode('utf-8', errors='ignore'), "html.parser")

            list_infos = soup.findAll('ul', class_='list-info')

            infos = []
            for list_info in list_infos:
                for info in list_info.findAll('li'):
                    info.find('label').extract()
                    infos.append(info.get_text(strip=True))
            infos_len = len(infos)
            data = {
                'url': url,
                'ten_san_pham': soup.find('h1', class_='title-detail').get_text(strip=True),
                'ngay_dang': soup.find(class_='date').get_text(strip=True),
                'nam_sx': int(infos[0]),
                'nhien_lieu': infos[1],
                'kieu_dang': infos[2],
                'tinh_trang': infos[3],
                'so_km': infos[4] if infos_len == 8 else '0km',
                'hop_so': infos[5] if infos_len == 8 else infos[4],
                'xuat_xu': infos[6] if infos_len == 8 else infos[5],
                'dia_diem': infos[7] if infos_len == 8 else infos[6],
                'gia_ban': soup.find(class_='price').get_text(strip=True)
            }

            return data

        except:
            return None

    def get_car_links(self):
        links = []
        selector = '.box-list-car .item-car .info .info-left .title a'
        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
        for elem in elements:
            href = elem.get_attribute('href')
            if href:
                links.append(urljoin(self.base_url, href))

        return links

    def load_first_page(self):
        self.driver.get(BASE_URL + self.path)
        time.sleep(1)
        for _ in range(3):
            try:
                btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//*[contains(text(),'Hiển thị thêm')]")))
                btn.click()
                time.sleep(0.2)
            except:
                break
        return self.get_car_links()

    def load_page(self, page_num):
        url = f"{self.base_url}{self.path}/p{page_num}"
        self.driver.get(url)
        time.sleep(0.2)
        return self.get_car_links()

    def crawl(self, max_samples=10000):
        print(self.base_url + self.path)
        all_links = set(self.load_first_page())

        page = 2
        while len(all_links) < max_samples:
            page_links = self.load_page(page)
            old_len = len(all_links)
            all_links.update(page_links)
            new_len = len(all_links)
            if old_len == new_len and page >= 3:
                break
            page += 1
            sys.stdout.write(f"\r{self.path}: Crawled {len(all_links)} links")
            sys.stdout.flush()
        print("\n")

        all_links = list(all_links)[:max_samples]

        results = []
        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = [executor.submit(self.extract_car_info, link) for link in all_links]
            for f in tqdm(as_completed(futures), total=len(futures)):
                try:
                    result = f.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    print(f"Error processing: {e}")

        self.data.extend(results)


    def save_final(self, filename=TARGET_FILE):
        df = pd.DataFrame(self.data)
        df.to_csv(filename + '.csv', index=False, encoding='utf-8', mode='a')



crawler = OtoCrawlerPERFECT(path=None)
for province in PROVINCES:
    crawler.path = f"/mua-ban-xe-{province}"
    crawler.crawl()

    crawler.path = f"/mua-ban-xe-{province}/f167772165555"
    crawler.crawl()

crawler.save_final()
