"""
scraper.py - Собирает данные с 100+ страниц манги с Remanga.org
Использует headless Selenium и точный парсинг структуры HTML
"""
from selenium.webdriver import Remote
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
import re
from datetime import datetime
import random
from bs4 import BeautifulSoup


def setup_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    return Remote(
        command_executor="http://selenium:4444/wd/hub",
        options=options
    )

def get_manga_list(driver):
    """Получаем список популярных манг для парсинга"""
    print("Получаем список популярной манги...")
    
    driver.get("https://remanga.org/manga")
    time.sleep(3)
    
    # Прокручиваем для загрузки
    for i in range(5):
        driver.execute_script(f"window.scrollTo(0, {1000 * (i+1)});")
        time.sleep(2)
    
    # Ждем ссылки на мангу
    WebDriverWait(driver, 10).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'a[href*="/manga/"]'))
    )
    
    manga_links = []
    all_links = driver.find_elements(By.TAG_NAME, "a")
    
    for link in all_links:
        try:
            href = link.get_attribute("href")
            if href and "/manga/" in href and "/main" in href:
                if href not in manga_links:
                    manga_links.append(href)
        except:
            continue
    
    return manga_links[:120]


def parse_manga_page(driver, url):
    """Парсинг одной страницы манги"""
    try:
        driver.get(url)
        time.sleep(2.5)
        
        html = driver.page_source
        
        data = {
            "title": clean_title(get_title(driver)),
            "description": get_description(driver),
            "year": get_year(html),
            "rating": get_rating(driver),
            "cover_url": get_cover_image(driver),
            "url": url,
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        print(f"{data['title'][:30]}... | Рейтинг: {data['rating']}")
        return data
        
    except Exception as e:
        print(f"Ошибка с {url}: {str(e)[:50]}")
        return None


def clean_title(title):
    """Очистка заголовка"""
    if title.startswith("Читать "):
        title = title[7:]
    return title.strip()


def get_title(driver):
    """Извлекаем заголовок"""
    try:
        return driver.find_element(By.TAG_NAME, "h1").text.strip()
    except:
        return driver.title.split("—")[0].strip()


def get_description(driver):
    """Ищем описание - теперь только из правильных элементов"""
    try:
        try:
            desc_elements = driver.find_elements(By.CSS_SELECTOR, '[data-sentry-component="Description"]')
            for elem in desc_elements:
                paragraphs = elem.find_elements(By.TAG_NAME, "p")
                for p in paragraphs:
                    text = p.text.strip()
                    if text and len(text) > 50:
                        return text[:500]
        except:
            pass
        
    except Exception as e:
        print(f"Ошибка поиска описания: {e}")
        return ""


def get_year(html):
    """Извлекает год из HTML с использованием BeautifulSoup и точных селекторов"""
    soup = BeautifulSoup(html, 'html.parser')

    year_link = soup.find('a', href=lambda x: x and 'issue_year' in str(x))
    if year_link:
        year_text = year_link.get_text(strip=True)

        if year_text.isdigit() and len(year_text) == 4:
            return year_text
    
    return ""


def get_rating(driver):
    """Извлекает рейтинг используя несколько возможных структур"""
    try:
        stat_heading = driver.find_element(
            By.XPATH, 
            "//h3[contains(text(), 'Статистика')]"
        )
        
        stat_container = stat_heading.find_element(By.XPATH, "../..")
        
        rating_elements = stat_container.find_elements(
            By.XPATH, 
            ".//*[contains(text(), 'Рейтинг за последнее время:')]"
        )
            
        for elem in rating_elements:
            text = elem.text.strip()
        
            match = re.search(r'(\d+\.\d+)', text)
            if match:
                rating = float(match.group(1))
                if 0.0 <= rating <= 10.0:
                    return match.group(1)
                    
    except Exception as e:
        print(f"Ошибка при получении рейтинга: {str(e)[:100]}")
        return "0.0"


def get_cover_image(driver):
    try:
        # Ищем изображение с определенными классами
        img_element = driver.find_element(
            By.CSS_SELECTOR, 
            'img[data-sentry-component="MediaOptimizedImage"]'
        )
        
        img_url = img_element.get_attribute("src")
        return img_url if img_url else ""
    except:
        return ""


def scrape_manga_data():
    """Основная функция скрапинга"""
    print("=" * 50)
    print("ЗАПУСК СКРАПИНГА REMANGA.ORG")
    print("=" * 50)
    
    driver = setup_driver()
    data = []
    
    try:
        manga_urls = get_manga_list(driver)
        print(f"Найдено {len(manga_urls)} манг для парсинга")
        
        for i, url in enumerate(manga_urls):
            if len(data) >= 110:  
                break
                
            manga_data = parse_manga_page(driver, url)
            if manga_data:
                data.append(manga_data)
            
            if (i + 1) % 10 == 0:
                print(f"Прогресс: {i+1}/{len(manga_urls)}, собрано: {len(data)}")
            
            time.sleep(random.uniform(1, 3))
        
        save_data(data)
        return data
        
    finally:
        driver.quit()
        print("Браузер закрыт")


def save_data(data, filename="data/raw_manga.json"):
    """Сохранение данных в JSON"""
    import os
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    print(f"\nДанные сохранены в {filename}")
    print(f"Всего записей: {len(data)}")
    
    # Статистика
    if data:
        ratings = [float(d['rating']) for d in data if d['rating'] != "0.0"]
        print(f"Средний рейтинг: {sum(ratings)/len(ratings):.2f}" if ratings else "")


if __name__ == "__main__":
    import os
    if not os.path.exists("data"):
        os.makedirs("data")
    
    result = scrape_manga_data()
    
    print("\n" + "=" * 50)
    print("СКРАПИНГ ЗАВЕРШЕН!")
