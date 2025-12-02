"""
Минимальный скрапер с точным поиском рейтинга по HTML структуре
"""
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import json
import re
from datetime import datetime

def scrape_one_manga():
    print("🚀 Запуск скрапинга с точным поиском рейтинга...")
    
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    
    driver = webdriver.Chrome(options=options)
    
    try:
        url = "https://remanga.org/manga/solo-leveling/main"
        driver.get(url)
        time.sleep(3)
        
        # Получаем весь HTML для поиска
        html = driver.page_source
        
        # Точный поиск данных
        data = {
            "title": clean_title(get_title(driver)),
            "description": get_description(driver),
            "year": get_year(html),
            "rating": get_rating(driver),
            "url": url,
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        print(f"✅ Собрано: {data['title']}")
        return data
        
    finally:
        driver.quit()

def clean_title(title):
    """Очистка заголовка от 'Читать ' и лишних пробелов"""
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
        # ПЕРВЫЙ СПОСОБ: Ищем по точному data-атрибуту из HTML
        try:
            desc_elements = driver.find_elements(By.CSS_SELECTOR, '[data-sentry-component="Description"]')
            for elem in desc_elements:
                # Внутри ищем параграфы
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

from bs4 import BeautifulSoup

def get_year(html):
    """Извлекает год из HTML с использованием BeautifulSoup и точных селекторов"""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Способ 1: Ищем ссылку с issue_year в href (точнее)
    year_link = soup.find('a', href=lambda x: x and 'issue_year' in str(x))
    if year_link:
        year_text = year_link.get_text(strip=True)
        # Проверяем, что это именно год (4 цифры)
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
        print(f"⚠️ Ошибка при получении рейтинга: {str(e)[:100]}")
        return "0.0"

if __name__ == "__main__":
    result = scrape_one_manga()
    
    if result:
        with open("data/single_manga.json", "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"\n📊 Результат:")
        print(f"  Название: {result['title']}")
        print(f"  Описание: {result['description'][:80]}...")
        print(f"  Год: {result['year']}")
        print(f"  Рейтинг: {result['rating']}")
        print(f"📁 Данные сохранены в data/single_manga.json")