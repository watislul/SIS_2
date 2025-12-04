from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os

def task1():
    """Простая тестовая задача"""
    print("="*60)
    print("TASK 1: Проверка Airflow")
    print("="*60)
    print("✅ Airflow DAG работает!")
    return "success"

def task2():
    """Проверка файлов проекта"""
    print("="*60)
    print("TASK 2: Проверка файлов проекта")
    print("="*60)
    
    # Проверяем что файлы существуют
    files_to_check = [
        "/opt/airflow/src/scraper.py",
        "/opt/airflow/src/cleaner.py", 
        "/opt/airflow/src/loader.py"
    ]
    
    for file in files_to_check:
        if os.path.exists(file):
            size = os.path.getsize(file)
            print(f"✅ {os.path.basename(file)}: {size} байт")
        else:
            print(f"❌ {os.path.basename(file)}: не найден")
    
    return "files_checked"

def task3():
    """Запуск реального пайплайна"""
    print("="*60)
    print("TASK 3: Запуск пайплайна")
    print("="*60)
    
    # Простой запуск скриптов
    scripts = [
        ("scraper.py", "Скрапинг"),
        ("cleaner.py", "Очистка"),
        ("loader.py", "Загрузка в БД")
    ]
    
    for script, description in scripts:
        script_path = f"/opt/airflow/src/{script}"
        if os.path.exists(script_path):
            print(f"▶️  {description}...")
            os.system(f"python {script_path}")
        else:
            print(f"⚠️  {script} не найден")
    
    return "pipeline_executed"

def task4():
    """Итоговая проверка"""
    print("="*60)
    print("TASK 4: Итоговая проверка")
    print("="*60)
    
    # Проверяем базу данных
    db_path = "/opt/airflow/data/output.db"
    if os.path.exists(db_path):
        import sqlite3
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем таблицы
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        if tables:
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
                count = cursor.fetchone()[0]
                print(f"📊 Таблица '{table_name}': {count} записей")
                
                if count >= 100:
                    print("   ✅ Требование ≥100 записей ВЫПОЛНЕНО")
                else:
                    print(f"   ⚠️  Только {count} записей")
        
        conn.close()
    else:
        print("❌ База данных не найдена")
    
    print("="*60)
    print("🎉 ПАЙПЛАЙН ЗАВЕРШЕН!")
    print("="*60)
    
    return "validation_complete"

with DAG(
    'manga_final',
    start_date=datetime(2025, 12, 1),
    schedule_interval=None,  # Только ручной запуск
    catchup=False,
) as dag:
    
    t1 = PythonOperator(
        task_id='test_airflow',
        python_callable=task1,
    )
    
    t2 = PythonOperator(
        task_id='check_files',
        python_callable=task2,
    )
    
    t3 = PythonOperator(
        task_id='run_pipeline',
        python_callable=task3,
    )
    
    t4 = PythonOperator(
        task_id='final_check',
        python_callable=task4,
    )
    
    t1 >> t2 >> t3 >> t4