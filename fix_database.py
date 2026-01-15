import sqlite3
import os
from datetime import datetime, timedelta
import random

def fix_database():
    """Полное исправление базы данных"""
    
    print("🔧 ИСПРАВЛЕНИЕ БАЗЫ ДАННЫХ ROSATOM")
    print("=" * 60)
    
    db_path = 'rosatom_database.db'
    
    if not os.path.exists(db_path):
        print(f"❌ Файл {db_path} не найден!")
        return False
    
    print(f"📁 База данных: {os.path.abspath(db_path)}")
    print(f"📊 Размер: {os.path.getsize(db_path):,} байт")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Проверяем таблицы
    print("\n📋 Проверка структуры...")
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [t[0] for t in cursor.fetchall()]
    print(f"Таблицы: {tables}")
    
    # 2. Если таблицы projects нет, создаем её
    if 'projects' not in tables:
        print("⚠️ Таблица projects отсутствует, создаём...")
        cursor.execute('''
        CREATE TABLE projects (
            project_id INTEGER PRIMARY KEY,
            project_name TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT,
            budget REAL NOT NULL,
            status TEXT NOT NULL,
            manager_id INTEGER,
            department TEXT NOT NULL
        )
        ''')
        
        # Заполняем тестовыми данными
        projects_data = []
        for i in range(1, 11):
            projects_data.append((
                i,
                f'Проект {i}',
                (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 100))).strftime('%Y-%m-%d'),
                (datetime(2024, 12, 31) + timedelta(days=random.randint(0, 100))).strftime('%Y-%m-%d'),
                random.randint(1000000, 10000000),
                random.choice(['В работе', 'Завершен', 'Планирование']),
                random.randint(1, 50),
                random.choice(['Ядерная энергетика', 'Научные исследования', 'Безопасность'])
            ))
        
        cursor.executemany(
            "INSERT INTO projects VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            projects_data
        )
        print(f"✅ Создана таблица projects с {len(projects_data)} записями")
    
    # 3. Создаем таблицу production если её нет или она пустая
    if 'production' not in tables:
        print("⚠️ Таблица production отсутствует, создаём...")
        cursor.execute('''
        CREATE TABLE production (
            production_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            product_name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            revenue REAL NOT NULL,
            department TEXT NOT NULL,
            project_id INTEGER
        )
        ''')
    
    # 4. Проверяем и заполняем таблицу production
    cursor.execute("SELECT COUNT(*) FROM production")
    count = cursor.fetchone()[0]
    
    if count == 0:
        print("🔄 Таблица production пустая, заполняем...")
        
        departments = ['Ядерная энергетика', 'Научные исследования', 'Безопасность', 
                      'Логистика', 'IT', 'Финансы']
        products = ['ТВЭЛ', 'Изотопы', 'Оборудование', 'Консультации', 'Обучение', 'Лицензии']
        
        production_data = []
        for i in range(1, 51):
            date = (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
            product = random.choice(products)
            quantity = random.randint(10, 100)
            revenue = quantity * random.randint(10000, 50000)
            
            production_data.append((
                date,
                product,
                quantity,
                revenue,
                random.choice(departments),
                random.randint(1, 10)
            ))
        
        cursor.executemany(
            "INSERT INTO production (date, product_name, quantity, revenue, department, project_id) VALUES (?, ?, ?, ?, ?, ?)",
            production_data
        )
        print(f"✅ Добавлено {len(production_data)} записей в production")
    
    # 5. Проверяем таблицу safety_incidents
    if 'safety_incidents' not in tables:
        print("⚠️ Таблица safety_incidents отсутствует, создаём...")
        cursor.execute('''
        CREATE TABLE safety_incidents (
            incident_id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            description TEXT NOT NULL,
            severity TEXT NOT NULL,
            department TEXT NOT NULL,
            resolved INTEGER,
            resolution_time_hours INTEGER
        )
        ''')
        
        # Добавляем тестовые данные
        incidents_data = []
        for i in range(1, 11):
            incidents_data.append((
                (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
                f'Инцидент {i}: {random.choice(["Утечка", "Ошибка", "Нарушение"])}',
                random.choice(['Низкий', 'Средний', 'Высокий']),
                random.choice(['Безопасность', 'Ядерная энергетика', 'IT']),
                random.choice([0, 1]),
                random.randint(1, 24) if random.random() > 0.5 else None
            ))
        
        cursor.executemany(
            "INSERT INTO safety_incidents (date, description, severity, department, resolved, resolution_time_hours) VALUES (?, ?, ?, ?, ?, ?)",
            incidents_data
        )
        print(f"✅ Добавлено {len(incidents_data)} инцидентов")
    
    # 6. Проверяем таблицу employees
    if 'employees' not in tables:
        print("⚠️ Таблица employees отсутствует, создаём...")
        cursor.execute('''
        CREATE TABLE employees (
            employee_id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            department TEXT NOT NULL,
            position TEXT NOT NULL,
            hire_date TEXT NOT NULL,
            salary REAL NOT NULL,
            project_id INTEGER,
            performance_score INTEGER
        )
        ''')
        
        # Добавляем тестовые данные
        employees_data = []
        for i in range(1, 51):
            employees_data.append((
                i,
                f'Имя{i}',
                f'Фамилия{i}',
                random.choice(['Ядерная энергетика', 'Научные исследования', 'Безопасность']),
                random.choice(['Инженер', 'Ученый', 'Менеджер', 'Аналитик']),
                (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1000))).strftime('%Y-%m-%d'),
                random.randint(50000, 200000),
                random.randint(1, 10),
                random.randint(60, 100)
            ))
        
        cursor.executemany(
            "INSERT INTO employees VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            employees_data
        )
        print(f"✅ Добавлено {len(employees_data)} сотрудников")
    
    # 7. Создаем индексы
    print("\n📊 Создание индексов...")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_prod_date ON production(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_prod_dept ON production(department)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_emp_dept ON employees(department)")
    
    # 8. Проверяем итоги
    print("\n📈 ИТОГОВАЯ ПРОВЕРКА:")
    
    cursor.execute("SELECT SUM(revenue) FROM production")
    total_revenue = cursor.fetchone()[0] or 0
    print(f"💰 Общая выручка: {total_revenue:,.0f} ₽")
    
    cursor.execute("SELECT COUNT(*) FROM employees")
    employees_count = cursor.fetchone()[0]
    print(f"👥 Сотрудников: {employees_count}")
    
    cursor.execute("SELECT COUNT(*) FROM projects WHERE status = 'В работе'")
    active_projects = cursor.fetchone()[0]
    print(f"🚀 Активных проектов: {active_projects}")
    
    cursor.execute("""
        SELECT 
            (COUNT(CASE WHEN severity = 'Низкий' THEN 1 END) * 100.0 / 
             NULLIF(COUNT(*), 0)) as safety_score 
        FROM safety_incidents
    """)
    safety_score = cursor.fetchone()[0] or 100
    print(f"🛡️ Безопасность: {safety_score:.1f}%")
    
    conn.commit()
    conn.close()
    
    print("\n" + "=" * 60)
    print("✅ БАЗА ДАННЫХ УСПЕШНО ИСПРАВЛЕНА!")
    print("=" * 60)
    print("\n🎯 Действия:")
    print("1. Остановите Flask приложение (Ctrl+C)")
    print("2. Перезапустите: python app.py")
    print("3. Проверьте отчеты на http://localhost:5000/reports")
    
    return True

if __name__ == '__main__':
    fix_database()