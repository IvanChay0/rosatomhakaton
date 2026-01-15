import sqlite3
import os
from datetime import datetime, timedelta
import random

def create_database():
    """Создание всей структуры базы данных"""
    
    # Удаляем старую базу данных если существует
    if os.path.exists('rosatom_database.db'):
        os.remove('rosatom_database.db')
        print("Старая база данных удалена")
    
    # Создаем новое подключение
    conn = sqlite3.connect('rosatom_database.db')
    cursor = conn.cursor()
    
    print("Создание таблиц...")
    
    # 1. Таблица сотрудников
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS employees (
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
    
    # 2. Таблица проектов
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS projects (
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
    
    # 3. Таблица оборудования
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS equipment (
        equipment_id INTEGER PRIMARY KEY,
        equipment_name TEXT NOT NULL,
        type TEXT NOT NULL,
        purchase_date TEXT NOT NULL,
        maintenance_date TEXT,
        status TEXT NOT NULL,
        department TEXT NOT NULL,
        cost REAL NOT NULL
    )
    ''')
    
    # 4. Таблица производства
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS production (
        production_id INTEGER PRIMARY KEY,
        date TEXT NOT NULL,
        product_name TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        revenue REAL NOT NULL,
        department TEXT NOT NULL,
        project_id INTEGER
    )
    ''')
    
    # 5. Таблица инцидентов безопасности
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS safety_incidents (
        incident_id INTEGER PRIMARY KEY,
        date TEXT NOT NULL,
        description TEXT NOT NULL,
        severity TEXT NOT NULL,
        department TEXT NOT NULL,
        resolved INTEGER,
        resolution_time_hours INTEGER
    )
    ''')
    
    print("Таблицы созданы успешно!")
    print("Заполнение данными...")
    
    # Генерация данных
    departments = ['Ядерная энергетика', 'Научные исследования', 'Безопасность', 
                   'Логистика', 'Инжиниринг', 'IT']
    positions = ['Инженер', 'Ученый', 'Менеджер', 'Аналитик', 'Техник', 'Специалист']
    project_names = ['АЭС-2006', 'БРЕСТ-ОД-300', 'ПАТЭС', 'ТОКАМАК', 
                     'Квантовые вычисления', 'Ядерная медицина', 'Радиационная безопасность']
    products = ['ТВЭЛ', 'Оборудование АЭС', 'Изотопы', 'Научные отчеты', 'Консультации']
    equipment_types = ['Реактор', 'Турбина', 'Генератор', 'Контрольная система', 
                       'Лабораторное оборудование', 'Криогенное оборудование']
    severity_levels = ['Низкий', 'Средний', 'Высокий', 'Критический']
    
    # Заполнение таблицы сотрудников
    employees_data = []
    for i in range(1, 51):  # 50 сотрудников
        employees_data.append((
            i,
            f'Имя{i}',
            f'Фамилия{i}',
            random.choice(departments),
            random.choice(positions),
            (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1000))).strftime('%Y-%m-%d'),
            random.randint(50000, 200000),
            random.randint(1, 10),
            random.randint(60, 100)
        ))
    
    cursor.executemany(
        'INSERT INTO employees VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
        employees_data
    )
    print(f"Добавлено {len(employees_data)} сотрудников")
    
    # Заполнение таблицы проектов
    projects_data = []
    for i in range(1, 11):  # 10 проектов
        projects_data.append((
            i,
            random.choice(project_names),
            (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 500))).strftime('%Y-%m-%d'),
            (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 500))).strftime('%Y-%m-%d'),
            random.randint(1000000, 50000000),
            random.choice(['В работе', 'Завершен', 'Планирование']),
            random.randint(1, 50),
            random.choice(departments)
        ))
    
    cursor.executemany(
        'INSERT INTO projects VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        projects_data
    )
    print(f"Добавлено {len(projects_data)} проектов")
    
    # Заполнение таблицы оборудования
    equipment_data = []
    for i in range(1, 31):  # 30 единиц оборудования
        equipment_data.append((
            i,
            f'Оборудование {i}',
            random.choice(equipment_types),
            (datetime(2018, 1, 1) + timedelta(days=random.randint(0, 2000))).strftime('%Y-%m-%d'),
            (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 500))).strftime('%Y-%m-%d'),
            random.choice(['Исправно', 'Требует ремонта', 'В обслуживании']),
            random.choice(departments),
            random.randint(100000, 5000000)
        ))
    
    cursor.executemany(
        'INSERT INTO equipment VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        equipment_data
    )
    print(f"Добавлено {len(equipment_data)} единиц оборудования")
    
    # Заполнение таблицы производства
    # В функции заполнения production_data в init_database.py:
    production_data = []
    for i in range(1, 101):  # 100 записей производства
        quantity = random.randint(10, 1000)
        price = random.randint(1000, 100000)
        revenue = quantity * price  # Гарантируем, что revenue > 0
        
        production_data.append((
            i,
            (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
            random.choice(products),
            quantity,
            revenue,  # Используем вычисленное значение
            random.choice(departments),
            random.randint(1, 10)
        ))
    
    cursor.executemany(
        'INSERT INTO production VALUES (?, ?, ?, ?, ?, ?, ?)',
        production_data
    )
    print(f"Добавлено {len(production_data)} записей производства")
    
    # Заполнение таблицы инцидентов
    incidents_data = []
    for i in range(1, 21):  # 20 инцидентов
        incidents_data.append((
            i,
            (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d'),
            f'Инцидент {i}: {random.choice(["Утечка данных", "Техническая неполадка", "Нарушение процедур"])}',
            random.choice(severity_levels),
            random.choice(departments),
            1 if random.random() > 0.3 else 0,  # resolved
            random.randint(1, 72) if random.random() > 0.5 else None
        ))
    
    cursor.executemany(
        'INSERT INTO safety_incidents VALUES (?, ?, ?, ?, ?, ?, ?)',
        incidents_data
    )
    print(f"Добавлено {len(incidents_data)} инцидентов")
    
    # Создаем индексы
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_emp_dept ON employees(department)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_prod_date ON production(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_proj_status ON projects(status)')
    
    conn.commit()
    conn.close()
    
    print("\n✅ База данных успешно создана и заполнена!")
    print(f"Файл: rosatom_database.db")
    print(f"Размер: {os.path.getsize('rosatom_database.db') / 1024:.1f} KB")

def verify_database():
    """Проверка структуры базы данных"""
    
    if not os.path.exists('rosatom_database.db'):
        print("❌ База данных не существует!")
        return False
    
    conn = sqlite3.connect('rosatom_database.db')
    cursor = conn.cursor()
    
    # Получаем список таблиц
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = cursor.fetchall()
    
    print("\n📊 Структура базы данных:")
    print("-" * 50)
    
    for table in tables:
        table_name = table[0]
        print(f"\nТаблица: {table_name}")
        
        # Получаем структуру таблицы
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        for col in columns:
            print(f"  - {col[1]} ({col[2]}) {'PRIMARY KEY' if col[5] else ''}")
        
        # Считаем количество записей
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"  Записей: {count}")
    
    conn.close()
    return True

if __name__ == '__main__':
    print("=" * 60)
    print("ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ ROSATOM BI SYSTEM")
    print("=" * 60)
    
    create_database()
    verify_database()