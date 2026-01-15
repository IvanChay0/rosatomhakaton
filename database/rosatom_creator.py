import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import random
import numpy as np

def create_sample_database():
    """Создание расширенной демонстрационной базы данных для Росатома с исправленными датами"""
    
    # Подключение к SQLite базе данных
    conn = sqlite3.connect('rosatom_database.db')
    cursor = conn.cursor()
    
    # Удаляем существующие таблицы, если они есть
    tables = [
        'employees', 'projects', 'equipment', 'production', 
        'safety_incidents', 'finance', 'suppliers', 'customers',
        'tasks', 'locations', 'maintenance_logs', 'energy_consumption'
    ]
    for table in tables:
        cursor.execute(f'DROP TABLE IF EXISTS {table}')
    
    # Создание таблиц
    
    # Таблица сотрудников
    cursor.execute('''
    CREATE TABLE employees (
        employee_id INTEGER PRIMARY KEY,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        department TEXT NOT NULL,
        position TEXT NOT NULL,
        hire_date DATE NOT NULL,
        salary REAL NOT NULL,
        project_id INTEGER,
        performance_score INTEGER,
        experience_years INTEGER,
        education_level TEXT,
        location_id INTEGER,
        manager_id INTEGER,
        skills TEXT,
        email TEXT
    )
    ''')
    
    # Таблица проектов
    cursor.execute('''
    CREATE TABLE projects (
        project_id INTEGER PRIMARY KEY,
        project_name TEXT NOT NULL,
        project_code TEXT UNIQUE,
        start_date DATE NOT NULL,
        end_date DATE,
        budget REAL NOT NULL,
        actual_cost REAL,
        status TEXT NOT NULL,
        manager_id INTEGER,
        department TEXT NOT NULL,
        priority TEXT,
        risk_level TEXT,
        completion_percentage INTEGER,
        client_id INTEGER,
        location_id INTEGER
    )
    ''')
    
    # Таблица оборудования
    cursor.execute('''
    CREATE TABLE equipment (
        equipment_id INTEGER PRIMARY KEY,
        equipment_name TEXT NOT NULL,
        serial_number TEXT UNIQUE,
        type TEXT NOT NULL,
        subtype TEXT,
        manufacturer TEXT,
        purchase_date DATE NOT NULL,
        warranty_end_date DATE,
        status TEXT NOT NULL,
        department TEXT NOT NULL,
        cost REAL NOT NULL,
        location_id INTEGER,
        maintenance_interval_days INTEGER,
        last_maintenance_date DATE,
        next_maintenance_date DATE,
        operational_hours INTEGER
    )
    ''')
    
    # Таблица продаж/производства - ИЗМЕНЕНА для правильного формата дат
    cursor.execute('''
    CREATE TABLE production (
        production_id INTEGER PRIMARY KEY,
        date DATE NOT NULL,
        product_name TEXT NOT NULL,
        product_category TEXT,
        quantity INTEGER NOT NULL,
        unit_price REAL NOT NULL,
        revenue REAL NOT NULL,
        cost REAL NOT NULL,
        profit REAL NOT NULL,
        department TEXT NOT NULL,
        project_id INTEGER,
        customer_id INTEGER,
        location_id INTEGER,
        quality_score INTEGER,
        production_line TEXT
    )
    ''')
    
    # Таблица инцидентов безопасности
    cursor.execute('''
    CREATE TABLE safety_incidents (
        incident_id INTEGER PRIMARY KEY,
        date DATE NOT NULL,
        time TIME,
        description TEXT NOT NULL,
        category TEXT NOT NULL,
        severity TEXT NOT NULL,
        department TEXT NOT NULL,
        location_id INTEGER,
        equipment_id INTEGER,
        employee_id INTEGER,
        resolved BOOLEAN,
        resolution_date DATE,
        resolution_time_hours INTEGER,
        investigation_report TEXT,
        preventive_measures TEXT
    )
    ''')
    
    # Остальные таблицы остаются без изменений...
    cursor.execute('''
    CREATE TABLE finance (
        transaction_id INTEGER PRIMARY KEY,
        date DATE NOT NULL,
        type TEXT NOT NULL,
        category TEXT NOT NULL,
        amount REAL NOT NULL,
        currency TEXT DEFAULT 'RUB',
        description TEXT,
        department TEXT,
        project_id INTEGER,
        supplier_id INTEGER,
        payment_method TEXT,
        status TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE suppliers (
        supplier_id INTEGER PRIMARY KEY,
        supplier_name TEXT NOT NULL,
        contact_person TEXT,
        phone TEXT,
        email TEXT,
        category TEXT,
        rating INTEGER,
        contract_start_date DATE,
        contract_end_date DATE,
        total_contract_amount REAL
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE customers (
        customer_id INTEGER PRIMARY KEY,
        customer_name TEXT NOT NULL,
        customer_type TEXT,
        industry TEXT,
        country TEXT,
        contact_person TEXT,
        phone TEXT,
        email TEXT,
        contract_value REAL,
        contract_start_date DATE,
        contract_end_date DATE
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE tasks (
        task_id INTEGER PRIMARY KEY,
        task_name TEXT NOT NULL,
        project_id INTEGER,
        assigned_to INTEGER,
        assigned_by INTEGER,
        start_date DATE,
        due_date DATE,
        completed_date DATE,
        status TEXT,
        priority TEXT,
        estimated_hours INTEGER,
        actual_hours INTEGER,
        description TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE locations (
        location_id INTEGER PRIMARY KEY,
        location_name TEXT NOT NULL,
        location_type TEXT,
        city TEXT,
        country TEXT,
        latitude REAL,
        longitude REAL,
        manager_id INTEGER,
        operational_since DATE
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE maintenance_logs (
        log_id INTEGER PRIMARY KEY,
        equipment_id INTEGER NOT NULL,
        maintenance_date DATE NOT NULL,
        maintenance_type TEXT,
        technician_id INTEGER,
        duration_hours REAL,
        cost REAL,
        description TEXT,
        parts_replaced TEXT,
        next_maintenance_date DATE
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE energy_consumption (
        consumption_id INTEGER PRIMARY KEY,
        date DATE NOT NULL,
        location_id INTEGER,
        department TEXT,
        energy_type TEXT,
        consumption_mwh REAL,
        cost REAL,
        efficiency_rating REAL,
        peak_hours INTEGER,
        notes TEXT
    )
    ''')
    
    # Генерация демо данных
    
    # Генерация локаций
    locations_data = []
    cities = ['Москва', 'Санкт-Петербург', 'Нижний Новгород', 'Новосибирск', 
              'Екатеринбург', 'Калининград', 'Владивосток', 'Сочи', 'Казань']
    
    for i in range(1, 26):
        city = random.choice(cities)
        locations_data.append((
            i,
            f'{city} Объект {i}',
            random.choice(['АЭС', 'НИИ', 'Завод', 'Офис', 'Склад', 'Лаборатория']),
            city,
            'Россия',
            round(random.uniform(45, 60), 4),
            round(random.uniform(30, 140), 4),
            random.randint(1, 100),
            datetime(2000 + random.randint(0, 20), 1, 1).strftime('%Y-%m-%d')
        ))
    
    cursor.executemany('INSERT INTO locations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)', locations_data)
    
    # Генерация поставщиков
    suppliers_data = []
    supplier_names = ['Ростех', 'Газпром', 'Лукойл', 'Сбербанк', 'РЖД', 'Роснефть', 
                      'Сименс', 'Альстом', 'Хитачи', 'Тошиба', 'Китайская CNNC']
    
    for i in range(1, 51):
        suppliers_data.append((
            i,
            f'{random.choice(supplier_names)} {i}',
            f'Контактное лицо {i}',
            f'+7{random.randint(900, 999)}{random.randint(1000000, 9999999)}',
            f'supplier{i}@example.com',
            random.choice(['Оборудование', 'Сырье', 'Услуги', 'ИТ', 'Консалтинг']),
            random.randint(1, 10),
            (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1000))).strftime('%Y-%m-%d'),
            (datetime(2025, 1, 1) + timedelta(days=random.randint(0, 1000))).strftime('%Y-%m-%d'),
            random.randint(1000000, 50000000)
        ))
    
    cursor.executemany('INSERT INTO suppliers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', suppliers_data)
    
    # Генерация клиентов
    customers_data = []
    customer_types = ['Государственный', 'Коммерческий', 'Международный', 'Научный', 'Медицинский']
    industries = ['Энергетика', 'Медицина', 'Наука', 'Промышленность', 'Образование']
    
    for i in range(1, 101):
        customers_data.append((
            i,
            f'Клиент {i}',
            random.choice(customer_types),
            random.choice(industries),
            random.choice(['Россия', 'Китай', 'Индия', 'Турция', 'Египет', 'Белоруссия']),
            f'Менеджер {i}',
            f'+7{random.randint(900, 999)}{random.randint(1000000, 9999999)}',
            f'customer{i}@example.com',
            random.randint(5000000, 500000000),
            (datetime(2019, 1, 1) + timedelta(days=random.randint(0, 1500))).strftime('%Y-%m-%d'),
            (datetime(2026, 1, 1) + timedelta(days=random.randint(0, 1500))).strftime('%Y-%m-%d')
        ))
    
    cursor.executemany('INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', customers_data)
    
    # Генерация сотрудников
    departments = ['Ядерная энергетика', 'Научные исследования', 'Безопасность', 
                   'Логистика', 'Инжиниринг', 'IT', 'Финансы', 'HR', 'Закупки']
    positions = ['Инженер', 'Ученый', 'Менеджер', 'Аналитик', 'Техник', 'Специалист',
                 'Директор', 'Консультант', 'Разработчик', 'Оператор']
    education_levels = ['Бакалавр', 'Магистр', 'Кандидат наук', 'Доктор наук']
    
    employees_data = []
    for i in range(1, 501):
        hire_date = datetime(2010 + random.randint(0, 14), random.randint(1, 12), random.randint(1, 28))
        experience = (datetime.now() - hire_date).days // 365
        
        employees_data.append((
            i,
            f'Имя{i}',
            f'Фамилия{i}',
            random.choice(departments),
            random.choice(positions),
            hire_date.strftime('%Y-%m-%d'),
            round(random.uniform(50000, 300000), 2),
            random.randint(1, 50) if random.random() > 0.3 else None,
            random.randint(50, 100),
            min(experience, 30),
            random.choice(education_levels),
            random.randint(1, 25),
            random.randint(1, 500) if i > 50 else None,
            ', '.join(random.sample(['SQL', 'Python', 'Аналитика', 'Управление', 'Безопасность'], 3)),
            f'employee{i}@rosatom.ru'
        ))
    
    cursor.executemany('INSERT INTO employees VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', employees_data)
    
    # Генерация проектов
    projects_data = []
    project_names = ['АЭС-2006', 'БРЕСТ-ОД-300', 'ПАТЭС', 'ТОКАМАК', 
                     'Квантовые вычисления', 'Ядерная медицина', 'Радиационная безопасность',
                     'Цифровизация', 'Зеленая энергия', 'Международное сотрудничество']
    
    for i in range(1, 51):
        start_date = datetime(2018 + random.randint(0, 5), random.randint(1, 12), random.randint(1, 28))
        end_date = start_date + timedelta(days=random.randint(180, 1500))
        
        projects_data.append((
            i,
            f'{random.choice(project_names)} {i}',
            f'PROJ-{i:04d}',
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d') if random.random() > 0.2 else None,
            round(random.uniform(10000000, 500000000), 2),
            round(random.uniform(10000000, 500000000) * random.uniform(0.8, 1.2), 2),
            random.choice(['В работе', 'Завершен', 'Планирование', 'Приостановлен', 'На проверке']),
            random.randint(1, 500),
            random.choice(departments),
            random.choice(['Высокий', 'Средний', 'Низкий']),
            random.choice(['Низкий', 'Средний', 'Высокий']),
            random.randint(0, 100),
            random.randint(1, 100),
            random.randint(1, 25)
        ))
    
    cursor.executemany('INSERT INTO projects VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', projects_data)
    
    # Генерация оборудования
    equipment_data = []
    equipment_types = ['Реактор', 'Турбина', 'Генератор', 'Контрольная система', 
                       'Лабораторное оборудование', 'Криогенное оборудование',
                       'Компьютер', 'Сервер', 'Сетевое оборудование']
    
    manufacturers = ['Сименс', 'Альстом', 'Хитачи', 'Тошиба', 'Росатом', 'Ростех']
    
    for i in range(1, 201):
        purchase_date = datetime(2015 + random.randint(0, 8), random.randint(1, 12), random.randint(1, 28))
        warranty_end = purchase_date + timedelta(days=365 * random.randint(1, 5))
        
        equipment_data.append((
            i,
            f'Оборудование {i}',
            f'SN-{random.randint(10000, 99999)}-{i}',
            random.choice(equipment_types),
            random.choice(['Основное', 'Вспомогательное', 'Измерительное', 'Контрольное']),
            random.choice(manufacturers),
            purchase_date.strftime('%Y-%m-%d'),
            warranty_end.strftime('%Y-%m-%d'),
            random.choice(['Исправно', 'Требует ремонта', 'В обслуживании', 'Выведено из эксплуатации']),
            random.choice(departments),
            round(random.uniform(100000, 10000000), 2),
            random.randint(1, 25),
            random.randint(30, 365),
            (purchase_date + timedelta(days=random.randint(30, 1000))).strftime('%Y-%m-%d'),
            (purchase_date + timedelta(days=random.randint(365, 2000))).strftime('%Y-%m-%d'),
            random.randint(1000, 50000)
        ))
    
    cursor.executemany('INSERT INTO equipment VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', equipment_data)
    
    # ГЕНЕРАЦИЯ ДАННЫХ ПРОИЗВОДСТВА - КЛЮЧЕВЫЕ ИЗМЕНЕНИЯ
    print("Генерация данных производства (продаж)...")
    production_data = []
    products = ['ТВЭЛ', 'Оборудование АЭС', 'Изотопы', 'Научные отчеты', 
                'Консультации', 'Обучение', 'Техническая поддержка', 'Лицензии']
    categories = ['Продукция', 'Услуги', 'Консалтинг', 'Лицензирование']
    
    # Создаем реалистичную динамику продаж с трендом роста
    base_date = datetime(2021, 1, 1)
    transaction_id = 1
    
    # Для каждого дня за 3 года
    for day_offset in range(0, 1096):  # 3 года = 1095 дней
        current_date = base_date + timedelta(days=day_offset)
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Количество транзакций в день варьируется
        transactions_per_day = random.randint(5, 20)
        
        # Базовый уровень продаж с сезонностью и трендом
        # Тренд роста: +0.1% в день
        trend_factor = 1 + (day_offset * 0.001)
        # Сезонность: выше продажи весной и осенью
        month = current_date.month
        if month in [3, 4, 5, 9, 10, 11]:  # Весна и осень
            season_factor = 1.2
        elif month in [12, 1, 2]:  # Зима
            season_factor = 0.8
        else:  # Лето
            season_factor = 0.9
        
        for _ in range(transactions_per_day):
            quantity = random.randint(1, 100)
            unit_price = round(random.uniform(1000, 500000) * trend_factor * season_factor, 2)
            cost_per_unit = unit_price * random.uniform(0.3, 0.8)
            
            production_data.append((
                transaction_id,
                date_str,  # Используем строго YYYY-MM-DD формат
                random.choice(products),
                random.choice(categories),
                quantity,
                unit_price,
                round(quantity * unit_price, 2),
                round(quantity * cost_per_unit, 2),
                round(quantity * (unit_price - cost_per_unit), 2),
                random.choice(departments),
                random.randint(1, 50) if random.random() > 0.4 else None,
                random.randint(1, 100),
                random.randint(1, 25),
                random.randint(70, 100),
                f'Линия {random.randint(1, 10)}'
            ))
            transaction_id += 1
            
            # Периодически добавляем выбросы (необычно высокие продажи)
            if random.random() < 0.01:  # 1% шанс на выброс
                big_quantity = random.randint(500, 5000)
                big_unit_price = round(random.uniform(10000, 1000000), 2)
                
                production_data.append((
                    transaction_id,
                    date_str,
                    random.choice(products),
                    random.choice(categories),
                    big_quantity,
                    big_unit_price,
                    round(big_quantity * big_unit_price, 2),
                    round(big_quantity * big_unit_price * random.uniform(0.2, 0.6), 2),
                    round(big_quantity * (big_unit_price - big_unit_price * random.uniform(0.2, 0.6)), 2),
                    random.choice(departments),
                    random.randint(1, 50),
                    random.randint(1, 100),
                    random.randint(1, 25),
                    95,  # Высокое качество для крупных заказов
                    f'Линия {random.randint(1, 3)}'
                ))
                transaction_id += 1
        
        # Прогресс
        if day_offset % 100 == 0:
            print(f"  Сгенерировано {day_offset} дней из 1095...")
    
    print(f"  Всего сгенерировано {len(production_data)} записей продаж")
    cursor.executemany('INSERT INTO production VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', production_data)
    
    # Генерация инцидентов безопасности
    incidents_data = []
    severity_levels = ['Низкий', 'Средний', 'Высокий', 'Критический']
    categories = ['Технический', 'Человеческий фактор', 'Кибербезопасность', 'Природный', 'Процедурный']
    
    for i in range(1, 201):
        incident_date = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))
        resolved = random.choice([True, False])
        
        incidents_data.append((
            i,
            incident_date.strftime('%Y-%m-%d'),
            f'{random.randint(0, 23):02d}:{random.randint(0, 59):02d}',
            f'Инцидент {i}: {random.choice(["Утечка данных", "Техническая неполадка", 
                                           "Нарушение процедур", "Кибератака", "Природное явление"])}',
            random.choice(categories),
            random.choice(severity_levels),
            random.choice(departments),
            random.randint(1, 25),
            random.randint(1, 200) if random.random() > 0.5 else None,
            random.randint(1, 500) if random.random() > 0.5 else None,
            resolved,
            (incident_date + timedelta(hours=random.randint(1, 168))).strftime('%Y-%m-%d') if resolved else None,
            random.randint(1, 168) if resolved else None,
            f'Отчет по расследованию {i}' if random.random() > 0.3 else None,
            f'Меры профилактики {i}' if random.random() > 0.4 else None
        ))
    
    cursor.executemany('INSERT INTO safety_incidents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', incidents_data)
    
    # Генерация финансовых транзакций
    finance_data = []
    transaction_types = ['Доход', 'Расход', 'Инвестиция', 'Кредит']
    categories = ['Зарплата', 'Закупки', 'Продажи', 'Аренда', 'Налоги', 'Исследования', 'Обслуживание']
    
    for i in range(1, 1001):
        finance_data.append((
            i,
            (datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))).strftime('%Y-%m-%d'),
            random.choice(transaction_types),
            random.choice(categories),
            round(random.uniform(1000, 5000000), 2),
            random.choice(['RUB', 'USD', 'EUR']),
            f'Транзакция {i}: {random.choice(["Оплата", "Поступление", "Перевод", "Инвестиция"])}',
            random.choice(departments),
            random.randint(1, 50) if random.random() > 0.5 else None,
            random.randint(1, 50) if random.random() > 0.3 else None,
            random.choice(['Карта', 'Безналичный', 'Наличные', 'Перевод']),
            random.choice(['Завершена', 'В процессе', 'Отклонена'])
        ))
    
    cursor.executemany('INSERT INTO finance VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', finance_data)
    
    # Генерация задач
    tasks_data = []
    
    for i in range(1, 501):
        start_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))
        due_date = start_date + timedelta(days=random.randint(7, 90))
        completed = random.choice([True, False])
        
        tasks_data.append((
            i,
            f'Задача {i}',
            random.randint(1, 50),
            random.randint(1, 500),
            random.randint(1, 500),
            start_date.strftime('%Y-%m-%d'),
            due_date.strftime('%Y-%m-%d'),
            (due_date - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d') if completed else None,
            'Завершена' if completed else random.choice(['В работе', 'На проверке', 'Отложена']),
            random.choice(['Высокий', 'Средний', 'Низкий']),
            random.randint(1, 100),
            random.randint(1, 150) if completed else None,
            f'Описание задачи {i}'
        ))
    
    cursor.executemany('INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', tasks_data)
    
    # Генерация логов обслуживания
    maintenance_data = []
    
    for i in range(1, 301):
        maintenance_date = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 730))
        
        maintenance_data.append((
            i,
            random.randint(1, 200),
            maintenance_date.strftime('%Y-%m-%d'),
            random.choice(['Плановое', 'Аварийное', 'Профилактическое', 'Модернизация']),
            random.randint(1, 500),
            round(random.uniform(0.5, 24), 1),
            round(random.uniform(1000, 500000), 2),
            f'Обслуживание оборудования {i}',
            random.choice([None, f'Деталь {random.randint(1, 100)}', f'Компонент {random.randint(1, 50)}']),
            (maintenance_date + timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d')
        ))
    
    cursor.executemany('INSERT INTO maintenance_logs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', maintenance_data)
    
    # Генерация потребления энергии
    energy_data = []
    
    base_date = datetime(2023, 1, 1)
    for i in range(1, 366):
        date = base_date + timedelta(days=i-1)
        
        energy_data.append((
            i,
            date.strftime('%Y-%m-%d'),
            random.randint(1, 25),
            random.choice(departments),
            random.choice(['Электричество', 'Тепло', 'Вода', 'Газ']),
            round(random.uniform(10, 1000), 2),
            round(random.uniform(1000, 100000), 2),
            round(random.uniform(0.7, 0.95), 2),
            random.randint(0, 24),
            f'Дневное потребление {date.strftime("%Y-%m-%d")}'
        ))
    
    cursor.executemany('INSERT INTO energy_consumption VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', energy_data)
    
    # Создание индексов для оптимизации
    print("Создание индексов для оптимизации запросов...")
    indexes = [
        'CREATE INDEX idx_employees_department ON employees(department)',
        'CREATE INDEX idx_employees_project ON employees(project_id)',
        'CREATE INDEX idx_production_date ON production(date)',
        'CREATE INDEX idx_production_department ON production(department)',
        'CREATE INDEX idx_production_product ON production(product_name)',
        'CREATE INDEX idx_production_revenue ON production(revenue)',
        'CREATE INDEX idx_projects_status ON projects(status)',
        'CREATE INDEX idx_projects_department ON projects(department)',
        'CREATE INDEX idx_safety_severity ON safety_incidents(severity)',
        'CREATE INDEX idx_safety_date ON safety_incidents(date)',
        'CREATE INDEX idx_finance_date ON finance(date)',
        'CREATE INDEX idx_finance_type ON finance(type)',
        'CREATE INDEX idx_tasks_project ON tasks(project_id)',
        'CREATE INDEX idx_tasks_status ON tasks(status)',
        'CREATE INDEX idx_equipment_status ON equipment(status)',
        'CREATE INDEX idx_equipment_department ON equipment(department)',
        'CREATE INDEX idx_energy_date ON energy_consumption(date)',
        'CREATE INDEX idx_maintenance_equipment ON maintenance_logs(equipment_id)'
    ]
    
    for index in indexes:
        try:
            cursor.execute(index)
        except Exception as e:
            print(f"  Ошибка создания индекса {index}: {e}")
    
    conn.commit()
    
    # Проверяем данные в таблице production
    print("\nПроверка данных в таблице production...")
    cursor.execute("SELECT COUNT(*) as total FROM production")
    total_records = cursor.fetchone()[0]
    
    cursor.execute("SELECT MIN(date), MAX(date) FROM production WHERE date IS NOT NULL")
    date_range = cursor.fetchone()
    
    cursor.execute("SELECT SUM(revenue) as total_revenue FROM production WHERE revenue IS NOT NULL")
    total_revenue = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT 
            substr(date, 1, 7) as month,
            COUNT(*) as transactions,
            SUM(revenue) as monthly_revenue
        FROM production 
        WHERE date IS NOT NULL AND revenue IS NOT NULL
        GROUP BY substr(date, 1, 7)
        ORDER BY month DESC
        LIMIT 5
    """)
    recent_months = cursor.fetchall()
    
    conn.close()
    
    print("\n" + "="*60)
    print("✅ РАСШИРЕННАЯ ДЕМОНСТРАЦИОННАЯ БАЗА ДАННЫХ УСПЕШНО СОЗДАНА!")
    print("="*60)
    print("\n📊 Созданы таблицы:")
    print("1. employees - 500 записей (сотрудники)")
    print("2. projects - 50 записей (проекты)")
    print("3. equipment - 200 записей (оборудование)")
    print(f"4. production - {total_records:,} записей (продажи/производство)")
    print("5. safety_incidents - 200 записей (инциденты безопасности)")
    print("6. finance - 1,000 записей (финансы)")
    print("7. suppliers - 50 записей (поставщики)")
    print("8. customers - 100 записей (клиенты)")
    print("9. tasks - 500 записей (задачи)")
    print("10. locations - 25 записей (локации)")
    print("11. maintenance_logs - 300 записей (логи обслуживания)")
    print("12. energy_consumption - 365 записей (потребление энергии)")
    print(f"\n📈 Всего: ~{total_records + 500 + 50 + 200 + 200 + 1000 + 50 + 100 + 500 + 25 + 300 + 365:,} записей")
    
    print(f"\n📅 Диапазон дат продаж: {date_range[0]} - {date_range[1]}")
    print(f"💰 Общая выручка: {total_revenue:,.2f} ₽")
    
    print("\n📊 Последние 5 месяцев продаж:")
    for month in recent_months:
        print(f"  {month[0]}: {month[1]:,} транзакций, {month[2]:,.2f} ₽")
    
    print("\n🎯 Примеры запросов для тестирования:")
    print("  • 'Динамика продаж за последний год'")
    print("  • 'Продажи по месяцам'")
    print("  • 'Топ-5 товаров по выручке'")
    print("  • 'Общая выручка за 2023 год'")
    print("  • 'Сравнение продаж по отделам'")
    print("\n🚀 Система готова к работе!")

if __name__ == '__main__':
    print("="*60)
    print("Создание демонстрационной базы данных Rosatom...")
    print("="*60)
    
    start_time = datetime.now()
    create_sample_database()
    end_time = datetime.now()
    
    print(f"\n⏱️ Время создания базы: {(end_time - start_time).total_seconds():.2f} секунд")