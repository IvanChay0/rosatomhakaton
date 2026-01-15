# Создайте файл test_dates.py
import sqlite3

conn = sqlite3.connect('rosatom_database.db')
cursor = conn.cursor()

print("🔍 Проверка формата дат в production:")
print("="*60)

# Проверка структуры
cursor.execute("SELECT date FROM production WHERE date IS NOT NULL LIMIT 5")
dates = cursor.fetchall()

print("Примеры дат:")
for i, (date,) in enumerate(dates, 1):
    print(f"  {i}. '{date}' (длина: {len(date) if date else 0})")

# Проверка разных форматов
print("\n📊 Статистика по форматам дат:")
cursor.execute("""
    SELECT 
        CASE 
            WHEN date IS NULL THEN 'NULL'
            WHEN date = '' THEN 'Пустая строка'
            WHEN date LIKE '____-__-__' THEN 'ГГГГ-ММ-ДД'
            WHEN date LIKE '__.__.____' THEN 'ДД.ММ.ГГГГ'
            ELSE 'Другой формат'
        END as format,
        COUNT(*) as count
    FROM production
    GROUP BY format
    ORDER BY count DESC
""")
stats = cursor.fetchall()
for fmt, count in stats:
    print(f"  - {fmt}: {count} записей")

# Проверка выручки по месяцам
print("\n💰 Выручка по месяцам (простой запрос):")
cursor.execute("""
    SELECT 
        substr(date, 1, 7) as month,
        SUM(revenue) as total_revenue,
        COUNT(*) as transactions
    FROM production 
    WHERE revenue IS NOT NULL 
        AND date IS NOT NULL
        AND date != ''
    GROUP BY substr(date, 1, 7)
    ORDER BY month DESC
    LIMIT 10
""")
months = cursor.fetchall()

if months:
    print("✅ Данные найдены:")
    for month, revenue, transactions in months:
        print(f"  - {month}: {revenue:,.0f} руб. ({transactions} транзакций)")
else:
    print("❌ Нет данных для группировки по месяцам")
    
    # Альтернатива: покажем любые данные
    print("\n📋 Пример любых данных из production:")
    cursor.execute("""
        SELECT date, product_name, revenue 
        FROM production 
        WHERE revenue IS NOT NULL 
        ORDER BY revenue DESC 
        LIMIT 5
    """)
    samples = cursor.fetchall()
    for date, product, revenue in samples:
        print(f"  - {date} | {product}: {revenue:,.0f} руб.")

conn.close()# Создайте файл test_dates.py
import sqlite3

conn = sqlite3.connect('rosatom_database.db')
cursor = conn.cursor()

print("🔍 Проверка формата дат в production:")
print("="*60)

# Проверка структуры
cursor.execute("SELECT date FROM production WHERE date IS NOT NULL LIMIT 5")
dates = cursor.fetchall()

print("Примеры дат:")
for i, (date,) in enumerate(dates, 1):
    print(f"  {i}. '{date}' (длина: {len(date) if date else 0})")

# Проверка разных форматов
print("\n📊 Статистика по форматам дат:")
cursor.execute("""
    SELECT 
        CASE 
            WHEN date IS NULL THEN 'NULL'
            WHEN date = '' THEN 'Пустая строка'
            WHEN date LIKE '____-__-__' THEN 'ГГГГ-ММ-ДД'
            WHEN date LIKE '__.__.____' THEN 'ДД.ММ.ГГГГ'
            ELSE 'Другой формат'
        END as format,
        COUNT(*) as count
    FROM production
    GROUP BY format
    ORDER BY count DESC
""")
stats = cursor.fetchall()
for fmt, count in stats:
    print(f"  - {fmt}: {count} записей")

# Проверка выручки по месяцам
print("\n💰 Выручка по месяцам (простой запрос):")
cursor.execute("""
    SELECT 
        substr(date, 1, 7) as month,
        SUM(revenue) as total_revenue,
        COUNT(*) as transactions
    FROM production 
    WHERE revenue IS NOT NULL 
        AND date IS NOT NULL
        AND date != ''
    GROUP BY substr(date, 1, 7)
    ORDER BY month DESC
    LIMIT 10
""")
months = cursor.fetchall()

if months:
    print("✅ Данные найдены:")
    for month, revenue, transactions in months:
        print(f"  - {month}: {revenue:,.0f} руб. ({transactions} транзакций)")
else:
    print("❌ Нет данных для группировки по месяцам")
    
    # Альтернатива: покажем любые данные
    print("\n📋 Пример любых данных из production:")
    cursor.execute("""
        SELECT date, product_name, revenue 
        FROM production 
        WHERE revenue IS NOT NULL 
        ORDER BY revenue DESC 
        LIMIT 5
    """)
    samples = cursor.fetchall()
    for date, product, revenue in samples:
        print(f"  - {date} | {product}: {revenue:,.0f} руб.")

conn.close()