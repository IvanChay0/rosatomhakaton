import sqlite3
from datetime import datetime, timedelta
import random

def add_revenue_data():
    """Добавление тестовых данных о выручке в базу данных"""
    
    print("🔄 Подключение к базе данных...")
    conn = sqlite3.connect('rosatom_database.db')
    cursor = conn.cursor()
    
    # 1. Сначала проверим текущие данные
    cursor.execute("SELECT COUNT(*) FROM production")
    total_count = cursor.fetchone()[0]
    print(f"📊 Всего записей в production: {total_count}")
    
    cursor.execute("SELECT COUNT(*) FROM production WHERE revenue > 0")
    revenue_count = cursor.fetchone()[0]
    print(f"📊 Записей с revenue > 0: {revenue_count}")
    
    # 2. Если есть записи без выручки, обновим их
    if revenue_count == 0:
        print("⚠️ В базе нет данных о выручке. Добавляем...")
        
        # Список отделов
        departments = ['Ядерная энергетика', 'Научные исследования', 'Безопасность', 
                      'Логистика', 'Инжиниринг', 'IT']
        
        # Список продуктов
        products = ['ТВЭЛ', 'Оборудование АЭС', 'Изотопы', 'Научные отчеты', 'Консультации']
        
        # Добавляем 50 тестовых записей с выручкой
        for i in range(1, 51):
            date = (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d')
            product = random.choice(products)
            quantity = random.randint(10, 500)
            price = random.randint(10000, 500000)
            revenue = quantity * price  # Вычисляем выручку
            
            cursor.execute("""
                INSERT INTO production (date, product_name, quantity, revenue, department, project_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                date,
                product,
                quantity,
                revenue,
                random.choice(departments),
                random.randint(1, 10)
            ))
        
        print(f"✅ Добавлено 50 новых записей с выручкой")
    
    else:
        print("🔄 Обновляем существующие записи без выручки...")
        
        # Обновляем записи, где revenue = 0 или NULL
        cursor.execute("SELECT production_id, quantity FROM production WHERE revenue IS NULL OR revenue = 0")
        records_to_update = cursor.fetchall()
        
        updated_count = 0
        for record_id, quantity in records_to_update:
            if quantity == 0:
                quantity = random.randint(10, 100)  # Устанавливаем случайное количество
            
            price = random.randint(1000, 100000)
            revenue = quantity * price
            
            cursor.execute("""
                UPDATE production 
                SET revenue = ?, 
                    product_name = CASE 
                        WHEN product_name IS NULL OR product_name = '' THEN 'ТВЭЛ'
                        ELSE product_name
                    END
                WHERE production_id = ?
            """, (revenue, record_id))
            
            updated_count += 1
        
        print(f"✅ Обновлено {updated_count} записей")
    
    # 3. Проверяем результат
    cursor.execute("SELECT SUM(revenue) FROM production")
    total_revenue = cursor.fetchone()[0]
    print(f"💰 Общая выручка: {total_revenue:,.0f} ₽")
    
    cursor.execute("SELECT COUNT(*) FROM production WHERE revenue > 0")
    final_count = cursor.fetchone()[0]
    print(f"📈 Записей с выручкой: {final_count}")
    
    # 4. Покажем примеры данных
    cursor.execute("""
        SELECT date, product_name, quantity, revenue 
        FROM production 
        WHERE revenue > 0 
        ORDER BY RANDOM() 
        LIMIT 5
    """)
    samples = cursor.fetchall()
    
    print("\n🎯 Примеры записей с выручкой:")
    print("-" * 60)
    for sample in samples:
        print(f"Дата: {sample[0]}, Продукт: {sample[1]}, Кол-во: {sample[2]}, Выручка: {sample[3]:,.0f} ₽")
    print("-" * 60)
    
    # Сохраняем изменения
    conn.commit()
    conn.close()
    
    print("\n✅ Тестовые данные о выручке успешно добавлены!")
    return True

def check_database_schema():
    """Проверка структуры базы данных"""
    
    print("\n🔍 Проверка структуры базы данных...")
    
    conn = sqlite3.connect('rosatom_database.db')
    cursor = conn.cursor()
    
    # Проверим все таблицы
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = cursor.fetchall()
    
    print("📋 Таблицы в базе данных:")
    for table in tables:
        table_name = table[0]
        print(f"\n  Таблица: {table_name}")
        
        # Структура таблицы
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        
        for col in columns:
            col_name = col[1]
            col_type = col[2]
            nullable = "NULL" if col[3] == 1 else "NOT NULL"
            pk = "PRIMARY KEY" if col[5] == 1 else ""
            print(f"    - {col_name} ({col_type}) {nullable} {pk}")
    
    conn.close()

if __name__ == '__main__':
    print("=" * 60)
    print("ДОБАВЛЕНИЕ ТЕСТОВЫХ ДАННЫХ О ВЫРУЧКЕ")
    print("=" * 60)
    
    try:
        # Проверим структуру
        check_database_schema()
        
        # Добавим данные
        add_revenue_data()
        
        print("\n" + "=" * 60)
        print("✅ Готово! Перезапустите Flask приложение")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        print("Проверьте, существует ли файл rosatom_database.db")