import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import re

def diagnose_sales_dynamics():
    """Диагностика проблемы с динамикой продаж"""
    print("🔍 Диагностика запроса 'динамика продаж за последний год'")
    print("="*60)
    
    try:
        conn = sqlite3.connect('rosatom_database.db')
        cursor = conn.cursor()
        
        # 1. Проверим структуру таблицы production
        print("1. 📊 Структура таблицы production:")
        cursor.execute("PRAGMA table_info(production)")
        columns = cursor.fetchall()
        for col in columns:
            print(f"   - {col[1]} ({col[2]})")
        
        # 2. Проверим данные в таблице
        print("\n2. 📈 Проверка данных в production:")
        cursor.execute("SELECT COUNT(*) FROM production")
        total = cursor.fetchone()[0]
        print(f"   - Всего записей: {total:,}")
        
        # 3. Проверим диапазон дат
        print("\n3. 📅 Проверка дат:")
        cursor.execute("SELECT MIN(date), MAX(date) FROM production WHERE date IS NOT NULL")
        min_date, max_date = cursor.fetchone()
        print(f"   - Минимальная дата: {min_date}")
        print(f"   - Максимальная дата: {max_date}")
        
        # 4. Проверим формат дат
        print("\n4. 🔄 Проверка формата дат:")
        cursor.execute("SELECT date, typeof(date) FROM production WHERE date IS NOT NULL LIMIT 5")
        sample_dates = cursor.fetchall()
        for i, (date_val, date_type) in enumerate(sample_dates, 1):
            print(f"   {i}. '{date_val}' (тип: {date_type})")
        
        # 5. Проверим наличие данных за последний год
        print("\n5. 📊 Данные за последний год:")
        one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
        
        # Попробуем разные форматы дат
        test_queries = [
            (f"SELECT COUNT(*) FROM production WHERE date >= '{one_year_ago}'", "Прямое сравнение"),
            (f"SELECT COUNT(*) FROM production WHERE strftime('%Y-%m-%d', date) >= '{one_year_ago}'", "strftime форматирование"),
            (f"SELECT COUNT(*) FROM production WHERE date >= date('now', '-1 year')", "SQLite date() функция"),
            (f"SELECT COUNT(*) FROM production WHERE date >= '2023-01-01'", "Фиксированная дата 2023"),
            (f"SELECT COUNT(*) FROM production WHERE date >= '2022-01-01'", "Фиксированная дата 2022")
        ]
        
        for sql, description in test_queries:
            try:
                cursor.execute(sql)
                count = cursor.fetchone()[0]
                print(f"   - {description}: {count:,} записей")
            except Exception as e:
                print(f"   - {description}: ОШИБКА - {str(e)}")
        
        # 6. Проверим выручку
        print("\n6. 💰 Проверка выручки:")
        cursor.execute("SELECT revenue FROM production WHERE revenue IS NOT NULL LIMIT 5")
        revenues = cursor.fetchall()
        for i, (revenue,) in enumerate(revenues, 1):
            print(f"   {i}. {revenue}")
        
        # 7. Тестовый запрос для динамики продаж
        print("\n7. 🧪 Тестовые SQL запросы:")
        
        test_sqls = [
            # Простой запрос - проверяем наличие данных
            """
            SELECT 'Проверка' as test, COUNT(*) as total_records 
            FROM production 
            WHERE date IS NOT NULL AND revenue IS NOT NULL
            """,
            
            # Без временного фильтра
            """
            SELECT 
                strftime('%Y-%m', date) as month,
                SUM(revenue) as total_revenue
            FROM production 
            WHERE date IS NOT NULL AND revenue IS NOT NULL
            GROUP BY strftime('%Y-%m', date)
            ORDER BY month
            LIMIT 10
            """,
            
            # С проверкой формата даты
            """
            SELECT 
                substr(date, 1, 7) as month,
                SUM(revenue) as total_revenue
            FROM production 
            WHERE date LIKE '____-__-__'
                AND revenue IS NOT NULL
            GROUP BY substr(date, 1, 7)
            ORDER BY month
            LIMIT 10
            """,
            
            # Самый простой запрос
            """
            SELECT date, revenue, product_name 
            FROM production 
            WHERE date IS NOT NULL 
            ORDER BY date DESC 
            LIMIT 10
            """
        ]
        
        for i, sql in enumerate(test_sqls, 1):
            print(f"\n   Тест {i}:")
            try:
                cursor.execute(sql)
                results = cursor.fetchall()
                if results:
                    print(f"     Успешно! Найдено {len(results)} записей")
                    for j, row in enumerate(results[:3], 1):  # Покажем первые 3
                        print(f"     {j}. {row}")
                    if len(results) > 3:
                        print(f"     ... и еще {len(results)-3} записей")
                else:
                    print(f"     Нет данных")
            except Exception as e:
                print(f"     ОШИБКА: {str(e)}")
        
        # 8. Проверим, какие данные есть в принципе
        print("\n8. 🔍 Что есть в данных:")
        
        # Проверяем наличие колонки date и ее содержимое
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN date IS NULL THEN 'NULL даты'
                    WHEN date = '' THEN 'Пустые даты'
                    WHEN date LIKE '____-__-__' THEN 'Правильный формат (ГГГГ-ММ-ДД)'
                    ELSE 'Другой формат'
                END as date_status,
                COUNT(*) as count
            FROM production 
            GROUP BY date_status
            ORDER BY count DESC
        """)
        date_stats = cursor.fetchall()
        for status, count in date_stats:
            print(f"   - {status}: {count:,}")
        
        # Проверяем наличие выручки
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN revenue IS NULL THEN 'NULL выручки'
                    WHEN revenue = 0 THEN 'Нулевая выручка'
                    WHEN revenue > 0 THEN 'Положительная выручка'
                    ELSE 'Отрицательная выручка'
                END as revenue_status,
                COUNT(*) as count
            FROM production 
            GROUP BY revenue_status
            ORDER BY count DESC
        """)
        revenue_stats = cursor.fetchall()
        for status, count in revenue_stats:
            print(f"   - {status}: {count:,}")
        
        # 9. Покажем образец данных
        print("\n9. 📄 Образец данных:")
        cursor.execute("""
            SELECT date, product_name, quantity, revenue, department
            FROM production 
            WHERE date IS NOT NULL AND revenue IS NOT NULL
            ORDER BY RANDOM()
            LIMIT 5
        """)
        samples = cursor.fetchall()
        for i, row in enumerate(samples, 1):
            date, product, qty, revenue, dept = row
            print(f"   {i}. {date} | {product[:20]:20} | {qty:>6} ед. | {revenue:>12,.2f} руб. | {dept}")
        
        conn.close()
        
        print("\n" + "="*60)
        print("📋 РЕКОМЕНДАЦИИ:")
        
        if total == 0:
            print("❌ Таблица production пуста! Нужно пересоздать базу данных.")
        elif min_date is None:
            print("❌ В таблице нет дат! Проверьте данные.")
        elif 'NULL выручки' in [s[0] for s in revenue_stats] and revenue_stats[0][0] == 'NULL выручки':
            print("❌ В большинстве записей нет выручки! Проверьте данные.")
        else:
            print("✅ Данные есть. Проблема в SQL запросе.")
            print("\n📝 Пример рабочего SQL для динамики продаж:")
            print("""
            SELECT 
                substr(date, 1, 7) as month,
                SUM(revenue) as total_revenue,
                SUM(quantity) as total_quantity
            FROM production 
            WHERE date IS NOT NULL 
                AND revenue IS NOT NULL
                AND date LIKE '____-__-__'
            GROUP BY substr(date, 1, 7)
            ORDER BY month
            """)
        
    except Exception as e:
        print(f"❌ Ошибка диагностики: {e}")
        import traceback
        traceback.print_exc()

def fix_date_format():
    """Исправление формата дат в таблице production"""
    print("\n🛠️ Исправление формата дат...")
    
    try:
        conn = sqlite3.connect('rosatom_database.db')
        cursor = conn.cursor()
        
        # Создаем копию таблицы с исправленными датами
        cursor.execute("""
            CREATE TABLE production_fixed AS
            SELECT 
                production_id,
                CASE 
                    -- Если дата в формате ГГГГ-ММ-ДД
                    WHEN date LIKE '____-__-__' THEN date
                    -- Если дата в формате ДД.ММ.ГГГГ
                    WHEN date LIKE '__.__.____' THEN 
                        substr(date, 7, 4) || '-' || 
                        substr(date, 4, 2) || '-' || 
                        substr(date, 1, 2)
                    -- Если что-то другое, пытаемся преобразовать
                    WHEN date IS NOT NULL AND date != '' THEN 
                        substr(date, 1, 4) || '-' || 
                        substr(date, 6, 2) || '-' || 
                        substr(date, 9, 2)
                    ELSE NULL
                END as date,
                product_name,
                product_category,
                quantity,
                unit_price,
                revenue,
                cost,
                profit,
                department,
                project_id,
                customer_id,
                location_id,
                quality_score,
                production_line
            FROM production
        """)
        
        # Удаляем старую таблицу и переименовываем новую
        cursor.execute("DROP TABLE IF EXISTS production_old")
        cursor.execute("ALTER TABLE production RENAME TO production_old")
        cursor.execute("ALTER TABLE production_fixed RENAME TO production")
        
        # Создаем индексы
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_production_date ON production(date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_production_revenue ON production(revenue)")
        
        conn.commit()
        
        # Проверяем результат
        cursor.execute("SELECT date FROM production WHERE date IS NOT NULL LIMIT 5")
        new_dates = cursor.fetchall()
        
        print("✅ Формат дат исправлен!")
        print("   Примеры новых дат:")
        for i, (date,) in enumerate(new_dates, 1):
            print(f"   {i}. {date}")
        
        cursor.execute("SELECT COUNT(*) FROM production WHERE date IS NOT NULL")
        valid_dates = cursor.fetchone()[0]
        print(f"   Всего корректных дат: {valid_dates:,}")
        
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка исправления формата дат: {e}")
        return False

def test_dynamic_query():
    """Тестирование запроса динамики продаж"""
    print("\n🚀 Тестирование запроса динамики продаж...")
    
    test_queries = [
        # Самый простой - проверка данных
        """
        SELECT 'Данные есть' as status, COUNT(*) as count 
        FROM production 
        WHERE date IS NOT NULL AND revenue IS NOT NULL
        """,
        
        # Месячная статистика без фильтра по году
        """
        SELECT 
            substr(date, 1, 7) as month,
            SUM(revenue) as total_revenue,
            SUM(quantity) as total_quantity,
            COUNT(*) as transactions
        FROM production 
        WHERE date IS NOT NULL 
            AND revenue IS NOT NULL
            AND date LIKE '____-__-__'
        GROUP BY substr(date, 1, 7)
        ORDER BY month DESC
        LIMIT 12
        """,
        
        # С фильтром за последний год
        """
        SELECT 
            substr(date, 1, 7) as month,
            SUM(revenue) as total_revenue
        FROM production 
        WHERE date IS NOT NULL 
            AND revenue IS NOT NULL
            AND date LIKE '____-__-__'
            AND date >= '2023-01-01'
        GROUP BY substr(date, 1, 7)
        ORDER BY month
        """,
        
        # Альтернативный вариант с date() функцией
        """
        SELECT 
            strftime('%Y-%m', date) as month,
            SUM(revenue) as total_revenue
        FROM production 
        WHERE date IS NOT NULL 
            AND revenue IS NOT NULL
            AND date >= date('now', '-1 year')
        GROUP BY strftime('%Y-%m', date)
        ORDER BY month
        """
    ]
    
    try:
        conn = sqlite3.connect('rosatom_database.db')
        cursor = conn.cursor()
        
        for i, sql in enumerate(test_queries, 1):
            print(f"\nТест {i}:")
            print(f"SQL: {sql[:100]}...")
            
            try:
                cursor.execute(sql)
                results = cursor.fetchall()
                
                if results:
                    print(f"✅ Успешно! Найдено {len(results)} записей")
                    
                    # Для простого запроса
                    if i == 1:
                        status, count = results[0]
                        print(f"   {status}: {count:,}")
                    
                    # Для остальных запросов
                    else:
                        headers = ['Месяц', 'Выручка', 'Количество', 'Транзакции'][:len(results[0])]
                        print(f"   {' | '.join(headers)}")
                        for row in results[:5]:  # Покажем первые 5
                            formatted = []
                            for val in row:
                                if isinstance(val, (int, float)):
                                    if val >= 1000:
                                        formatted.append(f"{val:,.0f}")
                                    else:
                                        formatted.append(str(val))
                                else:
                                    formatted.append(str(val))
                            print(f"   {' | '.join(formatted)}")
                        
                        if len(results) > 5:
                            print(f"   ... и еще {len(results)-5} месяцев")
                else:
                    print("❌ Нет данных")
                    
            except Exception as e:
                print(f"❌ ОШИБКА: {str(e)}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")

if __name__ == '__main__':
    print("🔧 ДИАГНОСТИКА ЗАПРОСА 'ДИНАМИКА ПРОДАЖ'")
    print("="*60)
    
    # Шаг 1: Диагностика
    diagnose_sales_dynamics()
    
    print("\n" + "="*60)
    input("Нажмите Enter для продолжения...")
    
    # Шаг 2: Исправление формата дат (если нужно)
    fix_date_format()
    
    print("\n" + "="*60)
    input("Нажмите Enter для тестирования запросов...")
    
    # Шаг 3: Тестирование
    test_dynamic_query()