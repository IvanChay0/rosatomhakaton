import openai
import os
import json
from dotenv import load_dotenv
import re
import sqlite3
import pandas as pd
from datetime import datetime, timedelta

load_dotenv()

class SQLGenerator:
    def __init__(self):
        self.api_key = os.getenv('OPENROUTER_API_KEY')
        self.api_url = os.getenv('OPENROUTER_API_URL')
        self.model = os.getenv('MODEL_NAME', 'qwen/qwen3-vl-235b-a22b-instruct')
        
        # Настройка клиента OpenAI для работы с OpenRouter
        self.client = openai.OpenAI(
            base_url=self.api_url,
            api_key=self.api_key
        )
        
        # Получаем реальные таблицы из базы данных
        self.available_tables = self._get_available_tables()
        print(f"📊 Доступные таблицы: {self.available_tables}")
        
        # Кэш схемы таблиц
        self.table_schemas = {}
        self._load_table_schemas()
        
        # Паттерны для специальных запросов
        self.special_patterns = {
            r'динамик[а-я]* продаж': self._handle_sales_dynamics,
            r'тренд[а-я]* продаж': self._handle_sales_dynamics,
            r'изменени[е-я]* продаж': self._handle_sales_dynamics,
            r'продажи за последний год': self._handle_sales_dynamics,
            r'продажи за год': self._handle_sales_dynamics,
            r'месячн[а-я]* продаж[а-я]*': self._handle_monthly_sales,
            r'еженедельн[а-я]* продаж[а-я]*': self._handle_weekly_sales,
            r'дневн[а-я]* продаж[а-я]*': self._handle_daily_sales,
            r'продажи по месяцам': self._handle_monthly_sales,
            r'продажи по неделям': self._handle_weekly_sales,
            r'продажи по дням': self._handle_daily_sales,
            r'график продаж': self._handle_sales_dynamics,
            r'выручка за период': self._handle_sales_dynamics,
        }

    def _get_available_tables(self):
        """Получение реальных таблиц из базы данных"""
        try:
            conn = sqlite3.connect('rosatom_database.db')
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()
            return tables
        except Exception as e:
            print(f"Ошибка при получении таблиц из БД: {e}")
            return ['employees', 'projects', 'production', 'equipment', 'safety_incidents']
    
    def _load_table_schemas(self):
        """Загрузка схем всех таблиц"""
        try:
            conn = sqlite3.connect('rosatom_database.db')
            cursor = conn.cursor()
            
            for table in self.available_tables:
                cursor.execute(f"PRAGMA table_info({table})")
                columns = cursor.fetchall()
                column_info = []
                for col in columns:
                    column_info.append({
                        'name': col[1],
                        'type': col[2],
                        'nullable': col[3] == 0
                    })
                self.table_schemas[table] = column_info
            
            conn.close()
            print(f"📋 Загружены схемы {len(self.table_schemas)} таблиц")
        except Exception as e:
            print(f"Ошибка загрузки схем таблиц: {e}")

    def _handle_sales_dynamics(self, query_lower):
        """Обработка запросов о динамике продаж"""
        if 'production' not in self.available_tables:
            return "SELECT 'Таблица production не найдена' as error;"
        
        # Проверяем формат дат в таблице
        try:
            conn = sqlite3.connect('rosatom_database.db')
            cursor = conn.cursor()
            cursor.execute("SELECT date FROM production LIMIT 1")
            sample_date = cursor.fetchone()
            conn.close()
            
            if sample_date:
                date_str = str(sample_date[0])
                print(f"📅 Формат даты в базе: '{date_str}'")
        except:
            pass
        
        # Универсальный SQL для динамики продаж
        return """
        WITH cleaned_dates AS (
            SELECT 
                CASE 
                    WHEN date LIKE '____-__-__%' THEN substr(date, 1, 10)
                    WHEN date LIKE '__.__.____%' THEN 
                        substr(date, 7, 4) || '-' || 
                        substr(date, 4, 2) || '-' || 
                        substr(date, 1, 2)
                    ELSE substr(date, 1, 10)
                END as clean_date,
                revenue,
                quantity,
                product_name,
                department
            FROM production
            WHERE revenue IS NOT NULL 
                AND revenue > 0
        ),
        monthly_aggregation AS (
            SELECT 
                substr(clean_date, 1, 7) as month,
                SUM(revenue) as total_revenue,
                SUM(quantity) as total_quantity,
                COUNT(*) as transaction_count,
                COUNT(DISTINCT product_name) as unique_products
            FROM cleaned_dates
            WHERE clean_date LIKE '____-__-__'
            GROUP BY substr(clean_date, 1, 7)
            HAVING COUNT(*) > 0
        )
        SELECT 
            month,
            COALESCE(total_revenue, 0) as total_revenue,
            COALESCE(total_quantity, 0) as total_quantity,
            transaction_count,
            unique_products,
            CASE 
                WHEN total_revenue > 0 THEN '📈 Данные есть'
                ELSE '📊 Нет выручки'
            END as status
        FROM monthly_aggregation
        WHERE month IS NOT NULL 
            AND month != '1900-01'
        ORDER BY month
        LIMIT 24;
        """
    
    def _handle_monthly_sales(self, query_lower):
        """Обработка ежемесячных продаж"""
        if 'production' not in self.available_tables:
            return "SELECT 'Таблица production не найдена' as error;"
        
        return """
        WITH cleaned_dates AS (
            SELECT 
                CASE 
                    WHEN date LIKE '____-__-__%' THEN substr(date, 1, 10)
                    WHEN date LIKE '__.__.____%' THEN 
                        substr(date, 7, 4) || '-' || 
                        substr(date, 4, 2) || '-' || 
                        substr(date, 1, 2)
                    ELSE substr(date, 1, 10)
                END as clean_date,
                revenue,
                product_name
            FROM production
            WHERE revenue IS NOT NULL 
                AND revenue > 0
        )
        SELECT 
            substr(clean_date, 1, 7) as month,
            SUM(revenue) as total_revenue,
            COUNT(*) as transaction_count,
            COUNT(DISTINCT product_name) as unique_products
        FROM cleaned_dates
        WHERE clean_date LIKE '____-__-__'
        GROUP BY substr(clean_date, 1, 7)
        HAVING COUNT(*) > 0
        ORDER BY month
        LIMIT 12;
        """
    
    def _handle_weekly_sales(self, query_lower):
        """Обработка еженедельных продаж"""
        if 'production' not in self.available_tables:
            return "SELECT 'Таблица production не найдена' as error;"
        
        return """
        WITH cleaned_dates AS (
            SELECT 
                CASE 
                    WHEN date LIKE '____-__-__%' THEN substr(date, 1, 10)
                    WHEN date LIKE '__.__.____%' THEN 
                        substr(date, 7, 4) || '-' || 
                        substr(date, 4, 2) || '-' || 
                        substr(date, 1, 2)
                    ELSE substr(date, 1, 10)
                END as clean_date,
                revenue,
                product_name
            FROM production
            WHERE revenue IS NOT NULL 
                AND revenue > 0
        ),
        valid_dates AS (
            SELECT *
            FROM cleaned_dates
            WHERE clean_date LIKE '____-__-__'
        )
        SELECT 
            strftime('%Y-%W', clean_date) as week,
            MIN(clean_date) as week_start,
            SUM(revenue) as total_revenue,
            COUNT(*) as transaction_count
        FROM valid_dates
        GROUP BY strftime('%Y-%W', clean_date)
        HAVING COUNT(*) > 0
        ORDER BY week_start
        LIMIT 20;
        """
    
    def _handle_daily_sales(self, query_lower):
        """Обработка ежедневных продаж"""
        if 'production' not in self.available_tables:
            return "SELECT 'Таблица production не найдена' as error;"
        
        # Определяем период
        if 'последнюю неделю' in query_lower:
            days = 7
        elif 'последний месяц' in query_lower:
            days = 30
        else:
            days = 30
        
        return f"""
        WITH cleaned_dates AS (
            SELECT 
                CASE 
                    WHEN date LIKE '____-__-__%' THEN substr(date, 1, 10)
                    WHEN date LIKE '__.__.____%' THEN 
                        substr(date, 7, 4) || '-' || 
                        substr(date, 4, 2) || '-' || 
                        substr(date, 1, 2)
                    ELSE substr(date, 1, 10)
                END as clean_date,
                revenue,
                product_name,
                quantity
            FROM production
            WHERE revenue IS NOT NULL 
                AND revenue > 0
        ),
        valid_dates AS (
            SELECT *
            FROM cleaned_dates
            WHERE clean_date LIKE '____-__-__'
        )
        SELECT 
            clean_date as day,
            SUM(revenue) as daily_revenue,
            SUM(quantity) as daily_quantity,
            COUNT(*) as transaction_count,
            COUNT(DISTINCT product_name) as unique_products
        FROM valid_dates
        GROUP BY clean_date
        HAVING COUNT(*) > 0
        ORDER BY day DESC
        LIMIT {days};
        """
    
    def generate_sql(self, natural_language_query, schema_info):
        """Генерация SQL запроса на основе естественного языка"""
        
        print(f"\n{'='*60}")
        print(f"🤔 Анализ запроса: '{natural_language_query}'")
        
        query_lower = natural_language_query.lower()
        
        # Проверяем специальные паттерны для временных запросов
        for pattern, handler in self.special_patterns.items():
            if re.search(pattern, query_lower):
                print(f"🎯 Распознан специальный паттерн: {pattern}")
                sql_query = handler(query_lower)
                if sql_query:
                    print(f"📝 Сгенерирован специальный SQL: {sql_query[:200]}...")
                    print(f"{'='*60}")
                    return sql_query
        
        # Сначала пытаемся определить наиболее подходящую таблицу
        target_table = self._determine_target_table(natural_language_query)
        print(f"🎯 Определена таблица: {target_table}")
        
        # Если не удалось определить таблицу, используем LLM
        if not target_table and self.available_tables:
            print("🔄 Использую LLM для определения таблицы...")
            return self._generate_sql_with_llm(natural_language_query, schema_info)
        
        # Генерируем SQL для определенной таблицы
        sql_query = self._generate_sql_for_table(natural_language_query, target_table)
        
        print(f"📝 Сгенерирован SQL: {sql_query}")
        print(f"{'='*60}")
        
        return sql_query
    
    def _determine_target_table(self, query):
        """Определение наиболее подходящей таблицы по запросу"""
        query_lower = query.lower()
        
        # Обновленная карта ключевых слов с приоритетами
        table_keywords = {
            'employees': ['сотрудник', 'employee', 'работник', 'персонал', 'зарплат', 'salary', 
                         'должность', 'position', 'отдел', 'department', 'эффективность', 
                         'performance', 'прием', 'hire', 'устроился'],
            
            'projects': ['проект', 'project', 'бюджет', 'budget', 'статус', 'status', 
                        'начало', 'start', 'окончание', 'end', 'руководитель', 'manager',
                        'планирование', 'planning', 'завершен', 'completed'],
            
            'production': ['продаж', 'sale', 'production', 'производств', 'товар', 'product', 
                          'выручк', 'revenue', 'доход', 'income', 'количество', 'quantity',
                          'топ', 'top', 'лучш', 'лидер', 'продукт', 'товарооборот',
                          'динамик', 'тренд', 'график', 'изменени', 'период', 'год', 'месяц', 'недел', 'день'],
            
            'equipment': ['оборудован', 'equipment', 'техника', 'машина', 'стоимость', 'cost',
                         'покупк', 'purchase', 'обслуживан', 'maintenance', 'тип', 'type',
                         'исправно', 'working', 'ремонт', 'repair'],
            
            'safety_incidents': ['инцидент', 'incident', 'безопасность', 'safety', 'авария', 
                                'происшествие', 'серьезность', 'severity', 'решен', 'resolved',
                                'время решения', 'resolution time', 'описание', 'description']
        }
        
        # Проверяем наличие таблиц
        available_tables_set = set(self.available_tables)
        
        # Ищем таблицу с наибольшим количеством совпадений ключевых слов
        best_table = None
        best_score = 0
        
        for table, keywords in table_keywords.items():
            if table not in available_tables_set:
                continue
                
            score = sum(1 for keyword in keywords if keyword in query_lower)
            
            # Дополнительные бонусы для специфичных запросов
            if table == 'production':
                if any(word in query_lower for word in ['динамик', 'тренд', 'график', 'год', 'месяц', 'недел', 'день']):
                    score += 5
                if 'топ' in query_lower:
                    score += 3
                if 'выручк' in query_lower or 'продаж' in query_lower:
                    score += 2
            
            if score > best_score:
                best_score = score
                best_table = table
        
        # Если набрали достаточно баллов, возвращаем таблицу
        if best_score >= 1:
            return best_table
        
        # Если не нашли явного совпадения, используем эвристики
        if 'сотрудник' in query_lower or 'employee' in query_lower:
            return 'employees' if 'employees' in available_tables_set else None
        elif 'проект' in query_lower or 'project' in query_lower:
            return 'projects' if 'projects' in available_tables_set else None
        elif 'продаж' in query_lower or 'товар' in query_lower or 'топ' in query_lower:
            return 'production' if 'production' in available_tables_set else None
        elif 'оборудован' in query_lower:
            return 'equipment' if 'equipment' in available_tables_set else None
        elif 'инцидент' in query_lower or 'безопасность' in query_lower:
            return 'safety_incidents' if 'safety_incidents' in available_tables_set else None
        
        return None
    
    def _generate_sql_for_table(self, query, table_name):
        """Генерация SQL для конкретной таблицы"""
        query_lower = query.lower()
        
        # Базовые запросы для каждой таблицы
        base_queries = {
            'employees': {
                'общие': "SELECT * FROM employees LIMIT 10",
                'сколько': "SELECT department, COUNT(*) as employee_count FROM employees GROUP BY department ORDER BY employee_count DESC",
                'отдел': "SELECT department, COUNT(*) as employee_count FROM employees GROUP BY department ORDER BY employee_count DESC",
                'зарплат': "SELECT first_name, last_name, department, salary FROM employees ORDER BY salary DESC LIMIT 10",
                'эффективность': "SELECT first_name, last_name, department, performance_score FROM employees ORDER BY performance_score DESC LIMIT 10",
                'топ': "SELECT first_name, last_name, department, performance_score FROM employees ORDER BY performance_score DESC LIMIT 5",
                'все': "SELECT * FROM employees LIMIT 20",
                'средняя зарплата': "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department ORDER BY avg_salary DESC"
            },
            'projects': {
                'общие': "SELECT * FROM projects LIMIT 10",
                'бюджет': "SELECT project_name, budget, status FROM projects ORDER BY budget DESC LIMIT 10",
                'статус': "SELECT status, COUNT(*) as count FROM projects GROUP BY status",
                'работа': "SELECT project_name, budget, start_date FROM projects WHERE status = 'В работе' ORDER BY budget DESC",
                'сравн': "SELECT project_name, budget, actual_cost FROM projects ORDER BY budget DESC LIMIT 10",
                'все': "SELECT * FROM projects ORDER BY start_date DESC LIMIT 15",
                'активные проекты': "SELECT project_name, budget, start_date, manager_id FROM projects WHERE status = 'В работе' ORDER BY start_date DESC"
            },
            'production': {
                'общие': """
                    SELECT 
                        substr(date, 1, 10) as date, 
                        product_name, 
                        revenue, 
                        quantity 
                    FROM production 
                    WHERE revenue IS NOT NULL 
                    ORDER BY date DESC 
                    LIMIT 10
                """,
                'динамика': self._handle_sales_dynamics('динамика продаж'),
                'ежемесячно': self._handle_monthly_sales('ежемесячные продажи'),
                'топ': """
                    SELECT 
                        product_name, 
                        SUM(revenue) as total_revenue,
                        SUM(quantity) as total_quantity
                    FROM production 
                    WHERE revenue IS NOT NULL
                    GROUP BY product_name 
                    ORDER BY total_revenue DESC 
                    LIMIT 5
                """,
                'продаж': """
                    SELECT 
                        product_name, 
                        SUM(revenue) as total_revenue,
                        COUNT(*) as transaction_count
                    FROM production 
                    WHERE revenue IS NOT NULL
                    GROUP BY product_name 
                    ORDER BY total_revenue DESC
                """,
                'выручк': """
                    SELECT 
                        product_name, 
                        SUM(revenue) as total_revenue
                    FROM production 
                    WHERE revenue IS NOT NULL
                    GROUP BY product_name 
                    ORDER BY total_revenue DESC
                """,
                'последний месяц': """
                    WITH cleaned_dates AS (
                        SELECT 
                            CASE 
                                WHEN date LIKE '____-__-__%' THEN substr(date, 1, 10)
                                WHEN date LIKE '__.__.____%' THEN 
                                    substr(date, 7, 4) || '-' || 
                                    substr(date, 4, 2) || '-' || 
                                    substr(date, 1, 2)
                                ELSE substr(date, 1, 10)
                            END as clean_date,
                            revenue,
                            product_name
                        FROM production
                        WHERE revenue IS NOT NULL 
                            AND revenue > 0
                    )
                    SELECT 
                        clean_date as date,
                        product_name, 
                        revenue 
                    FROM cleaned_dates 
                    WHERE clean_date LIKE '____-__-__'
                        AND clean_date >= date('now', '-1 month')
                    ORDER BY clean_date DESC 
                    LIMIT 20
                """,
                'товар': """
                    SELECT 
                        product_name, 
                        SUM(quantity) as total_quantity, 
                        SUM(revenue) as total_revenue,
                        COUNT(*) as transaction_count
                    FROM production 
                    WHERE revenue IS NOT NULL
                    GROUP BY product_name 
                    ORDER BY total_revenue DESC
                """,
                'все': """
                    SELECT 
                        substr(date, 1, 10) as date, 
                        product_name, 
                        revenue, 
                        quantity 
                    FROM production 
                    WHERE revenue IS NOT NULL
                    ORDER BY date DESC 
                    LIMIT 20
                """,
                'общая выручка': """
                    SELECT 
                        SUM(revenue) as total_revenue, 
                        SUM(quantity) as total_quantity,
                        COUNT(*) as total_transactions
                    FROM production 
                    WHERE revenue IS NOT NULL
                """,
                'по отделам': """
                    SELECT 
                        department, 
                        SUM(revenue) as department_revenue,
                        COUNT(*) as transaction_count
                    FROM production 
                    WHERE revenue IS NOT NULL
                    GROUP BY department 
                    ORDER BY department_revenue DESC
                """
            },
            'equipment': {
                'общие': "SELECT * FROM equipment LIMIT 10",
                'стоимость': "SELECT equipment_name, type, cost FROM equipment ORDER BY cost DESC LIMIT 10",
                'статус': "SELECT status, COUNT(*) as count FROM equipment GROUP BY status",
                'отдел': "SELECT department, COUNT(*) as equipment_count FROM equipment GROUP BY department ORDER BY equipment_count DESC",
                'ремонт': "SELECT * FROM equipment WHERE status = 'Требует ремонта' ORDER BY purchase_date",
                'все': "SELECT * FROM equipment ORDER BY purchase_date DESC LIMIT 15",
                'стоимость по типам': "SELECT type, SUM(cost) as total_cost FROM equipment GROUP BY type ORDER BY total_cost DESC"
            },
            'safety_incidents': {
                'общие': "SELECT * FROM safety_incidents ORDER BY date DESC LIMIT 10",
                'последний месяц': "SELECT * FROM safety_incidents WHERE date >= date('now', '-1 month') ORDER BY date DESC LIMIT 10",
                'серьезность': "SELECT severity, COUNT(*) as count FROM safety_incidents GROUP BY severity ORDER BY count DESC",
                'отдел': "SELECT department, COUNT(*) as incident_count FROM safety_incidents GROUP BY department ORDER BY incident_count DESC",
                'не решен': "SELECT * FROM safety_incidents WHERE resolved = 0 ORDER BY date DESC",
                'все': "SELECT * FROM safety_incidents ORDER BY date DESC LIMIT 15",
                'статистика': "SELECT severity, COUNT(*) as count, AVG(resolution_time_hours) as avg_resolution_time FROM safety_incidents GROUP BY severity"
            }
        }
        
        # Если таблицы нет в базовых запросах, возвращаем простой запрос
        if table_name not in base_queries:
            return f"SELECT * FROM {table_name} LIMIT 10;"
        
        # Определяем тип запроса
        query_type = self._determine_query_type(query_lower, table_name)
        print(f"📋 Тип запроса: {query_type}")
        
        # Получаем соответствующий SQL
        if query_type in base_queries[table_name]:
            sql = base_queries[table_name][query_type]
        else:
            # По умолчанию используем общий запрос
            sql = base_queries[table_name].get('общие', base_queries[table_name].get('все', f"SELECT * FROM {table_name} LIMIT 10"))
        
        # Добавляем ORDER BY для топ-запросов если его нет
        if 'топ' in query_lower and 'ORDER BY' not in sql.upper():
            # Пытаемся определить колонку для сортировки
            if table_name == 'production':
                sql = sql.replace('LIMIT', 'ORDER BY revenue DESC LIMIT') if 'ORDER BY' not in sql else sql
            elif table_name == 'employees':
                sql = sql.replace('LIMIT', 'ORDER BY performance_score DESC LIMIT') if 'ORDER BY' not in sql else sql
        
        return sql + ';' if not sql.endswith(';') else sql
    
    def _determine_query_type(self, query_lower, table_name):
        """Определение типа запроса"""
        if table_name == 'production':
            if any(word in query_lower for word in ['динамик', 'тренд', 'график', 'изменени']):
                return 'динамика'
            elif 'месячн' in query_lower:
                return 'ежемесячно'
            elif 'недел' in query_lower:
                return 'еженедельно'
            elif 'дневн' in query_lower or 'за день' in query_lower:
                return 'ежедневно'
        
        if any(word in query_lower for word in ['сколько', 'количество', 'count', 'число']):
            return 'сколько'
        elif any(word in query_lower for word in ['топ', 'лучш', 'первые', 'последние']):
            return 'топ'
        elif any(word in query_lower for word in ['сравн', 'compare', 'против']):
            return 'сравн'
        elif any(word in query_lower for word in ['выручк', 'revenue', 'доход']):
            return 'выручк'
        elif any(word in query_lower for word in ['зарплат', 'salary']):
            return 'зарплат'
        elif any(word in query_lower for word in ['бюджет', 'budget']):
            return 'бюджет'
        elif any(word in query_lower for word in ['последний месяц', 'за месяц', 'месяц']):
            return 'последний месяц'
        elif any(word in query_lower for word in ['отдел', 'department']):
            return 'отдел'
        elif any(word in query_lower for word in ['статус', 'status']):
            return 'статус'
        elif any(word in query_lower for word in ['работа', 'в работе']):
            return 'работа'
        elif any(word in query_lower for word in ['все', 'покажи все', 'весь']):
            return 'все'
        elif any(word in query_lower for word in ['средн', 'avg', 'average']):
            return 'средняя зарплата' if table_name == 'employees' else 'статистика'
        elif any(word in query_lower for word in ['общ', 'total', 'итого']):
            return 'общая выручка' if table_name == 'production' else 'стоимость по типам'
        else:
            return 'общие'
    
    # Остальные методы оставляем без изменений...
    def _generate_sql_with_llm(self, natural_language_query, schema_info):
        """Генерация SQL через LLM с улучшенным промптом"""
        try:
            prompt = f"""Пользователь спрашивает: "{natural_language_query}"

Доступные таблицы:
{', '.join(self.available_tables)}

ВАЖНО: Если запрос касается продаж, трендов, динамики, графиков - используй таблицу production.

Верни ТОЛЬКО SQL запрос для этого запроса (без объяснений, только SQL):

SQL:"""
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=300
            )
            
            sql_query = response.choices[0].message.content.strip()
            
            # Извлекаем SQL из ответа
            sql_query = self._extract_sql_from_response(sql_query)
            return sql_query
            
        except Exception as e:
            print(f"❌ Ошибка генерации SQL через LLM: {str(e)}")
            # Возвращаем fallback
            return "SELECT 'Ошибка генерации SQL' as error;"
    
    def _extract_sql_from_response(self, response_text):
        """Извлечение SQL запроса из текста ответа"""
        # Удаляем markdown блоки кода
        response_text = response_text.replace('```sql', '').replace('```', '').strip()
        
        # Ищем SQL запрос
        lines = response_text.split('\n')
        sql_lines = []
        
        for line in lines:
            line = line.strip()
            if line and not line.startswith('--'):
                sql_lines.append(line)
            if line.endswith(';'):
                break
        
        sql_query = ' '.join(sql_lines)
        
        # Если все еще нет запроса, возвращаем fallback
        if not sql_query or 'SELECT' not in sql_query.upper():
            return "SELECT 'Не удалось сгенерировать запрос' as error;"
        
        return sql_query
    
    def test_sql_query(self, sql_query):
        """Тестирование SQL запроса"""
        try:
            conn = sqlite3.connect('rosatom_database.db')
            df = pd.read_sql(sql_query, conn)
            conn.close()
            
            return True, f"Успешно. Возвращено {len(df)} строк"
            
        except Exception as e:
            return False, f"Ошибка: {str(e)}"

# Тестирование
if __name__ == '__main__':
    generator = SQLGenerator()
    
    test_queries = [
        "Динамика продаж за последний год",
        "Тренд продаж по месяцам",
        "Продажи за последний месяц",
        "Ежемесячные продажи",
        "График продаж",
    ]
    
    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Тест запроса: {query}")
        print(f"{'='*60}")
        
        schema = {"tables": {}}
        sql = generator.generate_sql(query, schema)
        
        success, message = generator.test_sql_query(sql)
        print(f"SQL: {sql[:200]}...")
        print(f"Результат теста: {'✅' if success else '❌'} {message}")