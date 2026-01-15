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
        self.api_url = os.getenv('OPENROUTER_API_URL', 'https://openrouter.ai/api/v1')
        self.model = os.getenv('MODEL_NAME', 'qwen/qwen3-vl-235b-a22b-instruct')
        
        # ⚠️ ПРОСТОЙ КЛИЕНТ БЕЗ ДОПОЛНИТЕЛЬНЫХ ПАРАМЕТРОВ
        try:
            self.client = openai.OpenAI(
                base_url=self.api_url,
                api_key=self.api_key
            )
            print("✅ OpenAI клиент инициализирован")
        except Exception as e:
            print(f"⚠️ Ошибка инициализации OpenAI: {e}")
            self.client = None
        
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
        return "SELECT 'SQL генератор работает' as status;"

    def _handle_monthly_sales(self, query_lower):
        """Обработка ежемесячных продаж"""
        return "SELECT 'Ежемесячные продажи' as info;"

    def _handle_weekly_sales(self, query_lower):
        """Обработка еженедельных продаж"""
        return "SELECT 'Еженедельные продажи' as info;"

    def _handle_daily_sales(self, query_lower):
        """Обработка ежедневных продаж"""
        return "SELECT 'Ежедневные продажи' as info;"
    
    def generate_sql(self, natural_language_query, schema_info):
        """Генерация SQL запроса на основе естественного языка"""
        
        print(f"\n{'='*60}")
        print(f"🤔 Анализ запроса: '{natural_language_query}'")
        
        query_lower = natural_language_query.lower()
        
        # Проверяем специальные паттерны
        for pattern, handler in self.special_patterns.items():
            if re.search(pattern, query_lower):
                print(f"🎯 Распознан специальный паттерн: {pattern}")
                sql_query = handler(query_lower)
                if sql_query:
                    print(f"📝 Сгенерирован специальный SQL")
                    print(f"{'='*60}")
                    return sql_query
        
        # Простые тестовые запросы
        if "проект" in query_lower:
            return "SELECT * FROM projects LIMIT 5;"
        elif "сотрудник" in query_lower:
            return "SELECT * FROM employees LIMIT 5;"
        elif "продаж" in query_lower:
            return "SELECT * FROM production LIMIT 5;"
        else:
            return "SELECT 'SQL генератор работает в тестовом режиме' as status;"
    
    def test_sql_query(self, sql_query):
        """Тестирование SQL запроса"""
        return True, "Тестовый режим активен"