import sqlite3
import pandas as pd
from sqlalchemy import create_engine, inspect, text
import json
import os
import numpy as np
from dotenv import load_dotenv

load_dotenv()

class DatabaseManager:
    def __init__(self, db_path=None):
        self.db_path = db_path or os.getenv('DATABASE_URL', 'sqlite:///rosatom_database.db')
        self.engine = create_engine(self.db_path)
        self.connection = self.engine.connect()
    
    def get_database_schema(self):
        """Получение схемы базы данных с обработкой NaN значений"""
        inspector = inspect(self.engine)
        
        schema = {
            'tables': {},
            'relationships': [],
            'metadata': {}
        }
        
        # Получаем информацию о таблицах
        tables = inspector.get_table_names()
        
        for table in tables:
            columns = inspector.get_columns(table)
            primary_keys = inspector.get_pk_constraint(table)['constrained_columns']
            foreign_keys = inspector.get_foreign_keys(table)
            
            schema['tables'][table] = {
                'columns': [
                    {
                        'name': col['name'],
                        'type': str(col['type']),
                        'nullable': col['nullable'],
                        'primary_key': col['name'] in primary_keys
                    }
                    for col in columns
                ],
                'primary_keys': primary_keys,
                'foreign_keys': foreign_keys
            }
            
            # Получаем пример данных для понимания формата
            try:
                sample_data = pd.read_sql(f"SELECT * FROM {table} LIMIT 3", self.engine)
                
                # Преобразуем DataFrame в список словарей с обработкой NaN
                sample_records = []
                for _, row in sample_data.iterrows():
                    record = {}
                    for col in sample_data.columns:
                        val = row[col]
                        # Заменяем NaN на None (который станет null в JSON)
                        if pd.isna(val):
                            record[col] = None
                        elif isinstance(val, (np.integer, np.int64, np.int32)):
                            record[col] = int(val)
                        elif isinstance(val, (np.floating, np.float64, np.float32)):
                            # Проверяем, является ли значение NaN
                            if np.isnan(val):
                                record[col] = None
                            else:
                                record[col] = float(val)
                        elif isinstance(val, np.bool_):
                            record[col] = bool(val)
                        elif isinstance(val, pd.Timestamp):
                            record[col] = val.isoformat()
                        else:
                            record[col] = str(val)
                    sample_records.append(record)
                
                schema['tables'][table]['sample_data'] = sample_records
                
            except Exception as e:
                print(f"Ошибка при получении примеров данных для таблицы {table}: {e}")
                schema['tables'][table]['sample_data'] = []
        
        # Сохраняем схему в JSON файл с кастомным сериализатором
        try:
            with open('rosatom_schema.json', 'w', encoding='utf-8') as f:
                # Используем кастомный сериализатор для обработки NaN
                json.dump(schema, f, indent=2, ensure_ascii=False, default=self._json_serializer)
        except Exception as e:
            print(f"Ошибка сохранения схемы в JSON: {e}")
            # Пробуем сохранить без sample_data
            for table in schema['tables']:
                schema['tables'][table]['sample_data'] = []
            with open('rosatom_schema.json', 'w', encoding='utf-8') as f:
                json.dump(schema, f, indent=2, ensure_ascii=False, default=self._json_serializer)
        
        return schema
    
    def _json_serializer(self, obj):
        """Кастомный сериализатор для JSON, обрабатывающий специальные типы"""
        if pd.isna(obj):
            return None
        elif isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            # Проверяем на NaN и Infinity
            if np.isnan(obj) or np.isinf(obj):
                return None
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        elif hasattr(obj, 'isoformat'):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8', errors='ignore')
        else:
            raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
    
    def execute_query(self, sql_query):
        """Выполнение SQL запроса"""
        try:
            # Убираем потенциально опасные символы
            sql_query = sql_query.replace(';', '').strip()
            
            # Выполняем запрос через pandas для удобства
            df = pd.read_sql(sql_query, self.engine)
            
            # Обрабатываем NaN значения в результате
            df = df.where(pd.notnull(df), None)
            
            return df
            
        except Exception as e:
            raise Exception(f"Ошибка выполнения запроса: {str(e)}")
    
    def get_table_data(self, table_name, limit=100):
        """Получение данных из таблицы"""
        try:
            df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT {limit}", self.engine)
            # Обрабатываем NaN значения
            df = df.where(pd.notnull(df), None)
            return df
        except Exception as e:
            raise Exception(f"Ошибка получения данных из таблицы {table_name}: {str(e)}")
    
    def close(self):
        """Закрытие соединения с БД"""
        if self.connection:
            self.connection.close()
        if self.engine:
            self.engine.dispose()