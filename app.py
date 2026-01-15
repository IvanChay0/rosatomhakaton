import os

from flask import Flask, render_template, request, jsonify, session

from flask_cors import CORS

from dotenv import load_dotenv

import json

import sqlite3

from datetime import datetime

import pandas as pd

import traceback



from database.manager import DatabaseManager

#from ai.sql_generator import SQLGenerator

from features.dashboard_viz import DashboardVisualizer

from features.report_generator import ReportGenerator



# Загрузка переменных окружения

load_dotenv()



app = Flask(__name__)

app.secret_key = os.urandom(24)

CORS(app)



def create_fallback_sql_generator():
    class SimpleSQLGenerator:
        def generate_sql(self, natural_language_query, schema_info):
            query = natural_language_query.lower()
            
            if ('топ' in query or 'лучш' in query) and ('товар' in query or 'продукт' in query or 'продаж' in query):
                return """
                    SELECT 
                        product_name,
                        SUM(revenue) as total_revenue,
                        SUM(quantity) as total_quantity,
                        COUNT(*) as transactions
                    FROM production 
                    WHERE date >= date('now', '-1 month')
                        AND revenue IS NOT NULL
                    GROUP BY product_name
                    ORDER BY total_revenue DESC
                    LIMIT 5
                """
            # ... остальные условия как в файле выше
            else:
                return "SELECT 'Пожалуйста, уточните запрос' as message"
    
    return SimpleSQLGenerator()

# Инициализация компонентов

db_manager = DatabaseManager()

sql_generator = create_fallback_sql_generator()


visualizer = DashboardVisualizer()

report_generator = ReportGenerator()



def check_and_create_database():

    """Проверка и создание базы данных при необходимости"""

    import os

    import sqlite3

    

    if not os.path.exists('rosatom_database.db'):

        print("⚠️ База данных не найдена. Создание тестовой базы...")

        try:

            conn = sqlite3.connect('rosatom_database.db')

            cursor = conn.cursor()

            

            # Создаем простую тестовую таблицу

            cursor.execute('''

                CREATE TABLE IF NOT EXISTS projects (

                    project_id INTEGER PRIMARY KEY,

                    project_name TEXT,

                    budget REAL,

                    status TEXT

                )

            ''')

            

            # Добавляем тестовые данные

            test_data = [

                (1, 'АЭС-2006', 10000000, 'В работе'),

                (2, 'БРЕСТ-ОД-300', 5000000, 'Завершен'),

                (3, 'ПАТЭС', 7500000, 'В работе'),

                (4, 'ТОКАМАК', 3000000, 'Планирование'),

                (5, 'Квантовые вычисления', 2000000, 'В работе')

            ]

            

            cursor.executemany('INSERT OR IGNORE INTO projects VALUES (?, ?, ?, ?)', test_data)

            conn.commit()

            conn.close()

            print("✅ Тестовая база данных создана")

        except Exception as e:

            print(f"❌ Ошибка создания базы данных: {e}")

    else:

        print("✅ База данных найдена")



@app.route('/')

def index():

    """Главная страница"""

    return render_template('index.html')



@app.route('/dashboard')

def dashboard():

    """Дашборд с визуализациями"""

    return render_template('dashboard.html')



@app.route('/reports')

def reports():

    """Страница отчетов"""

    return render_template('reports.html')



@app.route('/api/chat', methods=['POST'])

def chat_with_data():

    """Основной API эндпоинт для обработки запросов на естественном языке"""

    try:

        data = request.json

        user_query = data.get('query', '').strip()

        conversation_history = data.get('history', [])

        

        if not user_query:

            return jsonify({

                'success': False,

                'error': 'Запрос не может быть пустым'

            }), 400

        

        print(f"\n{'='*60}")

        print(f"📨 Получен запрос: {user_query}")

        print(f"{'='*60}")

        

        # Получаем схему базы данных

        schema_info = db_manager.get_database_schema()

        print(f"📋 Схема БД: {len(schema_info.get('tables', {}))} таблиц")

        

        # Генерируем SQL запрос

        sql_query = sql_generator.generate_sql(user_query, schema_info)

        print(f"📝 Сгенерирован SQL: {sql_query}")

        

        # Выполняем SQL запрос

        try:

            result_df = db_manager.execute_query(sql_query)

            print(f"✅ Получено данных: {len(result_df)} строк, {len(result_df.columns)} колонок")

        except Exception as sql_error:

            print(f"❌ Ошибка выполнения SQL: {sql_error}")

            

            # Пробуем простой запрос как fallback

            try:

                print("🔄 Пробуем выполнить простой запрос...")

                simple_sql = "SELECT 'Ошибка выполнения запроса' as error, ? as sql_query"

                result_df = db_manager.execute_query(simple_sql, (sql_query,))

            except:

                result_df = pd.DataFrame({'error': ['Ошибка выполнения запроса'], 'details': [str(sql_error)]})

        

        # Преобразуем результат в удобный формат

       # Преобразуем результат в удобный формат (с заменой NaN)
        result_data = {
            'sql_query': sql_query,
            'data': json.loads(result_df.fillna('').to_json(orient='records')) if not result_df.empty else [],
            'columns': list(result_df.columns) if not result_df.empty else [],
            'row_count': len(result_df)
        }
        

        # Генерируем текстовый анализ

        print("🧠 Генерация текстового анализа...")

        try:

            text_analysis = report_generator.generate_text_analysis(result_df, user_query)

            print("✅ Анализ сгенерирован успешно")

        except Exception as analysis_error:

            print(f"❌ Ошибка генерации анализа: {analysis_error}")

            traceback.print_exc()

            

            # Fallback анализ

            text_analysis = f"""

## 📊 Результат запроса



**Ваш запрос:** *{user_query}*



✅ **Данные успешно получены**



• Количество записей: **{len(result_df):,}**  

• Колонок в данных: **{len(result_df.columns)}**



### 💡 Краткая информация



Запрос выполнен успешно. {"Данные содержат информацию для анализа." if len(result_df) > 0 else "Запрос не вернул данных."}



### 🚀 Что можно сделать дальше:



1. Изучите данные во вкладке "Данные"

2. Используйте визуализацию для графического представления

3. Уточните запрос для получения конкретной информации



*Для детального анализа обратитесь к модулю визуализации.*

"""

        

        # Определяем тип визуализации

        visualization_type = visualizer.determine_visualization_type(user_query)

        print(f"🎨 Тип визуализации: {visualization_type}")

        

        # Генерируем визуализацию

        visualization_json = None

        if not result_df.empty and len(result_df) > 0:

            try:

                print("🎨 Создание визуализации...")

                visualization_json = visualizer.create_visualization(

                    result_df, 

                    visualization_type,

                    user_query

                )

                

                # Проверяем валидность JSON

                if visualization_json:

                    json.loads(visualization_json)

                    print("✅ Визуализация создана успешно")

            except Exception as viz_error:

                print(f"❌ Ошибка создания визуализации: {viz_error}")

                traceback.print_exc()

                

                # Пробуем создать простую таблицу

                try:

                    print("🔄 Пробуем создать таблицу...")

                    visualization_json = visualizer.create_visualization(

                        result_df,

                        'table',

                        user_query

                    )

                except:

                    visualization_json = None

        

        # Формируем ответ

        response = {

            'success': True,

            'query': user_query,

            'sql_query': sql_query,

            'data': result_data['data'],

            'columns': result_data['columns'],

            'row_count': result_data['row_count'],

            'text_analysis': text_analysis,

            'visualization': visualization_json,

            'timestamp': datetime.now().isoformat()

        }

        

        # Сохраняем в историю сессии

        if 'conversation' not in session:

            session['conversation'] = []

        

        session['conversation'].append({

            'user': user_query,

            'sql': sql_query,

            'timestamp': datetime.now().isoformat(),

            'row_count': len(result_df)

        })

        

        # Ограничиваем историю

        if len(session['conversation']) > 20:

            session['conversation'] = session['conversation'][-20:]

        

        print(f"📤 Ответ сформирован: {len(result_data['data'])} записей, анализ: {'✓' if text_analysis else '✗'}, визуализация: {'✓' if visualization_json else '✗'}")

        print(f"{'='*60}\n")

        

        return jsonify(response)

        

    except Exception as e:

        print(f"\n{'='*60}")

        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА в chat_with_data:")

        print(f"Тип: {type(e).__name__}")

        print(f"Описание: {str(e)}")

        traceback.print_exc()

        print(f"{'='*60}\n")

        

        error_response = f"""

## ❌ Произошла ошибка



**Системная ошибка при обработке запроса**



### Техническая информация:

- Тип ошибки: `{type(e).__name__}`

- Описание: {str(e)[:200]}



### Что можно сделать:



1. **Проверьте формулировку запроса**

2. **Упростите запрос** (используйте более простые фразы)

3. **Попробуйте один из примеров запросов:**

   - "Покажи все проекты"

   - "Сколько сотрудников в компании?"

   - "Какая общая выручка?"



### Примеры рабочих запросов:



• "Покажи топ-5 товаров"

• "Сколько всего сотрудников?"

• "Какие проекты в работе?"

• "Покажи последние продажи"





*Если ошибка повторяется, обратитесь к администратору системы.*

"""

        

        return jsonify({

            'success': False,

            'error': error_response,

            'message': 'Произошла ошибка при обработке запроса',

            'timestamp': datetime.now().isoformat()

        }), 500



@app.route('/api/schema', methods=['GET'])

def get_schema():

    """Получение схемы базы данных"""

    try:

        schema = db_manager.get_database_schema()

        return jsonify({

            'success': True,

            'schema': schema,

            'table_count': len(schema.get('tables', {})),

            'timestamp': datetime.now().isoformat()

        })

    except Exception as e:

        print(f"Ошибка получения схемы: {e}")

        return jsonify({

            'success': False,

            'error': str(e),

            'timestamp': datetime.now().isoformat()

        }), 500



@app.route('/api/execute_sql', methods=['POST'])

def execute_sql():

    """Выполнение произвольного SQL запроса"""

    try:

        data = request.json

        sql_query = data.get('sql', '').strip()

        

        if not sql_query:

            return jsonify({

                'success': False,

                'error': 'SQL запрос не может быть пустым'

            }), 400

        

        # Проверка на потенциально опасные операции

        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'TRUNCATE']

        sql_upper = sql_query.upper()

        

        for keyword in dangerous_keywords:

            if f' {keyword} ' in sql_upper or sql_upper.startswith(keyword):

                return jsonify({

                    'success': False,

                    'error': f'Операция {keyword} не разрешена для безопасности данных'

                }), 403

        

        result_df = db_manager.execute_query(sql_query)

        

        return jsonify({
        'success': True,
        'data': json.loads(result_df.fillna('').to_json(orient='records')),
        'columns': list(result_df.columns),
        'row_count': len(result_df),
        'sql_query': sql_query,
        'timestamp': datetime.now().isoformat()
        })

        

    except Exception as e:

        print(f"Ошибка выполнения SQL: {e}")

        return jsonify({

            'success': False,

            'error': str(e),

            'sql_query': sql_query if 'sql_query' in locals() else None,

            'timestamp': datetime.now().isoformat()

        }), 500



@app.route('/api/visualize', methods=['POST'])

def visualize_data():

    """Создание визуализации на основе данных"""

    try:

        data = request.json

        chart_type = data.get('chart_type', 'table')

        chart_data = data.get('data', [])

        chart_config = data.get('config', {})

        

        if not chart_data:

            return jsonify({

                'success': False,

                'error': 'Нет данных для визуализации'

            }), 400

        

        df = pd.DataFrame(chart_data)

        

        # Если данных слишком много, ограничиваем

        if len(df) > 1000:

            df = df.head(1000)

        

        visualization = visualizer.create_visualization(

            df, 

            chart_type, 

            chart_config.get('title', 'Визуализация данных')

        )

        

        return jsonify({

            'success': True,

            'visualization': visualization,

            'data_points': len(df),

            'timestamp': datetime.now().isoformat()

        })

        

    except Exception as e:

        print(f"Ошибка создания визуализации: {e}")

        traceback.print_exc()

        return jsonify({

            'success': False,

            'error': str(e),

            'timestamp': datetime.now().isoformat()

        }), 500



@app.route('/api/generate_report', methods=['POST'])
def generate_report():
    """Генерация отчета с реальными данными"""
    try:
        data = request.json
        report_type = data.get('report_type', 'summary')
        filters = data.get('filters', {})
        
        # Получаем реальные данные из базы
        report_data = generate_real_report(report_type, filters)
        
        return jsonify({
            'success': True,
            'report': report_data,
            'report_type': report_type,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        print(f"Ошибка генерации отчета: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

def generate_real_report(report_type, filters):
    """Генерация отчета с реальными данными из БД с учетом фильтров"""
    
    # Получаем данные из базы
    import sqlite3
    conn = sqlite3.connect('rosatom_database.db')
    cursor = conn.cursor()
    
    # Извлекаем фильтры
    departments = filters.get('departments', [])
    period = filters.get('period', 'month')
    period_text = filters.get('period_text', 'Месячный')
    include_charts = filters.get('include_charts', True)
    include_ai = filters.get('include_ai', True)
    
    print(f"📊 Генерация отчета {report_type} с фильтрами:")
    print(f"   - Отделы: {departments}")
    print(f"   - Период: {period} ({period_text})")
    
    # Подготавливаем условия WHERE
    where_conditions = []
    params = []
    
    # Фильтр по отделам
    if departments and 'all' not in departments:
        placeholders = ','.join(['?' for _ in departments])
        where_conditions.append(f"department IN ({placeholders})")
        params.extend(departments)
    
    # Создаем SQL для фильтра по периоду
    date_filter = ""
    if period == 'month':
        date_filter = "date >= date('now', '-1 month')"
    elif period == 'quarter':
        date_filter = "date >= date('now', '-3 months')"
    elif period == 'year':
        date_filter = "date >= date('now', '-1 year')"
    
    if report_type == 'summary':
        # Общий отчет
        metrics = {}
        
        # 1. Сотрудники с фильтром по отделам
        if where_conditions:
            sql = f"SELECT COUNT(*) FROM employees WHERE {' AND '.join(where_conditions)}"
            cursor.execute(sql, params)
        else:
            cursor.execute("SELECT COUNT(*) FROM employees")
        metrics['Сотрудников'] = cursor.fetchone()[0]
        
        # 2. Активные проекты
        project_where = ["status = 'В работе'"]
        if where_conditions:
            project_where.extend(where_conditions)
        
        if project_where:
            sql = f"SELECT COUNT(*) FROM projects WHERE {' AND '.join(project_where)}"
            cursor.execute(sql, params if where_conditions else [])
        else:
            cursor.execute("SELECT COUNT(*) FROM projects WHERE status = 'В работе'")
        metrics['Активных проектов'] = cursor.fetchone()[0]
        
        # 3. Выручка с фильтрами по дате и отделам
        revenue_where = ["revenue IS NOT NULL"]
        if date_filter:
            revenue_where.append(date_filter)
        if where_conditions:
            revenue_where.extend(where_conditions)

        sql = f"SELECT SUM(revenue) FROM production WHERE {' AND '.join(revenue_where)}"
        cursor.execute(sql, params if where_conditions else [])

        # БЕЗОПАСНОЕ ПОЛУЧЕНИЕ РЕЗУЛЬТАТА
        result = cursor.fetchone()
        if result and result[0] is not None:
            revenue_result = result[0]
        else:
            revenue_result = None

        if revenue_result is None or revenue_result == 0:
            # Проверяем, есть ли данные в production вообще
            cursor.execute("SELECT COUNT(*) FROM production WHERE revenue > 0")
            production_count_result = cursor.fetchone()
            production_count = production_count_result[0] if production_count_result and production_count_result[0] is not None else 0
            
            if production_count > 0:
                # Есть данные в production, но фильтры их отсекают
                revenue = 0
            else:
                # Нет данных в production вообще, используем бюджет проектов
                print("📊 Нет данных о продажах, используем бюджет проектов")
                
                # Считаем бюджет активных проектов с учетом фильтров
                budget_where = ["status = 'В работе'"]
                if where_conditions:
                    budget_where.extend(where_conditions)
                
                if budget_where:
                    sql = f"SELECT SUM(budget) FROM projects WHERE {' AND '.join(budget_where)}"
                    cursor.execute(sql, params if where_conditions else [])
                else:
                    sql = "SELECT SUM(budget) FROM projects WHERE status = 'В работе'"
                    cursor.execute(sql)
                
                # Безопасное получение бюджета
                budget_result_row = cursor.fetchone()
                budget_result = budget_result_row[0] if budget_result_row and budget_result_row[0] is not None else 0
                revenue = budget_result
        else:
            revenue = revenue_result

        # Форматируем результат
        if revenue == 0:
            metrics['Общая выручка'] = "Нет данных"
        else:
            metrics['Общая выручка'] = f"{revenue:,.0f} ₽"
                
        # 4. Безопасность с фильтрами
        safety_where = []
        if where_conditions:
            safety_where.extend(where_conditions)
        
        if safety_where:
            sql = f"""
                SELECT 
                    (COUNT(CASE WHEN severity = 'Низкий' THEN 1 END) * 100.0 / 
                     NULLIF(COUNT(*), 0)) as safety_score 
                FROM safety_incidents 
                WHERE {' AND '.join(safety_where)}
            """
            cursor.execute(sql, params if where_conditions else [])
        else:
            cursor.execute("""
                SELECT 
                    (COUNT(CASE WHEN severity = 'Низкий' THEN 1 END) * 100.0 / 
                     NULLIF(COUNT(*), 0)) as safety_score 
                FROM safety_incidents
            """)
        
        safety_score = cursor.fetchone()[0] or 100
        metrics['Безопасность'] = f"{safety_score:.1f}%"
        
        # 5. Динамика продаж за период
        sales_where = ["revenue IS NOT NULL"]
        if date_filter:
            sales_where.append(date_filter)
        if where_conditions:
            sales_where.extend(where_conditions)
        
        sql = f"""
            SELECT 
                substr(date, 1, 7) as month,
                SUM(revenue) as total_revenue
            FROM production 
            WHERE {' AND '.join(sales_where)}
            GROUP BY substr(date, 1, 7)
            ORDER BY month DESC
            LIMIT 12
        """
        cursor.execute(sql, params if where_conditions else [])
        sales_data = cursor.fetchall()
        
        # 6. Примеры проектов с фильтрами
        project_where = []
        if where_conditions:
            project_where.extend(where_conditions)
        
        if project_where:
            sql = f"SELECT project_name, budget, status FROM projects WHERE {' AND '.join(project_where)} LIMIT 10"
            cursor.execute(sql, params if where_conditions else [])
        else:
            sql = "SELECT project_name, budget, status FROM projects LIMIT 10"
            cursor.execute(sql)
        
        projects = cursor.fetchall()
        
        data = [{
            'project_name': row[0],
            'budget': f"{row[1]:,.0f} ₽",
            'status': row[2]
        } for row in projects]
        
        # Добавляем данные по динамике продаж
        sales_chart_data = [{
            'month': row[0],
            'revenue': row[1] or 0
        } for row in sales_data]
        
        conn.close()
        
        # Формируем анализ с учетом фильтров
        analysis = f"## 📊 Общий отчет\n\n"
        analysis += f"**Период:** {period_text}\n\n"
        
        if departments:
            analysis += f"**Отделы:** {', '.join(departments)}\n\n"
        
        analysis += f"### Ключевые показатели:\n"
        for key, value in metrics.items():
            analysis += f"- **{key}:** {value}\n"
        
        analysis += f"\n### Статистика:\n"
        analysis += f"- Отчет сгенерирован: {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
        analysis += f"- Количество активных проектов: {metrics['Активных проектов']}\n"
        analysis += f"- Общая выручка: {metrics['Общая выручка']}\n"
        
        if sales_data:
            analysis += f"\n### Динамика продаж:\n"
            for month, revenue in sales_data[:3]:
                analysis += f"- {month}: {revenue:,.0f} ₽\n"
        
        return {
            'title': f'Общий отчет ({period_text})',
            'date': datetime.now().strftime('%d.%m.%Y %H:%M'),
            'metrics': metrics,
            'data': data,
            'columns': ['project_name', 'budget', 'status'],
            'analysis': analysis,
            'type': 'summary',
            'period': period,
            'period_text': period_text,
            'departments': departments,
            'sales_data': sales_chart_data,
            'filters_applied': filters
        }
    
    elif report_type == 'performance':
        # Отчет по эффективности с фильтрами
        metrics = {}
        
        # Фильтр для сотрудников
        emp_where = ["performance_score IS NOT NULL"]
        if where_conditions:
            emp_where.extend(where_conditions)
        
        # Средняя эффективность
        sql = f"SELECT AVG(performance_score) FROM employees WHERE {' AND '.join(emp_where)}"
        cursor.execute(sql, params if where_conditions else [])
        avg_performance = cursor.fetchone()[0] or 0
        metrics['Средняя эффективность'] = f"{avg_performance:.1f}/100"
        
        # Топ сотрудники
        sql = f"SELECT COUNT(*) FROM employees WHERE performance_score >= 90"
        if where_conditions:
            sql += f" AND {' AND '.join(where_conditions)}"
        cursor.execute(sql, params if where_conditions else [])
        top_performers = cursor.fetchone()[0]
        metrics['Топ сотрудников (90+)'] = top_performers
        
        # Данные по сотрудникам
        sql = f"""
            SELECT 
                first_name || ' ' || last_name as name,
                department,
                position,
                performance_score,
                salary
            FROM employees 
            WHERE performance_score IS NOT NULL
        """
        if where_conditions:
            sql += f" AND {' AND '.join(where_conditions)}"
        sql += " ORDER BY performance_score DESC LIMIT 10"
        
        cursor.execute(sql, params if where_conditions else [])
        employees = cursor.fetchall()
        
        data = [{
            'name': row[0],
            'department': row[1],
            'position': row[2],
            'performance_score': row[3],
            'salary': f"{row[4]:,.0f} ₽"
        } for row in employees]
        
        conn.close()
        
        analysis = f"## 👥 Отчет по эффективности сотрудников\n\n"
        analysis += f"**Период:** {period_text}\n\n"
        
        if departments:
            analysis += f"**Отделы:** {', '.join(departments)}\n\n"
        
        analysis += f"### Ключевые показатели:\n"
        for key, value in metrics.items():
            analysis += f"- **{key}:** {value}\n"
        
        return {
            'title': f'Отчет по эффективности ({period_text})',
            'date': datetime.now().strftime('%d.%m.%Y %H:%M'),
            'metrics': metrics,
            'data': data,
            'columns': ['name', 'department', 'position', 'performance_score', 'salary'],
            'analysis': analysis,
            'type': 'performance',
            'period': period_text
        }
    
    elif report_type == 'financial':
        # Финансовый отчет с фильтрами по дате
        metrics = {}
        
        # Общий бюджет
        sql = "SELECT SUM(budget) FROM projects"
        cursor.execute(sql)
        total_budget = cursor.fetchone()[0] or 0
        metrics['Общий бюджет'] = f"{total_budget:,.0f} ₽"
        
        # Выручка за период с фильтром по дате
        revenue_where = ["revenue IS NOT NULL"]
        if date_filter:
            revenue_where.append(date_filter)
        if where_conditions:
            revenue_where.extend(where_conditions)
        
        sql = f"SELECT SUM(revenue) FROM production WHERE {' AND '.join(revenue_where)}"
        cursor.execute(sql, params if where_conditions else [])
        total_revenue = cursor.fetchone()[0] or 0
        metrics['Общая выручка'] = f"{total_revenue:,.0f} ₽"
        
        # Бюджет по проектам с фильтрами
        project_where = []
        if where_conditions:
            project_where.extend(where_conditions)
        
        if project_where:
            sql = f"SELECT project_name, budget, status FROM projects WHERE {' AND '.join(project_where)} ORDER BY budget DESC LIMIT 10"
            cursor.execute(sql, params if where_conditions else [])
        else:
            sql = "SELECT project_name, budget, status FROM projects ORDER BY budget DESC LIMIT 10"
            cursor.execute(sql)
        
        budgets = cursor.fetchall()
        
        data = [{
            'project_name': row[0],
            'budget': f"{row[1]:,.0f} ₽",
            'status': row[2]
        } for row in budgets]
        
        conn.close()
        
        analysis = f"## 💰 Финансовый отчет\n\n"
        analysis += f"**Период:** {period_text}\n\n"
        
        if departments:
            analysis += f"**Отделы:** {', '.join(departments)}\n\n"
        
        analysis += f"### Финансовые показатели:\n"
        for key, value in metrics.items():
            analysis += f"- **{key}:** {value}\n"
        
        return {
            'title': f'Финансовый отчет ({period_text})',
            'date': datetime.now().strftime('%d.%m.%Y %H:%M'),
            'metrics': metrics,
            'data': data,
            'columns': ['project_name', 'budget', 'status'],
            'analysis': analysis,
            'type': 'financial',
            'period': period_text
        }
    
    elif report_type == 'safety':
        # Отчет по безопасности с фильтром по дате
        metrics = {}
        
        # Всего инцидентов за период
        incidents_where = []
        if date_filter:
            incidents_where.append(date_filter)
        if where_conditions:
            incidents_where.extend(where_conditions)
        
        if incidents_where:
            sql = f"SELECT COUNT(*) FROM safety_incidents WHERE {' AND '.join(incidents_where)}"
            cursor.execute(sql, params if where_conditions else [])
        else:
            sql = "SELECT COUNT(*) FROM safety_incidents"
            cursor.execute(sql)
        
        total_incidents = cursor.fetchone()[0]
        metrics['Всего инцидентов'] = total_incidents
        
        # Решенные инциденты
        resolved_where = ["resolved = 1"]
        if date_filter:
            resolved_where.append(date_filter)
        if where_conditions:
            resolved_where.extend(where_conditions)
        
        sql = f"SELECT COUNT(*) FROM safety_incidents WHERE {' AND '.join(resolved_where)}"
        cursor.execute(sql, params if where_conditions else [])
        resolved = cursor.fetchone()[0]
        metrics['Решено'] = resolved
        
        # Последние инциденты
        recent_where = []
        if date_filter:
            recent_where.append(date_filter)
        if where_conditions:
            recent_where.extend(where_conditions)
        
        if recent_where:
            sql = f"""
                SELECT date, description, severity, department, resolved 
                FROM safety_incidents 
                WHERE {' AND '.join(recent_where)}
                ORDER BY date DESC 
                LIMIT 10
            """
            cursor.execute(sql, params if where_conditions else [])
        else:
            sql = """
                SELECT date, description, severity, department, resolved 
                FROM safety_incidents 
                ORDER BY date DESC 
                LIMIT 10
            """
            cursor.execute(sql)
        
        incidents = cursor.fetchall()
        
        data = [{
            'date': row[0],
            'description': row[1][:50] + ('...' if len(row[1]) > 50 else ''),
            'severity': row[2],
            'department': row[3],
            'resolved': 'Да' if row[4] else 'Нет'
        } for row in incidents]
        
        conn.close()
        
        analysis = f"## 🛡️ Отчет по безопасности\n\n"
        analysis += f"**Период:** {period_text}\n\n"
        
        if departments:
            analysis += f"**Отделы:** {', '.join(departments)}\n\n"
        
        analysis += f"### Статистика инцидентов:\n"
        for key, value in metrics.items():
            analysis += f"- **{key}:** {value}\n"
        
        if total_incidents > 0:
            resolution_rate = (resolved / total_incidents) * 100
            analysis += f"- **Процент решенных:** {resolution_rate:.1f}%\n"
        
        return {
            'title': f'Отчет по безопасности ({period_text})',
            'date': datetime.now().strftime('%d.%m.%Y %H:%M'),
            'metrics': metrics,
            'data': data,
            'columns': ['date', 'description', 'severity', 'department', 'resolved'],
            'analysis': analysis,
            'type': 'safety',
            'period': period_text
        }
    
    else:
        conn.close()
        return {
            'title': f'Отчет {report_type} ({period_text})',
            'date': datetime.now().strftime('%d.%m.%Y %H:%M'),
            'metrics': {'Статус': 'Сгенерировано'},
            'data': [],
            'analysis': f'Отчет {report_type} успешно сгенерирован.\n\n**Период:** {period_text}\n**Отделы:** {", ".join(departments) if departments else "Все"}',
            'type': report_type,
            'period': period_text
        }
@app.route('/api/download_report', methods=['POST'])
def download_report():
    """Скачивание отчета в разных форматах"""
    try:
        data = request.json
        report_data = data.get('report_data', {})
        report_type = data.get('report_type', 'summary')
        format_type = data.get('format', 'json')
        filename = data.get('filename', f'report_{datetime.now().strftime("%Y%m%d_%H%M%S")}')
        
        if not report_data:
            return jsonify({
                'success': False,
                'error': 'Нет данных отчета для скачивания'
            }), 400
        
        if format_type == 'json':
            # Возвращаем JSON файл
            response_data = json.dumps(report_data, ensure_ascii=False, indent=2)
            return Response(
                response_data,
                mimetype='application/json',
                headers={
                    'Content-Disposition': f'attachment; filename={filename}.json',
                    'Content-Type': 'application/json; charset=utf-8'
                }
            )
        
        elif format_type == 'html':
            # Генерируем HTML отчет
            html_content = generate_html_report(report_data, report_type)
            return Response(
                html_content,
                mimetype='text/html',
                headers={
                    'Content-Disposition': f'attachment; filename={filename}.html',
                    'Content-Type': 'text/html; charset=utf-8'
                }
            )
        
        elif format_type == 'csv':
            # Генерируем CSV из данных
            if 'data' in report_data and report_data['data']:
                df = pd.DataFrame(report_data['data'])
                csv_content = df.to_csv(index=False, encoding='utf-8-sig')
                return Response(
                    csv_content,
                    mimetype='text/csv',
                    headers={
                        'Content-Disposition': f'attachment; filename={filename}.csv',
                        'Content-Type': 'text/csv; charset=utf-8'
                    }
                )
        
        return jsonify({
            'success': False,
            'error': f'Формат {format_type} не поддерживается'
        }), 400
        
    except Exception as e:
        print(f"Ошибка скачивания отчета: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

def generate_html_report(report_data, report_type):
    """Генерация HTML отчета"""
    html = f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Отчет {report_type} - Rosatom BI System</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; color: #333; }}
            .header {{ text-align: center; margin-bottom: 40px; padding-bottom: 20px; border-bottom: 2px solid #667eea; }}
            .header h1 {{ color: #667eea; margin-bottom: 10px; }}
            .meta {{ color: #718096; font-size: 14px; }}
            .section {{ margin: 30px 0; }}
            .section h2 {{ color: #4a5568; border-bottom: 1px solid #e2e8f0; padding-bottom: 10px; }}
            .metrics-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }}
            .metric-card {{ background: linear-gradient(135deg, #667eea, #764ba2); color: white; padding: 20px; border-radius: 10px; text-align: center; }}
            .metric-value {{ font-size: 28px; font-weight: bold; margin-bottom: 10px; }}
            .metric-label {{ font-size: 14px; opacity: 0.9; }}
            table {{ width: 100%; border-collapse: collapse; margin: 20px 0; }}
            th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #e2e8f0; }}
            th {{ background-color: #f7fafc; font-weight: bold; }}
            tr:hover {{ background-color: #f9fafb; }}
            .insights {{ background: #fff8e1; padding: 20px; border-radius: 10px; margin: 20px 0; border-left: 4px solid #ffb74d; }}
            .footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #e2e8f0; color: #718096; font-size: 12px; text-align: center; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Отчет {report_type}</h1>
            <div class="meta">
                <p>Сгенерировано: {datetime.now().strftime('%d.%m.%Y %H:%M')}</p>
                <p>Rosatom BI System</p>
            </div>
        </div>
    """
    
    # Добавляем метрики если есть
    if 'metrics' in report_data:
        html += '<div class="section"><h2>Ключевые показатели</h2><div class="metrics-grid">'
        for key, value in report_data['metrics'].items():
            html += f'''
            <div class="metric-card">
                <div class="metric-value">{value}</div>
                <div class="metric-label">{key}</div>
            </div>
            '''
        html += '</div></div>'
    
    # Добавляем данные таблицей если есть
    if 'data' in report_data and report_data['data']:
        html += '<div class="section"><h2>Данные</h2><table>'
        
        # Заголовки
        html += '<thead><tr>'
        for col in report_data.get('columns', list(report_data['data'][0].keys())):
            html += f'<th>{col}</th>'
        html += '</tr></thead><tbody>'
        
        # Строки
        for row in report_data['data'][:50]:  # Ограничиваем 50 строками
            html += '<tr>'
            for col in report_data.get('columns', row.keys()):
                value = row.get(col, '')
                html += f'<td>{value}</td>'
            html += '</tr>'
        
        html += '</tbody></table>'
        if len(report_data['data']) > 50:
            html += f'<p style="text-align: center; color: #718096;">Показано 50 из {len(report_data["data"])} записей</p>'
        html += '</div>'
    
    # Добавляем анализ если есть
    if 'analysis' in report_data:
        html += f'<div class="section"><h2>Анализ</h2><div class="insights"><p>{report_data["analysis"]}</p></div></div>'
    
    html += '''
        <div class="footer">
            <p>© 2024 Rosatom BI System. Конфиденциально.</p>
            <p>Этот отчет был автоматически сгенерирован системой бизнес-аналитики.</p>
        </div>
    </body>
    </html>
    '''
    
    return html

@app.route('/api/conversation_history', methods=['GET'])

def get_conversation_history():

    """Получение истории диалога"""

    try:

        history = session.get('conversation', [])

        

        # Добавляем информацию о системе

        system_info = {

            'total_queries': len(history),

            'last_query': history[-1] if history else None,

            'timestamp': datetime.now().isoformat()

        }

        

        return jsonify({

            'success': True,

            'history': history,

            'system_info': system_info

        })

    except Exception as e:

        print(f"Ошибка получения истории: {e}")

        return jsonify({

            'success': False,

            'error': str(e),

            'history': []

        }), 500

    





@app.route('/api/dashboard/filtered_data', methods=['POST'])

def get_filtered_dashboard_data():

    """Получение отфильтрованных данных для дашборда"""

    try:

        data = request.json

        filters = data.get('filters', {})

        

        # Получаем параметры фильтров

        department = filters.get('department', 'all')

        period = filters.get('period', 'last_month')

        project = filters.get('project', 'all')

        

        # Базовые SQL запросы с учетом фильтров

        where_clauses = []

        params = []

        

        # 1. Фильтр по отделу

        if department != 'all':

            where_clauses.append("department = ?")

            params.append(department)

        

        # 2. Фильтр по периоду для данных с датами

        if period != 'all':

            if period == 'last_month':

                date_filter = "AND date >= date('now', '-1 month')"

            elif period == 'last_quarter':

                date_filter = "AND date >= date('now', '-3 months')"

            elif period == 'last_year':

                date_filter = "AND date >= date('now', '-1 year')"

            else:

                date_filter = ""

        else:

            date_filter = ""

        

        # 3. Фильтр по проекту

        if project != 'all':

            # Для таблицы production

            where_clauses.append("project_name LIKE ?")

            params.append(f'%{project}%')

        

        # Формируем WHERE условие

        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        

        # 1. KPI метрики

        # Сотрудники

        employees_query = f"""

            SELECT COUNT(*) as total_employees 

            FROM employees 

            {where_sql.replace('department = ?', 'department = ?') if department != 'all' else ''}

        """

        

        # Активные проекты

        projects_query = f"""

            SELECT COUNT(*) as active_projects 

            FROM projects 

            WHERE status = 'В работе'

            {f"AND project_name LIKE '%{project}%'" if project != 'all' else ""}

        """

        

        # Общая выручка

        revenue_query = f"""

            SELECT SUM(revenue) as total_revenue 

            FROM production 

            WHERE revenue IS NOT NULL

            {date_filter}

            {f"AND project_name LIKE '%{project}%'" if project != 'all' else ""}

        """

        

        # Показатель безопасности

        safety_query = """

            SELECT 

                (COUNT(CASE WHEN severity = 'Низкий' THEN 1 END) * 100.0 / 

                 NULLIF(COUNT(*), 0)) as safety_score 

            FROM safety_incidents

        """

        

        # 2. Распределение сотрудников по отделам

        department_chart_query = """

            SELECT department, COUNT(*) as employee_count 

            FROM employees 

            GROUP BY department 

            ORDER BY employee_count DESC 

            LIMIT 10

        """

        

        # 3. Динамика продаж с учетом фильтров

        sales_chart_query = f"""

            SELECT 

                substr(date, 1, 7) as month,

                SUM(revenue) as total_revenue

            FROM production 

            WHERE revenue IS NOT NULL 

                {date_filter}

                {f"AND project_name LIKE '%{project}%'" if project != 'all' else ""}

            GROUP BY substr(date, 1, 7)

            ORDER BY month DESC

            LIMIT 12

        """

        

        # 4. Статус проектов

        project_status_query = """

            SELECT status, COUNT(*) as count 

            FROM projects 

            GROUP BY status

        """

        

        # 5. Топ товаров с учетом фильтров

        top_products_query = f"""

            SELECT 

                product_name,

                SUM(revenue) as total_revenue

            FROM production 

            WHERE revenue IS NOT NULL

                {date_filter}

                {f"AND project_name LIKE '%{project}%'" if project != 'all' else ""}

            GROUP BY product_name 

            ORDER BY total_revenue DESC 

            LIMIT 5

        """

        

        # 6. Последние инциденты

        safety_incidents_query = """

            SELECT 

                date,

                description,

                severity,

                department,

                resolved

            FROM safety_incidents 

            ORDER BY date DESC 

            LIMIT 10

        """

        

        # 7. Топ сотрудников

        top_employees_query = """

            SELECT 

                first_name || ' ' || last_name as full_name,

                department,

                position,

                performance_score,

                salary

            FROM employees 

            WHERE performance_score IS NOT NULL

            ORDER BY performance_score DESC 

            LIMIT 10

        """

        

        # Выполняем все запросы

        results = {}

        

        # KPI метрики

        results['employees'] = db_manager.execute_query(employees_query, params if department != 'all' else None).to_dict('records')[0] if not db_manager.execute_query(employees_query, params if department != 'all' else None).empty else {'total_employees': 0}

        results['projects'] = db_manager.execute_query(projects_query).to_dict('records')[0] if not db_manager.execute_query(projects_query).empty else {'active_projects': 0}

        results['revenue'] = db_manager.execute_query(revenue_query).to_dict('records')[0] if not db_manager.execute_query(revenue_query).empty else {'total_revenue': 0}

        results['safety'] = db_manager.execute_query(safety_query).to_dict('records')[0] if not db_manager.execute_query(safety_query).empty else {'safety_score': 100}

        

        # Графики

        results['department_chart'] = db_manager.execute_query(department_chart_query).to_dict('records')

        results['sales_chart'] = db_manager.execute_query(sales_chart_query).to_dict('records')

        results['project_status'] = db_manager.execute_query(project_status_query).to_dict('records')

        results['top_products'] = db_manager.execute_query(top_products_query).to_dict('records')

        

        # Таблицы

        results['safety_incidents'] = db_manager.execute_query(safety_incidents_query).to_dict('records')

        results['top_employees'] = db_manager.execute_query(top_employees_query).to_dict('records')

        

        return jsonify({

            'success': True,

            'filters': filters,

            'data': results,

            'timestamp': datetime.now().isoformat()

        })

        

    except Exception as e:

        print(f"Ошибка получения отфильтрованных данных: {e}")

        return jsonify({

            'success': False,

            'error': str(e),

            'timestamp': datetime.now().isoformat()

        }), 500



@app.route('/api/health', methods=['GET'])

def health_check():

    """Проверка здоровья системы"""

    try:

        # Проверяем соединение с БД

        db_status = db_manager.get_database_schema() is not None

        

        # Проверяем доступность компонентов

        components = {

            'database': db_status,

            'sql_generator': sql_generator is not None,

            'visualizer': visualizer is not None,

            'report_generator': report_generator is not None

        }

        

        all_healthy = all(components.values())

        

        return jsonify({

            'status': 'healthy' if all_healthy else 'degraded',

            'timestamp': datetime.now().isoformat(),

            'components': components,

            'database_tables': len(db_manager.get_database_schema().get('tables', {})) if db_status else 0,

            'system': 'Rosatom BI System',

            'version': '1.0.0'

        })

        

    except Exception as e:

        return jsonify({

            'status': 'unhealthy',

            'error': str(e),

            'timestamp': datetime.now().isoformat(),

            'system': 'Rosatom BI System'

        }), 500



@app.route('/api/test_query', methods=['POST'])

def test_query():

    """Тестовый эндпоинт для проверки запросов"""

    try:

        data = request.json

        test_query = data.get('query', 'Покажи все проекты')

        

        # Используем стандартный обработчик

        return chat_with_data()

        

    except Exception as e:

        return jsonify({

            'success': False,

            'error': f'Тестовый запрос не удался: {str(e)}',

            'timestamp': datetime.now().isoformat()

        }), 500




@app.route('/api/debug/sql_generation', methods=['POST'])

def debug_sql_generation():

    """Отладка генерации SQL"""

    try:

        data = request.json

        query = data.get('query', '')

        

        if not query:

            return jsonify({

                'success': False,

                'error': 'Запрос не может быть пустым'

            }), 400

        

        schema_info = db_manager.get_database_schema()

        sql = sql_generator.generate_sql(query, schema_info)

        

        # Проверяем SQL

        test_result = sql_generator.test_sql_query(sql)

        

        return jsonify({

            'success': True,

            'original_query': query,

            'generated_sql': sql,

            'test_result': test_result,

            'timestamp': datetime.now().isoformat()

        })

        

    except Exception as e:

        return jsonify({

            'success': False,

            'error': str(e),

            'timestamp': datetime.now().isoformat()

        }), 500



# Обработчики ошибок

@app.errorhandler(404)

def not_found(error):

    return jsonify({

        'success': False,

        'error': 'Страница не найдена',

        'timestamp': datetime.now().isoformat()

    }), 404



@app.errorhandler(500)

def internal_error(error):

    return jsonify({

        'success': False,

        'error': 'Внутренняя ошибка сервера',

        'timestamp': datetime.now().isoformat()

    }), 500



# СТАЛО:
if __name__ == '__main__':
    check_and_create_database()
    print("\n" + "="*60)
    print("🚀 Rosatom BI System запускается...")
    print("="*60)
    print(f"📊 База данных: {os.path.exists('rosatom_database.db')}")
    print(f"🌐 API доступен по: http://localhost:5000")
    print(f"🔑 OpenRouter API: {'Настроен' if os.getenv('OPENROUTER_API_KEY') else 'Не настроен'}")
    print("="*60 + "\n")
    # Для локальной разработки оставьте это:
    app.run()
    # Для Railway нужно именно app.run() без параметров