import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import json
import numpy as np
from datetime import datetime

class DashboardVisualizer:
    def __init__(self):
        self.colors = px.colors.qualitative.Set3
        
    def determine_visualization_type(self, query):
        """Определение типа визуализации на основе запроса"""
        
        query_lower = query.lower()
        
        # Паттерны для определения типа визуализации
        if any(word in query_lower for word in ['топ', 'топ-', 'первые', 'последние', 'лучшие', 'худшие', 'больше всего']):
            return 'bar'
        elif any(word in query_lower for word in ['тренд', 'изменен', 'динамика', 'истори', 'времен', 'месяц', 'год', 'недел', 'день']):
            return 'line'
        elif any(word in query_lower for word in ['распределен', 'частота', 'сколько', 'количество', 'сколько всего']):
            return 'histogram'
        elif any(word in query_lower for word in ['сравнен', 'процент', 'доля', 'соотношен', 'часть', 'какой процент']):
            return 'pie'
        elif any(word in query_lower for word in ['корреляц', 'зависимос', 'связь', 'зависит']):
            return 'scatter'
        elif any(word in query_lower for word in ['таблица', 'список', 'перечень', 'все']):
            return 'table'
        elif any(word in query_lower for word in ['карта', 'гео', 'локац']):
            return 'map'
        elif any(word in query_lower for word in ['выручк', 'доход', 'прибыль', 'бюджет', 'зарплат', 'стоимость']):
            # Для финансовых данных часто подходит столбчатая диаграмма
            return 'bar'
        else:
            return None  # Автоматический выбор
    
    def create_visualization(self, df, chart_type='auto', query=None):
        """Создание визуализации на основе данных"""
        
        if df.empty or len(df) == 0:
            return self._create_empty_visualization("Нет данных для отображения")
        
        try:
            # Если тип не указан, определяем автоматически
            if chart_type is None or chart_type == 'auto':
                chart_type = self._determine_best_chart_type(df)
            
            print(f"📊 Создание визуализации типа: {chart_type}")
            print(f"   Данные: {len(df)} строк, {len(df.columns)} колонок")
            
            if chart_type == 'table':
                return self._create_table(df, query)
            elif chart_type == 'bar':
                return self._create_bar_chart(df, query)
            elif chart_type == 'line':
                return self._create_line_chart(df, query)
            elif chart_type == 'pie':
                return self._create_pie_chart(df, query)
            elif chart_type == 'histogram':
                return self._create_histogram(df, query)
            elif chart_type == 'scatter':
                return self._create_scatter_plot(df, query)
            else:
                return self._create_auto_chart(df, query)
                
        except Exception as e:
            print(f"❌ Ошибка создания визуализации: {str(e)}")
            import traceback
            traceback.print_exc()
            # Возвращаем простую таблицу в случае ошибки
            return self._create_table(df, query)
    
    def _determine_best_chart_type(self, df):
        """Автоматическое определение лучшего типа графика"""
        
        if df.empty:
            return 'table'
        
        numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
        categorical_cols = [col for col in df.columns if not pd.api.types.is_numeric_dtype(df[col])]
        
        # Если есть даты и числовые значения - линейный график
        date_cols = [col for col in df.columns if any(keyword in str(col).lower() for keyword in ['date', 'дата', 'время', 'time'])]
        if date_cols and len(numeric_cols) > 0:
            return 'line'
        
        # Если мало уникальных значений в категориальной колонке - круговая диаграмма
        if len(categorical_cols) > 0:
            try:
                unique_counts = df[categorical_cols[0]].nunique()
                if 2 <= unique_counts <= 8:
                    return 'pie'
            except:
                pass
        
        # Если есть категории и числа - столбчатая
        if len(categorical_cols) > 0 and len(numeric_cols) > 0:
            return 'bar'
        
        # Если только числа - гистограмма
        if len(numeric_cols) > 0 and len(categorical_cols) == 0:
            return 'histogram'
        
        # По умолчанию - таблица
        return 'table'
    
    def _create_table(self, df, query):
        """Создание интерактивной таблицы"""
        
        try:
            # Преобразуем все значения в строки и обрабатываем numpy массивы
            header_values = list(df.columns)
            
            # Преобразуем значения в строки для отображения
            cell_values = []
            for col in df.columns:
                col_data = []
                for val in df[col]:
                    # Обрабатываем разные типы данных
                    if pd.isna(val):
                        col_data.append('')
                    elif isinstance(val, (np.ndarray, list, tuple)):
                        col_data.append(str(val)[:50] + '...' if len(str(val)) > 50 else str(val))
                    else:
                        str_val = str(val)
                        col_data.append(str_val[:50] + '...' if len(str_val) > 50 else str_val)
                cell_values.append(col_data)
            
            # Создаем таблицу Plotly
            fig = go.Figure(data=[go.Table(
                header=dict(
                    values=[str(h) for h in header_values],
                    fill_color='#667eea',
                    align='center',
                    font=dict(color='white', size=12),
                    height=40
                ),
                cells=dict(
                    values=cell_values,
                    fill_color='#f7fafc',
                    align='left',
                    font=dict(color='#2d3748', size=11),
                    height=30
                )
            )])
            
            # Настройки layout
            title = f"Таблица данных: {str(query)[:50]}" if query else "Таблица данных"
            
            fig.update_layout(
                title={
                    'text': title,
                    'font': dict(size=16, color='#2d3748')
                },
                height=min(500, 150 + len(df) * 35),
                margin=dict(l=10, r=10, t=60, b=10),
                paper_bgcolor='white',
                plot_bgcolor='white'
            )
            
            # Конвертируем в JSON с обработкой numpy типов
            return self._to_json(fig.to_dict())
            
        except Exception as e:
            print(f"Ошибка создания таблицы: {e}")
            return self._create_empty_visualization("Ошибка создания таблицы")
    
    def _create_bar_chart(self, df, query):
        """Создание столбчатой диаграммы"""
        
        try:
            # Определяем числовые и категориальные колонки
            numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
            categorical_cols = [col for col in df.columns if not pd.api.types.is_numeric_dtype(df[col])]
            
            if len(numeric_cols) == 0 or len(categorical_cols) == 0:
                print("⚠️ Нет подходящих колонок для столбчатой диаграммы")
                return self._create_table(df, query)
            
            # Выбираем первую категориальную и первую числовую колонку
            x_col = categorical_cols[0]
            y_col = numeric_cols[0]
            
            # Преобразуем данные в списки Python
            x_data = df[x_col].tolist()
            y_data = df[y_col].tolist()
            
            # Обрабатываем NaN значения
            x_data_clean = []
            y_data_clean = []
            for x, y in zip(x_data, y_data):
                if not pd.isna(x) and not pd.isna(y):
                    x_data_clean.append(str(x)[:30])  # Ограничиваем длину текста
                    y_data_clean.append(float(y))
            
            if len(x_data_clean) == 0:
                return self._create_empty_visualization("Нет данных для отображения")
            
            # Если категорий слишком много, берем топ-10
            if len(set(x_data_clean)) > 10:
                # Создаем временный DataFrame для группировки
                temp_df = pd.DataFrame({x_col: x_data_clean, y_col: y_data_clean})
                grouped = temp_df.groupby(x_col)[y_col].sum().nlargest(10)
                top_categories = grouped.index.tolist()
                
                # Фильтруем данные
                filtered_data = [(x, y) for x, y in zip(x_data_clean, y_data_clean) if x in top_categories]
                if filtered_data:
                    x_data_clean, y_data_clean = zip(*filtered_data)
                else:
                    x_data_clean, y_data_clean = x_data_clean[:10], y_data_clean[:10]
            
            # Форматируем значения для отображения на столбцах
            text_data = [self._format_number(y) for y in y_data_clean]
            
            # Создаем столбчатую диаграмму
            fig = go.Figure(data=[
                go.Bar(
                    x=list(x_data_clean),
                    y=list(y_data_clean),
                    marker_color='#667eea',
                    text=text_data,
                    textposition='auto',
                    hovertemplate='<b>%{x}</b><br>%{y:,.0f}<extra></extra>'
                )
            ])
            
            # Настройки layout
            title = f"{self._translate_column(y_col)} по {self._translate_column(x_col)}"
            if query:
                title = f"{str(query)[:60]}..."
            
            fig.update_layout(
                title={
                    'text': title,
                    'font': dict(size=18, color='#2d3748')
                },
                xaxis_title=self._translate_column(x_col),
                yaxis_title=self._translate_column(y_col),
                height=500,
                margin=dict(l=60, r=30, t=80, b=60),
                paper_bgcolor='white',
                plot_bgcolor='white',
                xaxis=dict(tickangle=45 if len(set(x_data_clean)) > 5 else 0),
                hovermode='x'
            )
            
            # Форматируем оси
            if max(y_data_clean) > 1000:
                fig.update_yaxes(tickformat=',.0f')
            
            return self._to_json(fig.to_dict())
            
        except Exception as e:
            print(f"Ошибка создания столбчатой диаграммы: {e}")
            import traceback
            traceback.print_exc()
            return self._create_table(df, query)
    
    def _create_line_chart(self, df, query):
        """Создание линейного графика"""
        
        try:
            # Ищем колонку с датами
            date_cols = [col for col in df.columns if any(keyword in str(col).lower() for keyword in ['date', 'дата', 'время', 'time'])]
            numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
            
            if not date_cols or not numeric_cols:
                print("⚠️ Нет дат или числовых колонок для линейного графика")
                return self._create_bar_chart(df, query)
            
            date_col = date_cols[0]
            value_col = numeric_cols[0]
            
            # Преобразуем даты
            df_copy = df.copy()
            df_copy[date_col] = pd.to_datetime(df_copy[date_col], errors='coerce')
            df_copy = df_copy.dropna(subset=[date_col, value_col])
            
            if df_copy.empty:
                return self._create_empty_visualization("Нет данных для отображения")
            
            df_copy = df_copy.sort_values(date_col)
            
            # Подготавливаем данные
            dates = df_copy[date_col].tolist()
            values = df_copy[value_col].tolist()
            
            # Создаем линейный график
            fig = go.Figure()
            
            fig.add_trace(go.Scatter(
                x=dates,
                y=values,
                mode='lines+markers',
                line=dict(color='#667eea', width=3),
                marker=dict(size=8, color='#764ba2'),
                name=self._translate_column(value_col),
                hovertemplate='%{x|%d.%m.%Y}<br>%{y:,.0f}<extra></extra>'
            ))
            
            # Настройки layout
            title = f"Динамика {self._translate_column(value_col)}"
            if query:
                title = f"{str(query)[:60]}..."
            
            fig.update_layout(
                title={
                    'text': title,
                    'font': dict(size=18, color='#2d3748')
                },
                xaxis_title="Дата",
                yaxis_title=self._translate_column(value_col),
                height=500,
                margin=dict(l=60, r=30, t=80, b=60),
                paper_bgcolor='white',
                plot_bgcolor='white',
                hovermode='x unified'
            )
            
            # Форматируем оси
            fig.update_xaxes(
                tickformat='%d.%m.%Y',
                tickangle=45
            )
            
            if max(values) > 1000:
                fig.update_yaxes(tickformat=',.0f')
            
            return self._to_json(fig.to_dict())
            
        except Exception as e:
            print(f"Ошибка создания линейного графика: {e}")
            import traceback
            traceback.print_exc()
            return self._create_bar_chart(df, query)
    
    def _create_pie_chart(self, df, query):
        """Создание круговой диаграммы"""
        
        try:
            # Определяем категориальные и числовые колонки
            categorical_cols = [col for col in df.columns if not pd.api.types.is_numeric_dtype(df[col])]
            numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
            
            if not categorical_cols:
                print("⚠️ Нет категориальных колонок для круговой диаграммы")
                return self._create_histogram(df, query)
            
            cat_col = categorical_cols[0]
            
            # Если есть числовая колонка, используем ее для значений
            if numeric_cols:
                num_col = numeric_cols[0]
                # Группируем и суммируем
                grouped = df.groupby(cat_col)[num_col].sum().reset_index()
                labels = grouped[cat_col].astype(str).tolist()
                values = grouped[num_col].astype(float).tolist()
            else:
                # Считаем количество
                value_counts = df[cat_col].value_counts()
                labels = value_counts.index.astype(str).tolist()
                values = value_counts.values.astype(float).tolist()
            
            if not labels or not values:
                return self._create_empty_visualization("Нет данных для отображения")
            
            # Ограничиваем количество секторов
            if len(labels) > 8:
                # Берем топ-7, остальное в "Другие"
                top_labels = labels[:7]
                top_values = values[:7]
                other_sum = sum(values[7:])
                
                labels = list(top_labels) + ['Другие']
                values = list(top_values) + [other_sum]
            
            # Создаем круговую диаграмму
            fig = go.Figure(data=[go.Pie(
                labels=labels,
                values=values,
                hole=.3,
                marker_colors=px.colors.qualitative.Set3,
                textinfo='percent+label',
                hovertemplate='<b>%{label}</b><br>%{value:,.0f}<br>%{percent}<extra></extra>'
            )])
            
            # Настройки layout
            title = f"Распределение по {self._translate_column(cat_col)}"
            if query:
                title = f"{str(query)[:60]}..."
            
            fig.update_layout(
                title={
                    'text': title,
                    'font': dict(size=18, color='#2d3748'),
                    'y': 0.95
                },
                height=500,
                margin=dict(l=30, r=30, t=100, b=30),
                paper_bgcolor='white',
                plot_bgcolor='white',
                showlegend=True,
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.2,
                    xanchor="center",
                    x=0.5
                )
            )
            
            return self._to_json(fig.to_dict())
            
        except Exception as e:
            print(f"Ошибка создания круговой диаграммы: {e}")
            import traceback
            traceback.print_exc()
            return self._create_bar_chart(df, query)
    
    def _create_histogram(self, df, query):
        """Создание гистограммы"""
        
        try:
            numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
            
            if not numeric_cols:
                print("⚠️ Нет числовых колонок для гистограммы")
                return self._create_table(df, query)
            
            num_col = numeric_cols[0]
            values = df[num_col].dropna().astype(float).tolist()
            
            if not values:
                return self._create_empty_visualization("Нет данных для отображения")
            
            # Создаем гистограмму
            fig = go.Figure(data=[
                go.Histogram(
                    x=values,
                    nbinsx=min(20, len(set(values))),
                    marker_color='#667eea',
                    opacity=0.7,
                    hovertemplate='Диапазон: %{x}<br>Количество: %{y}<extra></extra>'
                )
            ])
            
            # Настройки layout
            title = f"Распределение {self._translate_column(num_col)}"
            if query:
                title = f"{str(query)[:60]}..."
            
            fig.update_layout(
                title={
                    'text': title,
                    'font': dict(size=18, color='#2d3748')
                },
                xaxis_title=self._translate_column(num_col),
                yaxis_title="Количество",
                height=500,
                margin=dict(l=60, r=30, t=80, b=60),
                paper_bgcolor='white',
                plot_bgcolor='white',
                bargap=0.1
            )
            
            return self._to_json(fig.to_dict())
            
        except Exception as e:
            print(f"Ошибка создания гистограммы: {e}")
            import traceback
            traceback.print_exc()
            return self._create_bar_chart(df, query)
    
    def _create_scatter_plot(self, df, query):
        """Создание точечного графика"""
        
        try:
            numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
            
            if len(numeric_cols) < 2:
                print("⚠️ Недостаточно числовых колонок для scatter plot")
                return self._create_histogram(df, query)
            
            x_col = numeric_cols[0]
            y_col = numeric_cols[1]
            
            # Подготавливаем данные
            x_data = df[x_col].dropna().astype(float).tolist()
            y_data = df[y_col].dropna().astype(float).tolist()
            
            # Синхронизируем данные (удаляем пары с NaN)
            paired_data = [(x, y) for x, y in zip(x_data, y_data) if not pd.isna(x) and not pd.isna(y)]
            if not paired_data:
                return self._create_empty_visualization("Нет данных для отображения")
            
            x_data_clean, y_data_clean = zip(*paired_data)
            
            # Добавляем категорию если есть
            categorical_cols = [col for col in df.columns if not pd.api.types.is_numeric_dtype(df[col])]
            
            if categorical_cols:
                color_col = categorical_cols[0]
                color_data = df[color_col].astype(str).tolist()
                # Синхронизируем с числовыми данными
                color_data_clean = [color_data[i] for i, (x, y) in enumerate(zip(x_data, y_data)) 
                                   if not pd.isna(x) and not pd.isna(y)]
                
                # Создаем scatter plot с цветовой кодировкой
                fig = go.Figure()
                
                # Группируем по категориям
                unique_categories = list(set(color_data_clean))
                for category in unique_categories[:10]:  # Ограничиваем 10 категориями
                    cat_x = [x for x, c in zip(x_data_clean, color_data_clean) if c == category]
                    cat_y = [y for y, c in zip(y_data_clean, color_data_clean) if c == category]
                    
                    if cat_x and cat_y:
                        fig.add_trace(go.Scatter(
                            x=cat_x,
                            y=cat_y,
                            mode='markers',
                            name=str(category)[:20],
                            marker=dict(size=10, opacity=0.7),
                            hovertemplate=f'{self._translate_column(x_col)}: %{{x}}<br>{self._translate_column(y_col)}: %{{y}}<br>Категория: {category}<extra></extra>'
                        ))
            else:
                # Простой scatter plot без категорий
                fig = go.Figure(data=[
                    go.Scatter(
                        x=list(x_data_clean),
                        y=list(y_data_clean),
                        mode='markers',
                        marker=dict(
                            color='#667eea',
                            size=10,
                            opacity=0.7
                        ),
                        hovertemplate=f'{self._translate_column(x_col)}: %{{x}}<br>{self._translate_column(y_col)}: %{{y}}<extra></extra>'
                    )
                ])
            
            # Настройки layout
            title = f"Корреляция: {self._translate_column(x_col)} и {self._translate_column(y_col)}"
            if query:
                title = f"{str(query)[:60]}..."
            
            fig.update_layout(
                title={
                    'text': title,
                    'font': dict(size=18, color='#2d3748')
                },
                xaxis_title=self._translate_column(x_col),
                yaxis_title=self._translate_column(y_col),
                height=500,
                margin=dict(l=60, r=30, t=80, b=60),
                paper_bgcolor='white',
                plot_bgcolor='white',
                hovermode='closest'
            )
            
            return self._to_json(fig.to_dict())
            
        except Exception as e:
            print(f"Ошибка создания scatter plot: {e}")
            import traceback
            traceback.print_exc()
            return self._create_bar_chart(df, query)
    
    def _create_auto_chart(self, df, query):
        """Автоматический выбор типа графика"""
        
        chart_type = self._determine_best_chart_type(df)
        print(f"📈 Автоматически выбран тип: {chart_type}")
        
        if chart_type == 'bar':
            return self._create_bar_chart(df, query)
        elif chart_type == 'line':
            return self._create_line_chart(df, query)
        elif chart_type == 'pie':
            return self._create_pie_chart(df, query)
        elif chart_type == 'histogram':
            return self._create_histogram(df, query)
        elif chart_type == 'scatter':
            return self._create_scatter_plot(df, query)
        else:
            return self._create_table(df, query)
    
    def _create_empty_visualization(self, message="Нет данных"):
        """Создание пустой визуализации"""
        
        try:
            fig = go.Figure()
            
            fig.add_annotation(
                text=message,
                xref="paper", 
                yref="paper",
                x=0.5, 
                y=0.5, 
                showarrow=False,
                font=dict(size=16, color="#718096")
            )
            
            fig.update_layout(
                title={
                    'text': "Визуализация",
                    'font': dict(size=18, color='#2d3748')
                },
                height=400,
                margin=dict(l=10, r=10, t=60, b=10),
                paper_bgcolor='white',
                plot_bgcolor='white'
            )
            
            return self._to_json(fig.to_dict())
            
        except Exception as e:
            print(f"Ошибка создания пустой визуализации: {e}")
            # Возвращаем простой JSON в случае ошибки
            return json.dumps({
                'error': True,
                'message': message
            }, ensure_ascii=False)
    
    def _to_json(self, data):
        """Безопасная конвертация в JSON с обработкой numpy типов"""
        try:
            # Функция для преобразования numpy типов
            def convert(obj):
                if isinstance(obj, (np.integer, np.int64, np.int32)):
                    return int(obj)
                elif isinstance(obj, (np.floating, np.float64, np.float32)):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                elif isinstance(obj, np.bool_):
                    return bool(obj)
                elif pd.isna(obj):
                    return None
                elif isinstance(obj, (pd.Timestamp, datetime)):
                    return obj.isoformat()
                elif hasattr(obj, 'isoformat'):
                    return obj.isoformat()
                return obj
            
            # Рекурсивно преобразуем данные
            def recursive_convert(obj):
                if isinstance(obj, dict):
                    return {k: recursive_convert(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [recursive_convert(item) for item in obj]
                else:
                    return convert(obj)
            
            converted_data = recursive_convert(data)
            return json.dumps(converted_data, ensure_ascii=False)
            
        except Exception as e:
            print(f"Ошибка конвертации в JSON: {e}")
            return json.dumps({
                'error': True,
                'message': f'Ошибка создания визуализации: {str(e)}'
            }, ensure_ascii=False)
    
    def _translate_column(self, column_name):
        """Перевод названий колонок на русский"""
        translations = {
            'project_id': 'ID проекта',
            'project_name': 'Название проекта',
            'budget': 'Бюджет',
            'revenue': 'Выручка',
            'salary': 'Зарплата',
            'employee_id': 'ID сотрудника',
            'first_name': 'Имя',
            'last_name': 'Фамилия',
            'department': 'Отдел',
            'position': 'Должность',
            'hire_date': 'Дата приема',
            'performance_score': 'Оценка эффективности',
            'start_date': 'Дата начала',
            'end_date': 'Дата окончания',
            'status': 'Статус',
            'manager_id': 'ID руководителя',
            'equipment_id': 'ID оборудования',
            'equipment_name': 'Название оборудования',
            'type': 'Тип',
            'purchase_date': 'Дата покупки',
            'maintenance_date': 'Дата обслуживания',
            'cost': 'Стоимость',
            'production_id': 'ID производства',
            'date': 'Дата',
            'product_name': 'Название продукта',
            'quantity': 'Количество',
            'incident_id': 'ID инцидента',
            'description': 'Описание',
            'severity': 'Уровень серьезности',
            'resolved': 'Решен',
            'resolution_time_hours': 'Время решения (часы)',
            'total_revenue': 'Общая выручка',
            'employee_count': 'Количество сотрудников',
            'average_salary': 'Средняя зарплата'
        }
        
        # Если название колонки - строка, ищем перевод
        if isinstance(column_name, str):
            return translations.get(column_name, column_name)
        
        # Если это не строка, просто возвращаем как есть
        return str(column_name)
    
    def _format_number(self, num):
        """Форматирование чисел для отображения"""
        try:
            if pd.isna(num):
                return ''
            
            num = float(num)
            if num >= 1_000_000_000:
                return f"{num/1_000_000_000:.1f} млрд"
            elif num >= 1_000_000:
                return f"{num/1_000_000:.1f} млн"
            elif num >= 1_000:
                return f"{num/1_000:.1f} тыс"
            elif num == int(num):
                return f"{int(num):,}".replace(',', ' ')
            else:
                return f"{num:,.1f}".replace(',', ' ')
        except (ValueError, TypeError):
            return str(num)