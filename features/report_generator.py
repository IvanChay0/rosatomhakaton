import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import re

class ReportGenerator:
    def __init__(self):
        pass
    
    def generate_text_analysis(self, df, original_query):
        """Генерация интеллектуального анализа данных"""
        
        if df.empty:
            return self._format_empty_response(original_query)
        
        try:
            # Анализируем запрос пользователя
            query_type = self._analyze_query_type(original_query)
            
            # Генерируем ответ в зависимости от типа запроса
            if query_type == "comparison":
                return self._generate_comparison_analysis(df, original_query)
            elif query_type == "ranking":
                return self._generate_ranking_analysis(df, original_query)
            elif query_type == "aggregation":
                return self._generate_aggregation_analysis(df, original_query)
            elif query_type == "trend":
                return self._generate_trend_analysis(df, original_query)
            elif query_type == "distribution":
                return self._generate_distribution_analysis(df, original_query)
            else:
                return self._generate_general_analysis(df, original_query)
                
        except Exception as e:
            print(f"Ошибка генерации анализа: {str(e)}")
            return self._generate_simple_analysis(df, original_query)
    
    def _analyze_query_type(self, query):
        """Анализ типа запроса"""
        query_lower = query.lower()
        
        if any(word in query_lower for word in ['сравн', 'compare', 'сопостав', 'против']):
            return "comparison"
        elif any(word in query_lower for word in ['топ', 'лучш', 'первые', 'последние', 'ranking', 'рейтинг']):
            return "ranking"
        elif any(word in query_lower for word in ['сколько', 'сумм', 'общ', 'всего', 'итог', 'total', 'sum']):
            return "aggregation"
        elif any(word in query_lower for word in ['тренд', 'динамик', 'изменен', 'рост', 'снижен', 'trend']):
            return "trend"
        elif any(word in query_lower for word in ['распределен', 'частота', 'сколько всего', 'distribution']):
            return "distribution"
        else:
            return "general"
    
    def _generate_comparison_analysis(self, df, query):
        """Анализ для сравнений"""
        analysis = f"# 🔍 Сравнительный анализ\n\n"
        analysis += f"**Запрос:** {query}\n\n"
        
        # Базовая информация
        analysis += f"## 📋 Обзор данных\n\n"
        analysis += f"• **Всего записей:** {len(df):,}\n"
        analysis += f"• **Количество показателей:** {len(df.columns)}\n\n"
        
        # Находим ключевые колонки для сравнения
        numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
        categorical_cols = [col for col in df.columns if not pd.api.types.is_numeric_dtype(df[col])]
        
        if numeric_cols and categorical_cols:
            # Для сравнения обычно нужна категория и число
            cat_col = categorical_cols[0]
            num_col = numeric_cols[0]
            
            analysis += f"## 📊 Сравнение по '{self._translate_column(cat_col)}'\n\n"
            
            # Группируем и сравниваем
            grouped = df.groupby(cat_col)[num_col].agg(['sum', 'mean', 'count']).round(2)
            grouped = grouped.sort_values('sum', ascending=False)
            
            for category, row in grouped.head(5).iterrows():
                analysis += f"### {category}\n"
                analysis += f"- **Общее значение:** {self._format_number(row['sum'])}\n"
                analysis += f"- **В среднем:** {self._format_number(row['mean'])}\n"
                analysis += f"- **Количество записей:** {int(row['count'])}\n\n"
            
            if len(grouped) > 5:
                analysis += f"*... и еще {len(grouped) - 5} категорий*\n\n"
        
        # Инсайты
        analysis += f"## 💡 Ключевые выводы\n\n"
        
        insights = []
        if len(numeric_cols) >= 2:
            # Сравниваем числовые колонки
            col1, col2 = numeric_cols[0], numeric_cols[1]
            corr = df[col1].corr(df[col2])
            
            if abs(corr) > 0.7:
                insights.append(f"Сильная корреляция между '{self._translate_column(col1)}' и '{self._translate_column(col2)}' ({corr:.2f})")
            elif abs(corr) > 0.3:
                insights.append(f"Умеренная связь между '{self._translate_column(col1)}' и '{self._translate_column(col2)}'")
        
        if categorical_cols:
            main_cat = categorical_cols[0]
            unique_count = df[main_cat].nunique()
            if unique_count <= 10:
                insights.append(f"Данные разделены на {unique_count} категорий для сравнения")
        
        if not insights:
            insights.append("Данные подходят для детального сравнения показателей")
            insights.append("Рекомендуется использовать визуализацию для наглядности")
        
        for i, insight in enumerate(insights[:3], 1):
            analysis += f"{i}. {insight}\n"
        analysis += "\n"
        
        # Рекомендации
        analysis += f"## 🚀 Рекомендации\n\n"
        recommendations = [
            "Используйте графики сравнения (столбчатые диаграммы)",
            "Примените фильтры для анализа конкретных категорий",
            "Сравните медианные значения для устранения влияния выбросов",
            "Рассмотрите относительные показатели (проценты, доли)",
            "Экспортируйте данные для детального сравнения в Excel"
        ]
        
        for i, rec in enumerate(recommendations, 1):
            analysis += f"{i}. {rec}\n"
        
        return self._format_response(analysis)
    
    def _generate_ranking_analysis(self, df, query):
        """Анализ для рейтингов и топов"""
        analysis = f"# 🏆 Рейтинговый анализ\n\n"
        analysis += f"**Запрос:** {query}\n\n"
        
        # Определяем, по какому показателю ранжировать
        numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
        categorical_cols = [col for col in df.columns if not pd.api.types.is_numeric_dtype(df[col])]
        
        if not numeric_cols:
            return self._generate_general_analysis(df, query)
        
        # Выбираем основной числовой показатель для ранжирования
        rank_col = numeric_cols[0]
        
        # Сортируем данные
        df_sorted = df.sort_values(rank_col, ascending=False).head(10)
        
        analysis += f"## 📊 Топ-{len(df_sorted)} по '{self._translate_column(rank_col)}'\n\n"
        
        for idx, (_, row) in enumerate(df_sorted.iterrows(), 1):
            # Формируем описание строки
            description = self._describe_row_for_ranking(row, categorical_cols)
            value = self._format_number(row[rank_col])
            
            medal = ""
            if idx == 1:
                medal = "🥇 "
            elif idx == 2:
                medal = "🥈 "
            elif idx == 3:
                medal = "🥉 "
            
            analysis += f"### {medal}{idx}. {description}\n"
            analysis += f"- **{self._translate_column(rank_col)}:** {value}\n"
            
            # Дополнительная информация
            if len(numeric_cols) > 1:
                extra_col = numeric_cols[1]
                extra_value = self._format_number(row[extra_col])
                analysis += f"- **{self._translate_column(extra_col)}:** {extra_value}\n"
            
            analysis += "\n"
        
        # Статистика
        analysis += f"## 📈 Статистика ранжирования\n\n"
        analysis += f"• **Всего в рейтинге:** {len(df)} записей\n"
        analysis += f"• **Лидер (максимум):** {self._format_number(df[rank_col].max())}\n"
        analysis += f"• **Среднее значение:** {self._format_number(df[rank_col].mean())}\n"
        analysis += f"• **Разрыв лидера от среднего:** {self._format_number((df[rank_col].max() / df[rank_col].mean() - 1) * 100)}%\n\n"
        
        # Инсайты
        analysis += f"## 💡 Наблюдения\n\n"
        
        # Проверяем на выбросы
        q75, q25 = np.percentile(df[rank_col].dropna(), [75, 25])
        iqr = q75 - q25
        outliers = df[df[rank_col] > q75 + 1.5 * iqr]
        
        if len(outliers) > 0:
            analysis += f"1. **Обнаружены выдающиеся значения** ({len(outliers)} записей значительно выше среднего)\n"
        
        # Проверяем равномерность распределения
        if df[rank_col].std() / df[rank_col].mean() > 0.5:
            analysis += f"2. **Высокая вариативность** данных (значения сильно различаются)\n"
        else:
            analysis += f"2. **Относительно равномерное** распределение значений\n"
        
        if len(df_sorted) < len(df):
            analysis += f"3. Показаны только топ-{len(df_sorted)} из {len(df)} записей\n"
        
        analysis += "\n"
        
        return self._format_response(analysis)
    
    def _generate_aggregation_analysis(self, df, query):
        """Анализ для агрегированных запросов"""
        analysis = f"# 🧮 Агрегированный анализ\n\n"
        analysis += f"**Запрос:** {query}\n\n"
        
        # Находим числовые колонки для агрегации
        numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
        
        if not numeric_cols:
            # Если нет числовых колонок, просто считаем количество
            analysis += f"## 📊 Результаты подсчета\n\n"
            analysis += f"• **Всего записей:** {len(df):,}\n\n"
            
            # Считаем уникальные значения для категориальных колонок
            categorical_cols = [col for col in df.columns if not pd.api.types.is_numeric_dtype(df[col])]
            if categorical_cols:
                for col in categorical_cols[:2]:
                    unique_count = df[col].nunique()
                    analysis += f"• **Уникальных значений в '{self._translate_column(col)}':** {unique_count}\n"
            
            return self._format_response(analysis)
        
        analysis += f"## 📈 Суммарные показатели\n\n"
        
        for col in numeric_cols[:3]:  # Ограничиваем 3 показателями
            total = df[col].sum()
            avg = df[col].mean()
            median_val = df[col].median()
            
            analysis += f"### {self._translate_column(col)}\n"
            analysis += f"- **Общая сумма:** {self._format_number(total)}\n"
            
            if len(df) > 1:
                analysis += f"- **Среднее значение:** {self._format_number(avg)}\n"
                analysis += f"- **Медиана:** {self._format_number(median_val)}\n"
                
                # Процент от общего если есть контекст
                if len(numeric_cols) > 1 and total > 0:
                    percentage = (total / sum(df[numeric_cols[0]].sum() for _ in numeric_cols[:3])) * 100
                    analysis += f"- **Доля от общего:** {percentage:.1f}%\n"
            
            analysis += "\n"
        
        # Группировка по категориям если есть
        categorical_cols = [col for col in df.columns if not pd.api.types.is_numeric_dtype(df[col])]
        
        if categorical_cols and len(df) > 5:
            cat_col = categorical_cols[0]
            num_col = numeric_cols[0]
            
            analysis += f"## 📊 Распределение по '{self._translate_column(cat_col)}'\n\n"
            
            grouped = df.groupby(cat_col)[num_col].sum().nlargest(5)
            
            for category, value in grouped.items():
                percentage = (value / df[num_col].sum()) * 100
                analysis += f"- **{category}:** {self._format_number(value)} ({percentage:.1f}%)\n"
            
            analysis += "\n"
        
        # Инсайты
        analysis += f"## 💡 Основные выводы\n\n"
        
        main_col = numeric_cols[0]
        total = df[main_col].sum()
        
        if total > 1000000:
            analysis += f"1. **Значительный объем** - общая сумма составляет {self._format_number(total)}\n"
        elif total < 1000:
            analysis += f"1. **Небольшой объем** данных для анализа\n"
        
        if len(df) > 100:
            analysis += f"2. **Большая выборка** ({len(df):,} записей) обеспечивает надежность\n"
        elif len(df) < 10:
            analysis += f"2. **Маленькая выборка** - результаты требуют осторожной интерпретации\n"
        
        # Проверка на выбросы
        if len(df) > 10:
            q1 = df[main_col].quantile(0.25)
            q3 = df[main_col].quantile(0.75)
            iqr = q3 - q1
            outliers = df[(df[main_col] < q1 - 1.5*iqr) | (df[main_col] > q3 + 1.5*iqr)]
            
            if len(outliers) > 0:
                analysis += f"3. **Обнаружены аномальные значения** ({len(outliers)} выбросов)\n"
        
        analysis += "\n"
        
        return self._format_response(analysis)
    
    def _generate_trend_analysis(self, df, query):
        """Анализ для трендов и динамики"""
        analysis = f"# 📈 Анализ динамики\n\n"
        analysis += f"**Запрос:** {query}\n\n"
        
        # Ищем колонки с датами
        date_cols = []
        for col in df.columns:
            col_lower = str(col).lower()
            if any(keyword in col_lower for keyword in ['date', 'дата', 'time', 'время', 'год', 'месяц', 'день']):
                date_cols.append(col)
        
        if not date_cols:
            return self._generate_general_analysis(df, query)
        
        date_col = date_cols[0]
        numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
        
        if not numeric_cols:
            return self._generate_general_analysis(df, query)
        
        num_col = numeric_cols[0]
        
        try:
            # Преобразуем даты
            df_copy = df.copy()
            df_copy['_temp_date'] = pd.to_datetime(df_copy[date_col], errors='coerce')
            df_copy = df_copy.dropna(subset=['_temp_date', num_col])
            
            if len(df_copy) < 2:
                return self._generate_general_analysis(df, query)
            
            df_copy = df_copy.sort_values('_temp_date')
            
            analysis += f"## 📅 Период анализа\n\n"
            min_date = df_copy['_temp_date'].min().strftime('%d.%m.%Y')
            max_date = df_copy['_temp_date'].max().strftime('%d.%m.%Y')
            days_count = (df_copy['_temp_date'].max() - df_copy['_temp_date'].min()).days
            
            analysis += f"• **Начало периода:** {min_date}\n"
            analysis += f"• **Конец периода:** {max_date}\n"
            analysis += f"• **Длительность:** {days_count} дней\n"
            analysis += f"• **Количество точек данных:** {len(df_copy)}\n\n"
            
            # Анализ тренда
            analysis += f"## 📊 Динамика '{self._translate_column(num_col)}'\n\n"
            
            first_value = df_copy.iloc[0][num_col]
            last_value = df_copy.iloc[-1][num_col]
            total_change = last_value - first_value
            percent_change = (total_change / first_value * 100) if first_value != 0 else 0
            
            trend_emoji = "↗️" if total_change > 0 else "↘️" if total_change < 0 else "➡️"
            trend_word = "рост" if total_change > 0 else "снижение" if total_change < 0 else "стабильность"
            
            analysis += f"• **Начальное значение:** {self._format_number(first_value)}\n"
            analysis += f"• **Конечное значение:** {self._format_number(last_value)}\n"
            analysis += f"• **Изменение:** {trend_emoji} {self._format_number(total_change)} ({percent_change:+.1f}%)\n"
            analysis += f"• **Тренд:** {trend_word}\n\n"
            
            # Анализ по периодам если достаточно данных
            if len(df_copy) >= 3:
                # Делим на трети для сравнения
                third = len(df_copy) // 3
                first_third_avg = df_copy.head(third)[num_col].mean()
                last_third_avg = df_copy.tail(third)[num_col].mean()
                third_change = ((last_third_avg - first_third_avg) / first_third_avg * 100) if first_third_avg != 0 else 0
                
                analysis += f"### 📊 Сравнение периодов\n\n"
                analysis += f"• **Среднее в начале периода:** {self._format_number(first_third_avg)}\n"
                analysis += f"• **Среднее в конце периода:** {self._format_number(last_third_avg)}\n"
                analysis += f"• **Изменение среднего:** {third_change:+.1f}%\n\n"
            
            # Статистика
            analysis += f"## 📈 Статистические показатели\n\n"
            analysis += f"• **Среднее значение:** {self._format_number(df_copy[num_col].mean())}\n"
            analysis += f"• **Медиана:** {self._format_number(df_copy[num_col].median())}\n"
            analysis += f"• **Минимум:** {self._format_number(df_copy[num_col].min())}\n"
            analysis += f"• **Максимум:** {self._format_number(df_copy[num_col].max())}\n"
            analysis += f"• **Волатильность (стандартное отклонение):** {self._format_number(df_copy[num_col].std())}\n\n"
            
            # Инсайты
            analysis += f"## 💡 Ключевые наблюдения\n\n"
            
            insights = []
            
            if abs(percent_change) > 20:
                insights.append(f"Значительный {trend_word} за период ({percent_change:+.1f}%)")
            elif abs(percent_change) > 5:
                insights.append(f"Умеренный {trend_word} за период")
            else:
                insights.append("Относительно стабильные показатели")
            
            if df_copy[num_col].std() / df_copy[num_col].mean() > 0.3:
                insights.append("Высокая волатильность данных")
            
            if len(df_copy) >= 10:
                # Проверяем на сезонность/периодичность
                insights.append(f"Достаточно данных ({len(df_copy)} точек) для анализа паттернов")
            
            for i, insight in enumerate(insights, 1):
                analysis += f"{i}. {insight}\n"
            
            analysis += "\n"
            
            # Рекомендации
            analysis += f"## 🚀 Рекомендации\n\n"
            recommendations = [
                "Используйте линейные графики для визуализации тренда",
                "Рассмотрите разбивку по месяцам/кварталам",
                "Исключите выбросы для более четкого тренда",
                "Проведите анализ сезонности при наличии данных",
                "Спрогнозируйте значения на следующий период"
            ]
            
            for i, rec in enumerate(recommendations, 1):
                analysis += f"{i}. {rec}\n"
            
        except Exception as e:
            print(f"Ошибка анализа тренда: {e}")
            return self._generate_general_analysis(df, query)
        
        return self._format_response(analysis)
    
    def _generate_distribution_analysis(self, df, query):
        """Анализ распределения"""
        analysis = f"# 📊 Анализ распределения\n\n"
        analysis += f"**Запрос:** {query}\n\n"
        
        analysis += f"## 📋 Обзор данных\n\n"
        analysis += f"• **Всего записей:** {len(df):,}\n"
        analysis += f"• **Количество показателей:** {len(df.columns)}\n\n"
        
        # Анализируем числовые колонки
        numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
        
        if numeric_cols:
            analysis += f"## 📈 Распределение числовых показателей\n\n"
            
            for col in numeric_cols[:2]:  # Ограничиваем 2 показателями
                values = df[col].dropna()
                
                if len(values) == 0:
                    continue
                
                analysis += f"### {self._translate_column(col)}\n"
                analysis += f"• **Диапазон:** от {self._format_number(values.min())} до {self._format_number(values.max())}\n"
                analysis += f"• **Среднее:** {self._format_number(values.mean())}\n"
                analysis += f"• **Медиана:** {self._format_number(values.median())}\n"
                
                # Асимметрия
                skewness = values.skew()
                if abs(skewness) > 1:
                    skew_type = "сильно скошенное" if skewness > 0 else "сильно левоскошенное"
                elif abs(skewness) > 0.5:
                    skew_type = "скошенное" if skewness > 0 else "левоскошенное"
                else:
                    skew_type = "симметричное"
                
                analysis += f"• **Распределение:** {skew_type} (асимметрия: {skewness:.2f})\n"
                
                # Процентили
                percentiles = values.quantile([0.25, 0.5, 0.75])
                analysis += f"• **25-й процентиль:** {self._format_number(percentiles[0.25])}\n"
                analysis += f"• **75-й процентиль:** {self._format_number(percentiles[0.75])}\n\n"
        
        # Анализируем категориальные колонки
        categorical_cols = [col for col in df.columns if not pd.api.types.is_numeric_dtype(df[col])]
        
        if categorical_cols:
            analysis += f"## 🏷️ Распределение категорий\n\n"
            
            for col in categorical_cols[:2]:  # Ограничиваем 2 колонками
                value_counts = df[col].value_counts()
                total = len(df[col].dropna())
                
                analysis += f"### {self._translate_column(col)}\n"
                analysis += f"• **Уникальных значений:** {len(value_counts)}\n"
                analysis += f"• **Самая частая категория:** {value_counts.index[0]} ({value_counts.iloc[0] / total * 100:.1f}%)\n"
                
                if len(value_counts) <= 10:
                    analysis += f"• **Все категории:**\n"
                    for value, count in value_counts.head(5).items():
                        percentage = count / total * 100
                        analysis += f"  - {value}: {count} ({percentage:.1f}%)\n"
                else:
                    analysis += f"• **Топ-5 категорий:**\n"
                    for value, count in value_counts.head(5).items():
                        percentage = count / total * 100
                        analysis += f"  - {value}: {count} ({percentage:.1f}%)\n"
                
                analysis += "\n"
        
        # Инсайты
        analysis += f"## 💡 Выводы\n\n"
        
        insights = []
        
        if numeric_cols:
            main_num = numeric_cols[0]
            cv = df[main_num].std() / df[main_num].mean() * 100 if df[main_num].mean() != 0 else 0
            
            if cv > 100:
                insights.append("Очень высокая вариативность данных")
            elif cv > 50:
                insights.append("Значительный разброс значений")
            elif cv > 20:
                insights.append("Умеренная вариативность")
            else:
                insights.append("Относительно однородные данные")
        
        if categorical_cols:
            main_cat = categorical_cols[0]
            top_percentage = df[main_cat].value_counts().iloc[0] / len(df) * 100
            
            if top_percentage > 50:
                insights.append(f"Доминирующая категория ({top_percentage:.0f}% всех записей)")
            elif top_percentage < 20:
                insights.append("Равномерное распределение по категориям")
        
        if len(df) > 1000:
            insights.append("Большой объем данных обеспечивает статистическую значимость")
        
        for i, insight in enumerate(insights[:3], 1):
            analysis += f"{i}. {insight}\n"
        
        analysis += "\n"
        
        return self._format_response(analysis)
    
    def _generate_general_analysis(self, df, query):
        """Общий анализ для любых запросов"""
        analysis = f"# 📋 Обзор данных\n\n"
        analysis += f"**Запрос:** {query}\n\n"
        
        analysis += f"## 📊 Базовая информация\n\n"
        analysis += f"• **Количество записей:** {len(df):,}\n"
        analysis += f"• **Количество колонок:** {len(df.columns)}\n"
        
        # Типы данных
        numeric_count = sum(1 for col in df.columns if pd.api.types.is_numeric_dtype(df[col]))
        text_count = len(df.columns) - numeric_count
        
        analysis += f"• **Числовых показателей:** {numeric_count}\n"
        analysis += f"• **Текстовых показателей:** {text_count}\n\n"
        
        # Обзор колонок
        analysis += f"## 📑 Структура данных\n\n"
        
        for i, col in enumerate(df.columns[:5], 1):  # Показываем первые 5 колонок
            col_type = "числовой" if pd.api.types.is_numeric_dtype(df[col]) else "текстовый"
            unique_count = df[col].nunique()
            
            analysis += f"{i}. **{self._translate_column(col)}** ({col_type})\n"
            analysis += f"   - Уникальных значений: {unique_count}\n"
            
            if col_type == "числовой":
                analysis += f"   - Диапазон: {self._format_number(df[col].min())} - {self._format_number(df[col].max())}\n"
            elif unique_count <= 5:
                analysis += f"   - Примеры: {', '.join(map(str, df[col].unique()[:3]))}\n"
            
            analysis += "\n"
        
        if len(df.columns) > 5:
            analysis += f"*... и еще {len(df.columns) - 5} показателей*\n\n"
        
        # Пример данных
        if len(df) > 0:
            analysis += f"## 👁️ Пример записей\n\n"
            
            for i in range(min(3, len(df))):
                row = df.iloc[i]
                # Берем первые 3 колонки
                sample_cols = df.columns[:3]
                sample_text = []
                
                for col in sample_cols:
                    value = row[col]
                    if pd.isna(value):
                        sample_text.append(f"{self._translate_column(col)}: нет данных")
                    else:
                        val_str = str(value)
                        if len(val_str) > 30:
                            val_str = val_str[:27] + "..."
                        sample_text.append(f"{self._translate_column(col)}: {val_str}")
                
                analysis += f"**Запись {i+1}:** {', '.join(sample_text)}\n\n"
        
        # Инсайты
        analysis += f"## 💡 Что можно сделать с этими данными?\n\n"
        
        suggestions = []
        
        if numeric_count >= 2:
            suggestions.append("Проанализировать корреляцию между числовыми показателями")
        
        if text_count >= 1 and numeric_count >= 1:
            suggestions.append("Сравнить числовые показатели по категориям")
        
        if len(df) > 50:
            suggestions.append("Провести статистический анализ распределения")
        
        suggestions.append("Создать визуализации для лучшего понимания")
        suggestions.append("Экспортировать данные для углубленного анализа")
        
        for i, suggestion in enumerate(suggestions[:5], 1):
            analysis += f"{i}. {suggestion}\n"
        
        analysis += "\n"
        
        return self._format_response(analysis)
    
    def _generate_simple_analysis(self, df, query):
        """Простой анализ в случае ошибок"""
        analysis = f"# 📋 Результаты запроса\n\n"
        analysis += f"**Запрос:** {query}\n\n"
        analysis += f"✅ **Данные успешно получены**\n\n"
        analysis += f"• Количество записей: **{len(df):,}**\n"
        analysis += f"• Колонки: {', '.join([self._translate_column(col) for col in df.columns[:5]])}\n"
        
        if len(df.columns) > 5:
            analysis += f"  ... и еще {len(df.columns) - 5} показателей\n"
        
        # Сумма числовых колонок
        numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
        if numeric_cols:
            analysis += f"\n**Суммарные значения:**\n"
            for col in numeric_cols[:2]:
                total = df[col].sum()
                if abs(total) > 0:
                    analysis += f"• {self._translate_column(col)}: **{self._format_number(total)}**\n"
        
        analysis += f"\n💡 **Используйте вкладку 'Визуализация' для графического представления данных.**"
        
        return self._format_response(analysis)
    
    def _format_empty_response(self, query):
        """Форматирование ответа при отсутствии данных"""
        response = f"# 📭 Результат запроса\n\n"
        response += f"**Запрос:** {query}\n\n"
        response += f"❌ **Данные не найдены**\n\n"
        response += f"Запрос не вернул результатов. Возможно:\n\n"
        response += f"1. **Нет данных**, соответствующих критериям\n"
        response += f"2. **Ошибка в запросе** или фильтрах\n"
        response += f"3. **Проблема с подключением** к данным\n\n"
        response += f"**Рекомендации:**\n\n"
        response += f"• Проверьте формулировку запроса\n"
        response += f"• Упростите условия фильтрации\n"
        response += f"• Используйте примеры запросов из раздела выше\n"
        
        return self._format_response(response)
    
    def _format_response(self, text):
        """Форматирование ответа с правильными отступами и разметкой"""
        # Убираем лишние пустые строки
        lines = text.strip().split('\n')
        formatted_lines = []
        
        for line in lines:
            line = line.rstrip()
            if line.strip() == '' and formatted_lines and formatted_lines[-1].strip() == '':
                continue  # Пропускаем последовательные пустые строки
            formatted_lines.append(line)
        
        return '\n'.join(formatted_lines)
    
    def _describe_row_for_ranking(self, row, categorical_cols):
        """Описание строки для рейтинга"""
        if not categorical_cols:
            # Если нет категорий, используем первые значения
            values = []
            for i in range(min(2, len(row))):
                val = row.iloc[i]
                if not pd.isna(val):
                    values.append(str(val)[:20])
            return ", ".join(values) if values else "Запись"
        
        # Используем категориальные колонки для описания
        desc_parts = []
        for col in categorical_cols[:2]:
            if col in row.index and not pd.isna(row[col]):
                val = str(row[col])
                desc_parts.append(val[:25])
        
        return ", ".join(desc_parts) if desc_parts else "Запись"
    
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
            'average_salary': 'Средняя зарплата',
            'quantity': 'Количество',
            'price': 'Цена',
            'amount': 'Сумма',
            'value': 'Значение',
            'count': 'Количество',
            'sum': 'Сумма',
            'avg': 'Среднее',
            'min': 'Минимум',
            'max': 'Максимум'
        }
        
        if isinstance(column_name, str):
            return translations.get(column_name, column_name.replace('_', ' ').title())
        return str(column_name)
    
    def _format_number(self, value, decimals=None):
        """Форматирование чисел"""
        if pd.isna(value):
            return "нет данных"
        
        try:
            value = float(value)
            
            # Определяем количество знаков после запятой
            if decimals is None:
                if value == 0:
                    return "0"
                elif abs(value) < 0.01:
                    decimals = 4
                elif abs(value) < 1:
                    decimals = 3
                elif abs(value) < 100:
                    decimals = 2
                elif abs(value) < 1000:
                    decimals = 1
                else:
                    decimals = 0
            else:
                decimals = int(decimals)
            
            # Форматируем в зависимости от размера
            if abs(value) >= 1_000_000_000:
                formatted = f"{value/1_000_000_000:,.{max(0, decimals-1)}f}".rstrip('0').rstrip('.')
                return f"{formatted} млрд"
            elif abs(value) >= 1_000_000:
                formatted = f"{value/1_000_000:,.{max(0, decimals-1)}f}".rstrip('0').rstrip('.')
                return f"{formatted} млн"
            elif abs(value) >= 1_000:
                formatted = f"{value/1_000:,.{max(0, decimals-1)}f}".rstrip('0').rstrip('.')
                return f"{formatted} тыс"
            else:
                if decimals == 0:
                    return f"{int(value):,}".replace(',', ' ')
                else:
                    formatted = f"{value:,.{decimals}f}".rstrip('0').rstrip('.')
                    return formatted.replace(',', ' ')
                    
        except (ValueError, TypeError):
            return str(value)
    
def generate_report(self, report_type='summary', filters=None):
    """Генерация отчета с данными (для совместимости)"""
    
    # В реальной реализации здесь будет запрос к БД
    # Сейчас возвращаем структурированные данные для фронтенда
    
    report_data = {
        'title': f'Отчет {report_type}',
        'date': datetime.now().strftime('%d.%m.%Y %H:%M'),
        'type': report_type,
        'metrics': {
            'Сгенерировано': 'Да',
            'Тип отчета': report_type,
            'Формат': 'Стандартный'
        },
        'data': [
            {'Показатель': 'Пример 1', 'Значение': 100},
            {'Показатель': 'Пример 2', 'Значение': 200},
            {'Показатель': 'Пример 3', 'Значение': 300}
        ],
        'columns': ['Показатель', 'Значение'],
        'analysis': 'Это демонстрационный отчет. В реальной системе здесь будут данные из базы.',
        'recommendations': [
            'Интегрируйте с реальной базой данных',
            'Настройте фильтры для отчетов',
            'Добавьте больше типов отчетов'
        ]
    }
    
    return json.dumps(report_data, ensure_ascii=False, indent=2)

    