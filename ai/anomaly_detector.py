import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import warnings
warnings.filterwarnings('ignore')

class AnomalyDetector:
    def __init__(self):
        self.models = {}
        self.scalers = {}
    
    def detect_anomalies(self, df, columns=None, contamination=0.1):
        """Обнаружение аномалий в данных"""
        
        if df.empty or len(df) < 10:
            return pd.DataFrame()
        
        # Используем все числовые колонки если не указаны
        if columns is None:
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            columns = numeric_cols
        
        # Выбираем только указанные колонки
        data = df[columns].copy()
        
        # Заполняем пропущенные значения
        data = data.fillna(data.mean())
        
        # Масштабируем данные
        scaler = StandardScaler()
        scaled_data = scaler.fit_transform(data)
        
        # Обучаем модель Isolation Forest
        model = IsolationForest(
            contamination=contamination,
            random_state=42,
            n_estimators=100
        )
        
        predictions = model.fit_predict(scaled_data)
        
        # Добавляем метки аномалий в исходный DataFrame
        result_df = df.copy()
        result_df['is_anomaly'] = predictions == -1
        result_df['anomaly_score'] = model.decision_function(scaled_data)
        
        return result_df
    
    def detect_time_series_anomalies(self, df, value_column, date_column=None):
        """Обнаружение аномалий во временных рядах"""
        
        if len(df) < 20:
            return df
        
        if date_column and date_column in df.columns:
            df = df.sort_values(date_column)
        
        values = df[value_column].values.reshape(-1, 1)
        
        # Используем простой метод z-score для временных рядов
        mean = np.mean(values)
        std = np.std(values)
        
        if std == 0:
            df['is_anomaly'] = False
            df['z_score'] = 0
        else:
            z_scores = np.abs((values - mean) / std)
            df['z_score'] = z_scores.flatten()
            df['is_anomaly'] = z_scores.flatten() > 3  # Порог 3 стандартных отклонения
        
        return df
    
    def generate_anomaly_report(self, df_with_anomalies):
        """Генерация отчета об аномалиях"""
        
        if df_with_anomalies.empty or 'is_anomaly' not in df_with_anomalies.columns:
            return "Аномалии не обнаружены"
        
        anomalies = df_with_anomalies[df_with_anomalies['is_anomaly']]
        
        if len(anomalies) == 0:
            return "Аномалии не обнаружены"
        
        report = f"Обнаружено аномалий: {len(anomalies)}\n\n"
        report += "Топ аномалий:\n"
        
        # Выбираем топ 5 аномалий по наибольшему z-score или anomaly_score
        if 'z_score' in anomalies.columns:
            top_anomalies = anomalies.nlargest(5, 'z_score')
            for idx, row in top_anomalies.iterrows():
                report += f"- Значение: {row.get('z_score', 0):.2f}σ\n"
        elif 'anomaly_score' in anomalies.columns:
            top_anomalies = anomalies.nlargest(5, 'anomaly_score')
            for idx, row in top_anomalies.iterrows():
                report += f"- Счет аномалии: {row.get('anomaly_score', 0):.3f}\n"
        
        return report