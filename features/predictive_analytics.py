import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import LabelEncoder
import warnings
warnings.filterwarnings('ignore')

class PredictiveAnalytics:
    def __init__(self):
        self.models = {}
        
    def predict_trend(self, df, target_column, date_column=None, periods=5):
        """Прогнозирование тренда"""
        
        if df.empty or len(df) < 10:
            return None
        
        try:
            # Если есть колонка с датами, используем ее как индекс
            if date_column and date_column in df.columns:
                df = df.sort_values(date_column)
                df['time_index'] = range(len(df))
                X = df[['time_index']].values.reshape(-1, 1)
            else:
                X = np.array(range(len(df))).reshape(-1, 1)
            
            y = df[target_column].values
            
            # Обучаем линейную регрессию
            model = LinearRegression()
            model.fit(X, y)
            
            # Прогноз на будущие периоды
            future_X = np.array(range(len(df), len(df) + periods)).reshape(-1, 1)
            predictions = model.predict(future_X)
            
            result = {
                'current_trend': 'возрастающий' if model.coef_[0] > 0 else 'убывающий',
                'trend_strength': abs(model.coef_[0]),
                'predictions': predictions.tolist(),
                'r_squared': model.score(X, y)
            }
            
            return result
            
        except Exception as e:
            print(f"Ошибка прогнозирования тренда: {str(e)}")
            return None
    
    def predict_category(self, df, target_column, feature_columns):
        """Прогнозирование категории"""
        
        if df.empty or len(df) < 20:
            return None
        
        try:
            # Подготовка данных
            data = df[feature_columns + [target_column]].copy()
            
            # Кодирование категориальных переменных
            encoders = {}
            for col in feature_columns:
                if data[col].dtype == 'object':
                    encoder = LabelEncoder()
                    data[col] = encoder.fit_transform(data[col].astype(str))
                    encoders[col] = encoder
            
            # Разделение на обучающую и тестовую выборки
            X = data[feature_columns]
            y = data[target_column]
            
            # Если целевая переменная категориальная, кодируем ее
            if y.dtype == 'object':
                y_encoder = LabelEncoder()
                y = y_encoder.fit_transform(y)
            
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
            
            # Обучение модели
            model = RandomForestRegressor(n_estimators=100, random_state=42)
            model.fit(X_train, y_train)
            
            # Важность признаков
            feature_importance = dict(zip(feature_columns, model.feature_importances_))
            
            result = {
                'accuracy': model.score(X_test, y_test),
                'feature_importance': feature_importance,
                'model_type': 'Random Forest'
            }
            
            return result
            
        except Exception as e:
            print(f"Ошибка прогнозирования категории: {str(e)}")
            return None
    
    def detect_seasonality(self, df, value_column, date_column):
        """Обнаружение сезонности во временных рядах"""
        
        if df.empty or len(df) < 50:
            return None
        
        try:
            df = df.sort_values(date_column)
            
            # Преобразуем даты в числовой формат (дни)
            df['days'] = (df[date_column] - df[date_column].min()).dt.days
            
            # Используем FFT для обнаружения периодичности
            values = df[value_column].values
            n = len(values)
            
            # Быстрое преобразование Фурье
            fft_values = np.fft.fft(values)
            frequencies = np.fft.fftfreq(n)
            
            # Находим доминирующие частоты
            amplitudes = np.abs(fft_values)
            dominant_freq_idx = np.argsort(amplitudes)[-3:]  # Топ 3 частоты
            
            result = {
                'has_seasonality': len(dominant_freq_idx) > 0,
                'dominant_periods': []
            }
            
            for idx in dominant_freq_idx:
                if frequencies[idx] != 0:
                    period = 1 / abs(frequencies[idx])
                    result['dominant_periods'].append({
                        'period_days': period,
                        'amplitude': amplitudes[idx]
                    })
            
            return result
            
        except Exception as e:
            print(f"Ошибка обнаружения сезонности: {str(e)}")
            return None