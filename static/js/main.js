// Основной JavaScript файл для главной страницы

let currentConversation = [];
let currentVisualizationData = null;

// Установка примера запроса
function setExample(exampleCard) {
    const exampleText = exampleCard.querySelector('p').textContent;
    document.getElementById('userInput').value = exampleText.trim();
    document.getElementById('userInput').focus();
}

// Отправка сообщения
async function sendMessage() {
    const userInput = document.getElementById('userInput');
    const query = userInput.value.trim();
    
    if (!query) {
        alert('Пожалуйста, введите запрос');
        return;
    }
    
    // Добавляем сообщение пользователя в историю чата
    addMessageToChat(query, 'user');
    
    // Очищаем поле ввода
    userInput.value = '';
    
    // Показываем индикатор загрузки
    showLoading();
    
    try {
        // Отправляем запрос на сервер
        const response = await fetch('/api/chat', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                query: query,
                history: currentConversation
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            // Добавляем ответ AI в историю чата
            addMessageToChat(data.text_analysis || 'Данные успешно получены', 'ai');
            
            // Сохраняем данные для визуализации
            currentVisualizationData = data;
            
            // Обновляем все вкладки
            updateVisualizationTab(data);
            updateDataTab(data);
            updateAnalysisTab(data);
            updateSQLTab(data);
            
            // Добавляем в историю диалога
            currentConversation.push({
                user: query,
                response: data.text_analysis
            });
            
        } else {
            throw new Error(data.error || 'Произошла ошибка');
        }
        
    } catch (error) {
        console.error('Ошибка:', error);
        addMessageToChat(`❌ Ошибка: ${error.message}`, 'ai');
    } finally {
        hideLoading();
    }
}

// Добавление сообщения в чат
function addMessageToChat(message, sender) {
    const chatHistory = document.getElementById('chatHistory');
    const messageDiv = document.createElement('div');
    messageDiv.className = `chat-message ${sender}-message`;
    messageDiv.innerHTML = `<p>${message}</p>`;
    chatHistory.appendChild(messageDiv);
    
    // Прокручиваем вниз
    chatHistory.scrollTop = chatHistory.scrollHeight;
    
    // Убираем welcome сообщение если оно есть
    const welcomeMessage = chatHistory.querySelector('.welcome-message');
    if (welcomeMessage) {
        welcomeMessage.remove();
    }
}

// Обновление вкладки визуализации
// Обновление вкладки визуализации
function updateVisualizationTab(data) {
    const container = document.getElementById('visualizationContainer');
    
    if (!data.visualization) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">📊</div>
                <h4>Нет данных для визуализации</h4>
                <p>Запрос не вернул данных или визуализация недоступна</p>
            </div>
        `;
        return;
    }
    
    try {
        // Пробуем распарсить визуализацию
        const vizData = typeof data.visualization === 'string' 
            ? JSON.parse(data.visualization) 
            : data.visualization;
        
        // Проверяем, что это данные для Plotly
        if (vizData.data || vizData.layout) {
            // Очищаем контейнер
            container.innerHTML = '';
            
            // Создаем контейнер для графика
            const plotDiv = document.createElement('div');
            plotDiv.className = 'plotly-chart';
            plotDiv.style.width = '100%';
            plotDiv.style.height = '500px';
            container.appendChild(plotDiv);
            
            // Отображаем график
            Plotly.newPlot(plotDiv, vizData.data || [], vizData.layout || {}, {
                responsive: true,
                displayModeBar: true,
                displaylogo: false,
                modeBarButtonsToRemove: ['sendDataToCloud', 'lasso2d', 'select2d']
            });
            
            // Добавляем информацию о данных
            const infoDiv = document.createElement('div');
            infoDiv.className = 'viz-info';
            infoDiv.innerHTML = `
                <div class="viz-stats">
                    <span class="viz-stat">
                        📊 <strong>${data.row_count || 0}</strong> записей
                    </span>
                    <span class="viz-stat">
                        📋 <strong>${data.columns?.length || 0}</strong> показателей
                    </span>
                    <button class="btn btn-small" onclick="downloadChart()">
                        📥 Скачать график
                    </button>
                </div>
            `;
            container.appendChild(infoDiv);
            
        } else if (vizData.error) {
            // Если есть ошибка
            container.innerHTML = `
                <div class="empty-state">
                    <div class="empty-icon">⚠️</div>
                    <h4>Ошибка визуализации</h4>
                    <p>${vizData.message || 'Не удалось создать визуализацию'}</p>
                    <button class="btn btn-outline" onclick="showDataAsTable()">
                        Показать как таблицу
                    </button>
                </div>
            `;
        } else {
            throw new Error('Неправильный формат данных визуализации');
        }
        
    } catch (error) {
        console.error('Ошибка отображения визуализации:', error);
        
        // Показываем данные как таблицу
        container.innerHTML = `
            <div class="error-state">
                <h4>⚠️ Ошибка отображения графика</h4>
                <p>Показаны данные в виде таблицы:</p>
                ${createSimpleTable(data.data, data.columns)}
                <button class="btn btn-outline" onclick="tryAgainViz()">
                    Попробовать другой тип графика
                </button>
            </div>
        `;
    }
}

// Вспомогательная функция для создания простой таблицы
function createSimpleTable(data, columns) {
    if (!data || data.length === 0) {
        return '<p>Нет данных</p>';
    }
    
    let tableHTML = `
        <div class="simple-table-container">
            <table class="simple-table">
                <thead>
                    <tr>
                        ${columns.map(col => `<th>${col}</th>`).join('')}
                    </tr>
                </thead>
                <tbody>
    `;
    
    data.slice(0, 10).forEach(row => {
        tableHTML += '<tr>';
        columns.forEach(col => {
            const val = row[col];
            tableHTML += `<td>${val !== null && val !== undefined ? val : ''}</td>`;
        });
        tableHTML += '</tr>';
    });
    
    tableHTML += `
                </tbody>
            </table>
            ${data.length > 10 ? `<p class="table-note">Показано 10 из ${data.length} записей</p>` : ''}
        </div>
    `;
    
    return tableHTML;
}

// Функции для кнопок
function downloadChart() {
    const plotDiv = document.querySelector('.plotly-chart');
    if (plotDiv) {
        Plotly.downloadImage(plotDiv, {
            format: 'png',
            filename: 'rosatom_chart',
            width: 1200,
            height: 800
        });
    }
}

function showDataAsTable() {
    showTab('data');
}

function tryAgainViz() {
    const currentData = window.currentVisualizationData;
    if (currentData) {
        // Показываем модальное окно для выбора типа визуализации
        showVisualizationOptions();
    }
}

// Обновление вкладки данных
function updateDataTab(data) {
    if (data.data && data.data.length > 0) {
        const columns = data.columns || Object.keys(data.data[0]);
        const rows = data.data;
        
        let tableHTML = `
            <div class="table-responsive">
                <table class="data-table">
                    <thead>
                        <tr>
                            ${columns.map(col => `<th>${col}</th>`).join('')}
                        </tr>
                    </thead>
                    <tbody>
        `;
        
        rows.forEach(row => {
            tableHTML += '<tr>';
            columns.forEach(col => {
                tableHTML += `<td>${row[col] || ''}</td>`;
            });
            tableHTML += '</tr>';
        });
        
        tableHTML += `
                    </tbody>
                </table>
            </div>
            <div class="table-info">
                <p>Найдено записей: ${data.row_count || rows.length}</p>
            </div>
        `;
        
        document.getElementById('dataContainer').innerHTML = tableHTML;
    } else {
        document.getElementById('dataContainer').innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">📋</div>
                <h4>Нет данных для отображения</h4>
                <p>Попробуйте другой запрос</p>
            </div>
        `;
    }
}

// Обновление вкладки анализа
// В функции обработки ответа добавьте:
// Обновление вкладки анализа
function updateAnalysisTab(data) {
    if (data.text_analysis) {
        const analysisContainer = document.getElementById('analysisContainer');
        
        // Создаем красивый контейнер для анализа
        let analysisHTML = `
            <div class="analysis-result">
                <div class="analysis-content markdown-content">
        `;
        
        // Форматируем анализ с поддержкой markdown-like разметки
        const formattedAnalysis = this.formatMarkdown(data.text_analysis);
        
        analysisHTML += formattedAnalysis;
        analysisHTML += `
                </div>
            </div>
        `;
        
        analysisContainer.innerHTML = analysisHTML;
    } else {
        document.getElementById('analysisContainer').innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">📝</div>
                <h4>Анализ временно недоступен</h4>
                <p>Данные успешно загружены. Попробуйте использовать визуализацию.</p>
            </div>
        `;
    }
}

// Функция форматирования markdown
function formatMarkdown(text) {
    if (!text) return '';
    
    let formatted = text;
    
    // Заголовки
    formatted = formatted.replace(/^## (.*$)/gim, '<h2 class="analysis-h2">$1</h2>');
    formatted = formatted.replace(/^### (.*$)/gim, '<h3 class="analysis-h3">$1</h3>');
    
    // Жирный текст
    formatted = formatted.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
    
    // Курсив
    formatted = formatted.replace(/\*(.*?)\*/g, '<em>$1</em>');
    
    // Списки
    formatted = formatted.replace(/^\d+\. (.*$)/gim, '<li class="numbered">$1</li>');
    formatted = formatted.replace(/^[•▪] (.*$)/gim, '<li class="bulleted">$1</li>');
    
    // Wrap списки в ul/ol
    formatted = formatted.replace(/(<li class="numbered">.*<\/li>)/gim, '<ol class="analysis-list">$1</ol>');
    formatted = formatted.replace(/(<li class="bulleted">.*<\/li>)/gim, '<ul class="analysis-list">$1</ul>');
    
    // Переносы строк
    formatted = formatted.replace(/\n\n/g, '</p><p class="analysis-paragraph">');
    formatted = formatted.replace(/\n/g, '<br>');
    
    // Эмодзи
    const emojiMap = {
        '📊': '📊', '💰': '💰', '👥': '👥', '🎯': '🎯', '💡': '💡',
        '🚀': '🚀', '⚠️': '⚠️', '✅': '✅', '❌': '❌', '📈': '📈',
        '🏷️': '🏷️', '👁️': '👁️', '📭': '📭', '🧠': '🧠', '🎨': '🎨'
    };
    
    Object.keys(emojiMap).forEach(emoji => {
        formatted = formatted.replace(new RegExp(emoji, 'g'), 
            `<span class="emoji" title="${emojiMap[emoji]}">${emoji}</span>`);
    });
    
    // Оборачиваем в параграф если нет тегов
    if (!formatted.includes('<h') && !formatted.includes('<li')) {
        formatted = `<p class="analysis-paragraph">${formatted}</p>`;
    }
    
    return formatted;
}

// Обновление вкладки SQL
function updateSQLTab(data) {
    if (data.sql_query) {
        document.getElementById('sqlContainer').innerHTML = `
            <div class="sql-content">
                <pre><code class="sql">${data.sql_query}</code></pre>
                <button class="btn btn-small" onclick="copySQL()">Копировать SQL</button>
                <button class="btn btn-small btn-outline" onclick="executeCustomSQL()">Выполнить этот запрос</button>
            </div>
        `;
    } else {
        document.getElementById('sqlContainer').innerHTML = `
            <div class="empty-state">
                <div class="empty-icon">💻</div>
                <h4>SQL запрос не сгенерирован</h4>
                <p>Попробуйте другой запрос</p>
            </div>
        `;
    }
}

// Показ вкладок
function showTab(tabName) {
    // Скрываем все вкладки
    document.querySelectorAll('.tab-content').forEach(tab => {
        tab.classList.remove('active');
    });
    
    // Убираем активный класс со всех кнопок
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.classList.remove('active');
    });
    
    // Показываем выбранную вкладку
    document.getElementById(tabName + 'Tab').classList.add('active');
    
    // Активируем соответствующую кнопку
    event.target.classList.add('active');
}

// Очистка чата
function clearChat() {
    if (confirm('Очистить всю историю чата?')) {
        document.getElementById('chatHistory').innerHTML = `
            <div class="welcome-message">
                <p>👋 Привет! Я ваш AI-ассистент по анализу данных. Задайте вопрос о ваших данных, и я помогу вам получить нужную информацию.</p>
            </div>
        `;
        currentConversation = [];
        currentVisualizationData = null;
        
        // Очищаем все вкладки
        ['visualization', 'data', 'analysis', 'sql'].forEach(tab => {
            document.getElementById(tab + 'Container').innerHTML = `
                <div class="empty-state">
                    <div class="empty-icon">📊</div>
                    <h4>Здесь появятся данные</h4>
                    <p>Задайте вопрос, чтобы увидеть результаты</p>
                </div>
            `;
        });
    }
}

// Модальное окно для выбора визуализации
function showVisualizationOptions() {
    document.getElementById('vizModal').style.display = 'flex';
}

function closeModal() {
    document.getElementById('vizModal').style.display = 'none';
}

function selectVizType(vizType) {
    closeModal();
    
    if (currentVisualizationData) {
        // Перестраиваем визуализацию с выбранным типом
        fetch('/api/visualize', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                chart_type: vizType,
                data: currentVisualizationData.data,
                config: {
                    title: 'Визуализация данных'
                }
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                try {
                    const plotData = JSON.parse(data.visualization);
                    Plotly.newPlot('visualizationContainer', plotData.data || [], plotData.layout || {});
                } catch (e) {
                    console.error('Ошибка отображения визуализации:', e);
                }
            }
        });
    }
}

// Генерация отчета
async function generateReport() {
    try {
        const response = await fetch('/api/generate_report', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                report_type: 'summary',
                filters: {}
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            addMessageToChat('📄 Отчет успешно сгенерирован!', 'ai');
            // Здесь можно добавить логику для отображения отчета
        }
    } catch (error) {
        console.error('Ошибка генерации отчета:', error);
    }
}

// Показать схему БД
async function showSchema() {
    try {
        const response = await fetch('/api/schema');
        const data = await response.json();
        
        if (data.success) {
            const schemaInfo = JSON.stringify(data.schema, null, 2);
            addMessageToChat(`🗃️ Схема базы данных:\n\`\`\`json\n${schemaInfo}\n\`\`\``, 'ai');
        }
    } catch (error) {
        console.error('Ошибка получения схемы:', error);
    }
}

// Копирование SQL запроса
function copySQL() {
    const sqlQuery = currentVisualizationData?.sql_query;
    if (sqlQuery) {
        navigator.clipboard.writeText(sqlQuery)
            .then(() => alert('SQL запрос скопирован в буфер обмена'))
            .catch(err => console.error('Ошибка копирования:', err));
    }
}

// Выполнение кастомного SQL
async function executeCustomSQL() {
    const sqlQuery = currentVisualizationData?.sql_query;
    if (!sqlQuery) return;
    
    try {
        const response = await fetch('/api/execute_sql', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                sql: sqlQuery
            })
        });
        
        const data = await response.json();
        
        if (data.success) {
            addMessageToChat('✅ SQL запрос успешно выполнен', 'ai');
            updateDataTab(data);
        }
    } catch (error) {
        console.error('Ошибка выполнения SQL:', error);
    }
}

// Обработка нажатия Enter в поле ввода
function handleKeyPress(event) {
    if (event.key === 'Enter' && !event.shiftKey) {
        event.preventDefault();
        sendMessage();
    }
}

// Показать индикатор загрузки
function showLoading() {
    const chatHistory = document.getElementById('chatHistory');
    const loadingDiv = document.createElement('div');
    loadingDiv.className = 'chat-message ai-message';
    loadingDiv.id = 'loadingMessage';
    loadingDiv.innerHTML = '<p>🤔 Думаю...</p>';
    chatHistory.appendChild(loadingDiv);
    chatHistory.scrollTop = chatHistory.scrollHeight;
}

// Скрыть индикатор загрузки
function hideLoading() {
    const loadingMessage = document.getElementById('loadingMessage');
    if (loadingMessage) {
        loadingMessage.remove();
    }
}

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    // Фокус на поле ввода
    document.getElementById('userInput').focus();
    
    // Загружаем историю диалога если есть
    fetch('/api/conversation_history')
        .then(response => response.json())
        .then(data => {
            if (data.success && data.history.length > 0) {
                currentConversation = data.history;
                // Можно восстановить историю чата если нужно
            }
        });
});