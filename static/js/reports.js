// JavaScript для страницы отчетов
// Добавьте глобальную переменную в начале файла
let selectedReportType = 'summary';
let currentReport = null;
let reportHistory = [];

// Инициализация при загрузке страницы
document.addEventListener('DOMContentLoaded', function() {
    loadReportHistory();
});

// Выбор типа отчета
// Обновленная функция selectReportType
function selectReportType(type) {
    selectedReportType = type;
    
    // Убираем выделение со всех карточек
    document.querySelectorAll('.report-type-card').forEach(card => {
        card.style.borderColor = '#e2e8f0';
        card.style.boxShadow = 'none';
        card.style.transform = 'none';
    });
    
    // Выделяем выбранную карточку
    const target = event.currentTarget;
    target.style.borderColor = '#667eea';
    target.style.boxShadow = '0 10px 20px rgba(102, 126, 234, 0.2)';
    target.style.transform = 'translateY(-5px)';
    
    console.log('Выбран тип отчета:', type);
}


// Обновленная функция generateReport
async function generateReport() {
    // Получаем тип отчета из глобальной переменной
    const reportType = selectedReportType;
    
    // Получаем период
    const period = document.getElementById('reportPeriod').value;
    const periodText = document.getElementById('reportPeriod').options[document.getElementById('reportPeriod').selectedIndex].text;
    
    // Получаем другие параметры
    const format = document.getElementById('reportFormat').value;
    const includeCharts = document.getElementById('includeCharts').checked;
    const includeAI = document.getElementById('includeAI').checked;
    
    // Получаем выбранные отделы
    const departmentSelect = document.getElementById('reportDepartments');
    const selectedDepartments = Array.from(departmentSelect.selectedOptions)
        .map(option => option.value)
        .filter(value => value !== 'all');
    
    // Выводим отладочную информацию
    console.log('Параметры отчета:', {
        reportType: reportType,
        period: period,
        periodText: periodText,
        format: format,
        includeCharts: includeCharts,
        includeAI: includeAI,
        departments: selectedDepartments
    });
    
    const filters = {
        report_type: reportType,
        period: period,
        period_text: periodText,
        departments: selectedDepartments,
        include_charts: includeCharts,
        include_ai: includeAI,
        format: format
    };
    
    try {
        // Показать индикатор загрузки
        showLoading();
        
        console.log('Отправляю запрос на генерацию отчета:', filters);
        
        const response = await fetch('/api/generate_report', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                report_type: reportType,
                filters: filters
            })
        });
        
        const data = await response.json();
        console.log('Ответ сервера:', data);
        
        if (data.success) {
            currentReport = data.report;
            displayReportPreview(data.report);
            
            // Добавляем в историю
            addToReportHistory({
                id: Date.now(),
                type: reportType,
                date: new Date().toISOString(),
                period: period,
                departments: selectedDepartments,
                filters: filters
            });
            
            showToast(`Отчет "${formatReportType(reportType)}" успешно сгенерирован!`);
            
        } else {
            throw new Error(data.error || 'Ошибка генерации отчета');
        }
        
    } catch (error) {
        console.error('Ошибка генерации отчета:', error);
        alert(`Ошибка: ${error.message}\n\nПроверьте консоль для подробностей.`);
    } finally {
        hideLoading();
    }
}

// Отображение предпросмотра отчета
function displayReportPreview(reportData) {
    const previewContainer = document.getElementById('reportPreview');
    
    try {
        const report = typeof reportData === 'string' ? JSON.parse(reportData) : reportData;
        
        let previewHTML = `
            <div class="report-preview-content">
                <div class="report-header">
                    <h2>${report.title || 'Отчет'}</h2>
                    <div class="report-meta">
                        <span class="meta-item">📅 ${report.date || new Date().toLocaleDateString()}</span>
                        <span class="meta-item">📊 ${report.report_type || 'Общий'}</span>
                    </div>
                </div>
        `;
        
        // Добавляем метрики если есть
        if (report.metrics) {
            previewHTML += `
                <div class="report-metrics">
                    <h3>Ключевые показатели</h3>
                    <div class="metrics-grid">
            `;
            
            for (const [key, value] of Object.entries(report.metrics)) {
                previewHTML += `
                    <div class="metric-card">
                        <div class="metric-value">${value}</div>
                        <div class="metric-label">${formatMetricLabel(key)}</div>
                    </div>
                `;
            }
            
            previewHTML += `
                    </div>
                </div>
            `;
        }
        
        // Добавляем разделы если есть
        if (report.sections) {
            previewHTML += `<div class="report-sections">`;
            
            report.sections.forEach((section, index) => {
                previewHTML += `
                    <div class="section">
                        <h3>${index + 1}. ${section.title}</h3>
                        <p>${section.content}</p>
                    </div>
                `;
            });
            
            previewHTML += `</div>`;
        }
        
        // Добавляем рекомендации если есть
        if (report.recommendations) {
            previewHTML += `
                <div class="report-recommendations">
                    <h3>Рекомендации</h3>
                    <ul>
            `;
            
            report.recommendations.forEach(rec => {
                previewHTML += `<li>${rec}</li>`;
            });
            
            previewHTML += `
                    </ul>
                </div>
            `;
        }
        
        // Добавляем инсайты если есть
        if (report.insights) {
            previewHTML += `
                <div class="report-insights">
                    <h3>AI Инсайты</h3>
                    <ul>
            `;
            
            report.insights.forEach(insight => {
                previewHTML += `<li>${insight}</li>`;
            });
            
            previewHTML += `
                    </ul>
                </div>
            `;
        }
        
        previewHTML += `</div>`;
        
        previewContainer.innerHTML = previewHTML;
        
    } catch (error) {
        previewContainer.innerHTML = `
            <div class="preview-error">
                <h4>Ошибка отображения отчета</h4>
                <p>${error.message}</p>
                <pre>${JSON.stringify(reportData, null, 2)}</pre>
            </div>
        `;
    }
}

// Запланировать отчет
function scheduleReport() {
    const reportType = document.getElementById('reportType')?.value || 'summary';
    const period = document.getElementById('reportPeriod').value;
    
    const schedule = prompt(
        'Запланировать отчет?\n\n' +
        'Введите частоту (daily, weekly, monthly):',
        'weekly'
    );
    
    if (schedule) {
        alert(`Отчет "${reportType}" запланирован на выполнение ${schedule}`);
        
        // Здесь можно добавить логику для реального планирования
        // Например, отправку на сервер для сохранения в расписании
    }
}

// Сохранение шаблона
function saveTemplate() {
    const reportType = document.getElementById('reportType')?.value || 'summary';
    
    const templateName = prompt('Введите название шаблона:', `${reportType}_template`);
    
    if (templateName) {
        // Сохраняем текущие параметры
        const template = {
            name: templateName,
            type: reportType,
            period: document.getElementById('reportPeriod').value,
            format: document.getElementById('reportFormat').value,
            includeCharts: document.getElementById('includeCharts').checked,
            includeAI: document.getElementById('includeAI').checked,
            departments: Array.from(document.getElementById('reportDepartments').selectedOptions)
                .map(opt => opt.value),
            timestamp: new Date().toISOString()
        };
        
        // Сохраняем в localStorage
        const templates = JSON.parse(localStorage.getItem('reportTemplates') || '[]');
        templates.push(template);
        localStorage.setItem('reportTemplates', JSON.stringify(templates));
        
        alert(`Шаблон "${templateName}" сохранен!`);
    }
}

// Загрузка истории отчетов
function loadReportHistory() {
    // Здесь можно загружать историю с сервера
    // Для демо используем примерные данные
    reportHistory = [
        {
            id: 1,
            type: 'summary',
            date: '2024-01-15T10:30:00',
            period: 'month',
            status: 'completed'
        },
        {
            id: 2,
            type: 'performance',
            date: '2024-01-14T14:20:00',
            period: 'quarter',
            status: 'completed'
        },
        {
            id: 3,
            type: 'financial',
            date: '2024-01-13T09:15:00',
            period: 'year',
            status: 'completed'
        },
        {
            id: 4,
            type: 'safety',
            date: '2024-01-12T16:45:00',
            period: 'month',
            status: 'completed'
        }
    ];
    
    updateReportHistoryList();
}

// Обновление списка истории
function updateReportHistoryList() {
    const reportsList = document.getElementById('reportsList');
    
    if (reportHistory.length === 0) {
        reportsList.innerHTML = `
            <div class="empty-reports">
                <p>История отчетов пуста</p>
            </div>
        `;
        return;
    }
    
    let historyHTML = '';
    
    reportHistory.forEach(report => {
        const date = new Date(report.date);
        const formattedDate = date.toLocaleDateString('ru-RU', {
            day: 'numeric',
            month: 'short',
            year: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
        
        const typeIcons = {
            'summary': '📊',
            'performance': '👥',
            'financial': '💰',
            'safety': '🛡️'
        };
        
        historyHTML += `
            <div class="report-item">
                <div class="report-info">
                    <div class="report-icon">
                        ${typeIcons[report.type] || '📄'}
                    </div>
                    <div class="report-details">
                        <h5>${formatReportType(report.type)}</h5>
                        <p class="report-date">${formattedDate}</p>
                        <p class="report-period">Период: ${formatPeriod(report.period)}</p>
                    </div>
                </div>
                <div class="report-actions">
                    <button class="btn btn-small" onclick="viewReport(${report.id})">
                        👁️ Просмотр
                    </button>
                    <button class="btn btn-small btn-outline" onclick="deleteReport(${report.id})">
                        🗑️ Удалить
                    </button>
                </div>
            </div>
        `;
    });
    
    reportsList.innerHTML = historyHTML;
}

// Добавление в историю
function addToReportHistory(report) {
    reportHistory.unshift(report);
    if (reportHistory.length > 20) {
        reportHistory = reportHistory.slice(0, 20);
    }
    updateReportHistoryList();
}

// Просмотр отчета из истории
function viewReport(reportId) {
    const report = reportHistory.find(r => r.id === reportId);
    if (report) {
        // Загружаем отчет по ID
        alert(`Загрузка отчета #${reportId}`);
        // Здесь можно добавить реальную загрузку отчета
    }
}

// Удаление отчета
function deleteReport(reportId) {
    if (confirm('Удалить этот отчет из истории?')) {
        reportHistory = reportHistory.filter(r => r.id !== reportId);
        updateReportHistoryList();
    }
}

// Поиск по отчетам
function searchReports() {
    const searchTerm = document.getElementById('searchReports').value.toLowerCase();
    
    const filteredReports = reportHistory.filter(report => {
        const type = formatReportType(report.type).toLowerCase();
        const period = formatPeriod(report.period).toLowerCase();
        const date = new Date(report.date).toLocaleDateString('ru-RU').toLowerCase();
        
        return type.includes(searchTerm) || 
               period.includes(searchTerm) || 
               date.includes(searchTerm);
    });
    
    // Временно заменяем список
    const reportsList = document.getElementById('reportsList');
    
    if (filteredReports.length === 0) {
        reportsList.innerHTML = `
            <div class="empty-reports">
                <p>Ничего не найдено</p>
            </div>
        `;
        return;
    }
    
    let historyHTML = '';
    
    filteredReports.forEach(report => {
        const date = new Date(report.date);
        const formattedDate = date.toLocaleDateString('ru-RU', {
            day: 'numeric',
            month: 'short',
            year: 'numeric'
        });
        
        historyHTML += `
            <div class="report-item">
                <div class="report-info">
                    <h5>${formatReportType(report.type)}</h5>
                    <p class="report-date">${formattedDate}</p>
                </div>
            </div>
        `;
    });
    
    reportsList.innerHTML = historyHTML;
}

// Обновление истории
function refreshHistory() {
    loadReportHistory();
}

// Экспорт отчета
// Экспорт отчета
async function exportReport() {
    if (!currentReport) {
        alert('Сначала сгенерируйте отчет');
        return;
    }
    
    const format = document.getElementById('reportFormat').value;
    const reportType = document.getElementById('reportType')?.value || 'summary';
    
    // Формируем данные для экспорта
    const exportData = prepareReportData(currentReport, reportType);
    
    switch(format) {
        case 'excel':
            await exportToExcel(exportData, reportType);
            break;
        case 'pdf':
            await exportToPDF(exportData, reportType);
            break;
        case 'json':
            await exportToJSON(exportData, reportType);
            break;
        case 'html':
        default:
            await exportToHTML(exportData, reportType);
    }
}

// Подготовка данных отчета
// Обновите функцию prepareReportData:
function prepareReportData(reportData, reportType) {
    let data;
    
    // Если отчет в строковом формате, пытаемся распарсить
    if (typeof reportData === 'string') {
        try {
            data = JSON.parse(reportData);
        } catch (e) {
            data = {
                title: `Отчет ${reportType}`,
                date: new Date().toISOString(),
                content: reportData,
                type: reportType
            };
        }
    } else {
        data = reportData;
    }
    
    // Обеспечиваем структуру данных
    if (!data.metrics) {
        data.metrics = {};
    }
    
    if (!data.data) {
        data.data = [];
    }
    
    if (!data.columns && data.data.length > 0) {
        data.columns = Object.keys(data.data[0]);
    }
    
    return data;

    
    // Извлекаем данные из preview если нужно
    const previewContainer = document.getElementById('reportPreview');
    if (previewContainer && (!data.metrics || !data.data)) {
        // Пытаемся извлечь данные из HTML preview
        const metricCards = previewContainer.querySelectorAll('.metric-card');
        if (metricCards.length > 0) {
            data.metrics = {};
            metricCards.forEach(card => {
                const label = card.querySelector('.metric-label')?.textContent || 'Показатель';
                const value = card.querySelector('.metric-value')?.textContent || '0';
                data.metrics[label] = value;
            });
        }
    }
    
    return data;
}

// Экспорт в Excel (используем CSV как временное решение)
async function exportToExcel(data, reportType) {
    try {
        // Преобразуем данные в CSV
        let csvContent = '';
        
        // Добавляем заголовок
        csvContent += `Отчет: ${reportType}\n`;
        csvContent += `Дата: ${new Date().toISOString().split('T')[0]}\n\n`;
        
        // Добавляем метрики
        if (data.metrics) {
            csvContent += 'Ключевые показатели:\n';
            Object.entries(data.metrics).forEach(([key, value]) => {
                csvContent += `${key},${value}\n`;
            });
            csvContent += '\n';
        }
        
        // Добавляем табличные данные
        if (data.data && Array.isArray(data.data)) {
            csvContent += 'Данные:\n';
            // Заголовки
            const headers = Object.keys(data.data[0]);
            csvContent += headers.join(',') + '\n';
            
            // Строки
            data.data.forEach(row => {
                const values = headers.map(header => 
                    row[header] !== undefined ? row[header] : ''
                );
                csvContent += values.join(',') + '\n';
            });
        }
        
        // Скачиваем CSV как Excel
        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = `report_${reportType}_${Date.now()}.csv`;
        link.click();
        URL.revokeObjectURL(url);
        
        showToast('Отчет экспортирован в CSV');
        
    } catch (error) {
        console.error('Ошибка экспорта в Excel:', error);
        alert('Ошибка экспорта в Excel: ' + error.message);
    }
}

// Экспорт в PDF (используем HTML для печати)
async function exportToPDF(data, reportType) {
    // Используем браузерную печать для PDF
    const printWindow = window.open('', '_blank');
    printWindow.document.write(`
        <html>
        <head>
            <title>Отчет ${reportType}</title>
            <style>
                body { font-family: Arial; padding: 20px; }
                h1 { color: #333; }
                table { width: 100%; border-collapse: collapse; }
                th, td { border: 1px solid #ddd; padding: 8px; }
                th { background-color: #f2f2f2; }
            </style>
        </head>
        <body>
            <h1>Отчет ${reportType}</h1>
            <p>Дата: ${new Date().toLocaleDateString()}</p>
    `);
    
    // Добавляем данные
    if (data.metrics) {
        printWindow.document.write('<h2>Показатели</h2><ul>');
        Object.entries(data.metrics).forEach(([key, value]) => {
            printWindow.document.write(`<li><strong>${key}:</strong> ${value}</li>`);
        });
        printWindow.document.write('</ul>');
    }
    
    printWindow.document.write('</body></html>');
    printWindow.document.close();
    printWindow.print();
}

// Экспорт в JSON
async function exportToJSON(data, reportType) {
    try {
        const response = await fetch('/api/download_report', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                report_data: data,
                report_type: reportType,
                format: 'json',
                filename: `report_${reportType}_${Date.now()}`
            })
        });
        
        if (response.ok) {
            // Создаем ссылку для скачивания
            const blob = await response.blob();
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = `report_${reportType}_${Date.now()}.json`;
            link.click();
            URL.revokeObjectURL(url);
            
            showToast('Отчет экспортирован в JSON');
        } else {
            // Fallback: создаем JSON локально
            const dataStr = JSON.stringify(data, null, 2);
            const blob = new Blob([dataStr], { type: 'application/json' });
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = `report_${reportType}_${Date.now()}.json`;
            link.click();
            URL.revokeObjectURL(url);
            
            showToast('Отчет экспортирован в JSON (локально)');
        }
        
    } catch (error) {
        console.error('Ошибка экспорта в JSON:', error);
        // Fallback на локальный экспорт
        const dataStr = JSON.stringify(data, null, 2);
        const dataUri = 'data:application/json;charset=utf-8,'+ encodeURIComponent(dataStr);
        const link = document.createElement('a');
        link.href = dataUri;
        link.download = `report_${reportType}_${Date.now()}.json`;
        link.click();
    }
}

// Экспорт в HTML
async function exportToHTML(data, reportType) {
    try {
        const response = await fetch('/api/download_report', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                report_data: data,
                report_type: reportType,
                format: 'html',
                filename: `report_${reportType}_${Date.now()}`
            })
        });
        
        if (response.ok) {
            const blob = await response.blob();
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = `report_${reportType}_${Date.now()}.html`;
            link.click();
            URL.revokeObjectURL(url);
            
            showToast('Отчет экспортирован в HTML');
        } else {
            // Fallback: создаем HTML локально
            const htmlContent = `
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Отчет ${reportType}</title>
                    <meta charset="UTF-8">
                    <style>
                        body { font-family: Arial; padding: 20px; }
                        h1 { color: #333; }
                        .metrics { margin: 20px 0; }
                        .metric { margin: 5px 0; }
                    </style>
                </head>
                <body>
                    <h1>Отчет ${reportType}</h1>
                    <p>Дата генерации: ${new Date().toLocaleString()}</p>
                    <div class="metrics">
                        ${data.metrics ? Object.entries(data.metrics).map(([k, v]) => 
                            `<div class="metric"><strong>${k}:</strong> ${v}</div>`
                        ).join('') : ''}
                    </div>
                </body>
                </html>
            `;
            
            const blob = new Blob([htmlContent], { type: 'text/html' });
            const url = URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            link.download = `report_${reportType}_${Date.now()}.html`;
            link.click();
            URL.revokeObjectURL(url);
        }
        
    } catch (error) {
        console.error('Ошибка экспорта в HTML:', error);
        alert('Ошибка экспорта: ' + error.message);
    }
}

// Вспомогательная функция для уведомлений
function showToast(message) {
    // Упрощенная версия для репортов
    const toast = document.createElement('div');
    toast.textContent = message;
    toast.style.cssText = `
        position: fixed;
        bottom: 20px;
        right: 20px;
        background: #48bb78;
        color: white;
        padding: 10px 20px;
        border-radius: 4px;
        z-index: 10000;
    `;
    document.body.appendChild(toast);
    
    setTimeout(() => {
        document.body.removeChild(toast);
    }, 3000);
}
// Индикатор загрузки
function showLoading() {
    const previewContainer = document.getElementById('reportPreview');
    previewContainer.innerHTML = `
        <div class="loading-indicator">
            <div class="spinner"></div>
            <p>Генерация отчета...</p>
        </div>
    `;
}

// Добавьте эти вспомогательные функции в reports.js:

function formatReportType(type) {
    const types = {
        'summary': 'Общий отчет',
        'performance': 'Отчет по эффективности',
        'financial': 'Финансовый отчет',
        'safety': 'Отчет по безопасности'
    };
    return types[type] || type;
}

function formatPeriod(period) {
    const periods = {
        'month': 'Месяц',
        'quarter': 'Квартал',
        'year': 'Год',
        'custom': 'Произвольный'
    };
    return periods[period] || period;
}

function formatMetricLabel(label) {
    const labels = {
        'average_performance': 'Средняя эффективность',
        'top_performers_count': 'Топ сотрудников',
        'improvement_needed_count': 'Требуют улучшения',
        'total': 'Всего',
        'resolved': 'Решено',
        'critical': 'Критичных',
        'revenue': 'Выручка',
        'expenses': 'Расходы',
        'profit': 'Прибыль',
        'budget_utilization': 'Использование бюджета',
        'Сотрудников': 'Сотрудников',
        'Активных проектов': 'Активных проектов',
        'Общая выручка': 'Общая выручка',
        'Безопасность': 'Безопасность',
        'Средняя эффективность': 'Средняя эффективность',
        'Топ сотрудников (90+)': 'Топ сотрудников (90+)',
        'Требуют улучшения (<60)': 'Требуют улучшения (<60)',
        'Общий бюджет': 'Общий бюджет',
        'Всего инцидентов': 'Всего инцидентов',
        'Решено': 'Решено',
        'Критичных': 'Критичных'
    };
    return labels[label] || label.replace(/_/g, ' ');
}

function hideLoading() {
    // Загрузка скрывается автоматически при отображении отчета
}

// Добавляем стили для индикатора загрузки
const loadingStyles = document.createElement('style');
loadingStyles.textContent = `
    .loading-indicator {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        height: 200px;
    }
    
    .spinner {
        width: 50px;
        height: 50px;
        border: 5px solid #f3f3f3;
        border-top: 5px solid #667eea;
        border-radius: 50%;
        animation: spin 1s linear infinite;
        margin-bottom: 20px;
    }
    
    @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    
    .report-preview-content {
        max-width: 800px;
        margin: 0 auto;
    }
    
    .report-header {
        text-align: center;
        margin-bottom: 40px;
        padding-bottom: 20px;
        border-bottom: 2px solid #e2e8f0;
    }
    
    .report-meta {
        display: flex;
        justify-content: center;
        gap: 20px;
        margin-top: 10px;
        color: #718096;
    }
    
    .metrics-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
        gap: 20px;
        margin: 30px 0;
    }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea, #764ba2);
        color: white;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
    
    .metric-value {
        font-size: 28px;
        font-weight: bold;
        margin-bottom: 10px;
    }
    
    .metric-label {
        font-size: 14px;
        opacity: 0.9;
    }
    
    .section {
        margin: 30px 0;
        padding: 20px;
        background: #f7fafc;
        border-radius: 10px;
    }
    
    .report-recommendations,
    .report-insights {
        margin: 30px 0;
        padding: 20px;
        background: #fff8e1;
        border-radius: 10px;
        border-left: 4px solid #ffb74d;
    }
    
    .report-recommendations h3,
    .report-insights h3 {
        color: #f57c00;
    }
    
    .preview-error {
        padding: 20px;
        background: #ffebee;
        border-radius: 10px;
        border-left: 4px solid #f44336;
    }
`;
document.head.appendChild(loadingStyles);