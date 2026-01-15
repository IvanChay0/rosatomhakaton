// Dashboard JavaScript - Rosatom BI System
document.addEventListener('DOMContentLoaded', function() {
    console.log('Dashboard initialized');
    loadDashboardData();
    setupEventListeners();
});

function setupEventListeners() {
    // Фильтры
    document.getElementById('departmentFilter').addEventListener('change', applyFilters);
    document.getElementById('dateFilter').addEventListener('change', applyFilters);
    document.getElementById('projectFilter').addEventListener('change', applyFilters);
    
    // Кнопки обновления
    document.querySelector('button[onclick="refreshSafetyIncidents()"]').addEventListener('click', refreshSafetyIncidents);
    document.querySelector('button[onclick="refreshTopEmployees()"]').addEventListener('click', refreshTopEmployees);
    document.querySelector('button[onclick="generateAIInsights()"]').addEventListener('click', generateAIInsights);
}

function loadDashboardData() {
    console.log('Loading dashboard data...');
    
    // Загружаем все данные параллельно
    Promise.all([
        loadKPIMetrics(),
        loadDepartmentChart(),
        loadSalesChart(),
        loadProjectStatusChart(),
        loadTopProductsChart(),
        loadSafetyIncidents(),
        loadTopEmployees(),
        loadAIInsights()
    ]).then(() => {
        console.log('All dashboard data loaded successfully');
    }).catch(error => {
        console.error('Error loading dashboard data:', error);
        showError('Ошибка загрузки данных дашборда');
    });
}

function loadKPIMetrics() {
    return new Promise((resolve, reject) => {
        fetch('/api/execute_sql', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({
                sql: "SELECT COUNT(*) as total_employees FROM employees"
            })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                document.getElementById('totalEmployees').textContent = 
                    data.data[0]?.total_employees || 0;
            }
            return fetch('/api/execute_sql', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    sql: "SELECT COUNT(*) as active_projects FROM projects WHERE status = 'В работе'"
                })
            });
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                document.getElementById('activeProjects').textContent = 
                    data.data[0]?.active_projects || 0;
            }
            return fetch('/api/execute_sql', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    sql: "SELECT SUM(revenue) as total_revenue FROM production WHERE revenue IS NOT NULL"
                })
            });
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                const revenue = data.data[0]?.total_revenue || 0;
                document.getElementById('totalRevenue').textContent = 
                    formatCurrency(revenue);
            }
            return fetch('/api/execute_sql', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    sql: "SELECT (COUNT(CASE WHEN severity = 'Низкий' THEN 1 END) * 100.0 / COUNT(*)) as safety_score FROM safety_incidents"
                })
            });
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                const score = data.data[0]?.safety_score || 100;
                document.getElementById('safetyScore').textContent = 
                    Math.round(score) + '%';
            }
            resolve();
        })
        .catch(error => {
            console.error('Error loading KPI metrics:', error);
            reject(error);
        });
    });
}

function loadDepartmentChart() {
    fetch('/api/execute_sql', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            sql: "SELECT department, COUNT(*) as employee_count FROM employees GROUP BY department ORDER BY employee_count DESC LIMIT 10"
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success && data.data.length > 0) {
            createDepartmentChart(data.data);
        } else {
            document.getElementById('departmentChart').innerHTML = 
                '<div class="no-data">Нет данных для отображения</div>';
        }
    })
    .catch(error => {
        console.error('Error loading department chart:', error);
        document.getElementById('departmentChart').innerHTML = 
            '<div class="error">Ошибка загрузки графика</div>';
    });
}

function createDepartmentChart(data) {
    const departments = data.map(item => item.department);
    const counts = data.map(item => item.employee_count);
    
    const chartData = [{
        x: departments,
        y: counts,
        type: 'bar',
        marker: {
            color: '#667eea'
        },
        hovertemplate: '<b>%{x}</b><br>Сотрудников: %{y}<extra></extra>'
    }];
    
    const layout = {
        title: {
            text: 'Распределение сотрудников по отделам',
            font: {
                size: 14,
                color: '#2d3748'
            }
        },
        xaxis: {
            title: 'Отдел',
            tickangle: 45
        },
        yaxis: {
            title: 'Количество сотрудников'
        },
        height: 300,
        margin: {
            l: 60,
            r: 30,
            t: 60,
            b: 100
        },
        paper_bgcolor: 'white',
        plot_bgcolor: 'white'
    };
    
    Plotly.newPlot('departmentChart', chartData, layout, {
        responsive: true,
        displayModeBar: false
    });
}

function loadSalesChart() {
    fetch('/api/execute_sql', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            sql: `
                SELECT 
                    substr(date, 1, 7) as month,
                    SUM(revenue) as total_revenue
                FROM production 
                WHERE date IS NOT NULL AND revenue IS NOT NULL
                GROUP BY substr(date, 1, 7)
                ORDER BY month DESC
                LIMIT 12
            `
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success && data.data.length > 0) {
            createSalesChart(data.data.reverse());
        } else {
            document.getElementById('salesChart').innerHTML = 
                '<div class="no-data">Нет данных для отображения</div>';
        }
    })
    .catch(error => {
        console.error('Error loading sales chart:', error);
        document.getElementById('salesChart').innerHTML = 
            '<div class="error">Ошибка загрузки графика</div>';
    });
}

function createSalesChart(data) {
    const months = data.map(item => item.month);
    const revenues = data.map(item => item.total_revenue || 0);
    
    const chartData = [{
        x: months,
        y: revenues,
        type: 'scatter',
        mode: 'lines+markers',
        line: {
            color: '#48bb78',
            width: 3
        },
        marker: {
            color: '#38a169',
            size: 8
        },
        name: 'Выручка',
        hovertemplate: '%{x}<br>Выручка: %{y:,.0f} ₽<extra></extra>'
    }];
    
    const layout = {
        title: {
            text: 'Динамика продаж по месяцам',
            font: {
                size: 14,
                color: '#2d3748'
            }
        },
        xaxis: {
            title: 'Месяц'
        },
        yaxis: {
            title: 'Выручка (₽)',
            tickformat: ',.0f'
        },
        height: 300,
        margin: {
            l: 60,
            r: 30,
            t: 60,
            b: 80
        },
        paper_bgcolor: 'white',
        plot_bgcolor: 'white',
        hovermode: 'x unified'
    };
    
    Plotly.newPlot('salesChart', chartData, layout, {
        responsive: true,
        displayModeBar: false
    });
}

function loadProjectStatusChart() {
    fetch('/api/execute_sql', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            sql: "SELECT status, COUNT(*) as count FROM projects GROUP BY status"
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success && data.data.length > 0) {
            createProjectStatusChart(data.data);
        } else {
            document.getElementById('projectStatusChart').innerHTML = 
                '<div class="no-data">Нет данных для отображения</div>';
        }
    })
    .catch(error => {
        console.error('Error loading project status chart:', error);
        document.getElementById('projectStatusChart').innerHTML = 
            '<div class="error">Ошибка загрузки графика</div>';
    });
}

function createProjectStatusChart(data) {
    const labels = data.map(item => item.status);
    const values = data.map(item => item.count);
    
    const chartData = [{
        labels: labels,
        values: values,
        type: 'pie',
        hole: 0.4,
        marker: {
            colors: ['#667eea', '#ed8936', '#48bb78', '#f56565', '#9f7aea']
        },
        textinfo: 'percent+label',
        hovertemplate: '<b>%{label}</b><br>Количество: %{value}<br>Доля: %{percent}<extra></extra>'
    }];
    
    const layout = {
        title: {
            text: 'Статус проектов',
            font: {
                size: 14,
                color: '#2d3748'
            }
        },
        height: 300,
        margin: {
            t: 60,
            b: 30,
            l: 30,
            r: 30
        },
        paper_bgcolor: 'white',
        showlegend: true,
        legend: {
            orientation: 'h',
            y: -0.2
        }
    };
    
    Plotly.newPlot('projectStatusChart', chartData, layout, {
        responsive: true,
        displayModeBar: false
    });
}

function loadTopProductsChart() {
    fetch('/api/execute_sql', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            sql: `
                SELECT 
                    product_name,
                    SUM(revenue) as total_revenue
                FROM production 
                WHERE revenue IS NOT NULL
                GROUP BY product_name 
                ORDER BY total_revenue DESC 
                LIMIT 5
            `
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success && data.data.length > 0) {
            createTopProductsChart(data.data);
        } else {
            document.getElementById('topProductsChart').innerHTML = 
                '<div class="no-data">Нет данных для отображения</div>';
        }
    })
    .catch(error => {
        console.error('Error loading top products chart:', error);
        document.getElementById('topProductsChart').innerHTML = 
            '<div class="error">Ошибка загрузки графика</div>';
    });
}

function createTopProductsChart(data) {
    const products = data.map(item => {
        const name = item.product_name || 'Неизвестный товар';
        return name.length > 20 ? name.substring(0, 20) + '...' : name;
    });
    const revenues = data.map(item => item.total_revenue || 0);
    
    const chartData = [{
        x: products,
        y: revenues,
        type: 'bar',
        marker: {
            color: '#ed8936'
        },
        hovertemplate: '<b>%{x}</b><br>Выручка: %{y:,.0f} ₽<extra></extra>'
    }];
    
    const layout = {
        title: {
            text: 'Топ-5 товаров по продажам',
            font: {
                size: 14,
                color: '#2d3748'
            }
        },
        xaxis: {
            title: 'Товар',
            tickangle: 45
        },
        yaxis: {
            title: 'Выручка (₽)',
            tickformat: ',.0f'
        },
        height: 300,
        margin: {
            l: 60,
            r: 30,
            t: 60,
            b: 120
        },
        paper_bgcolor: 'white',
        plot_bgcolor: 'white'
    };
    
    Plotly.newPlot('topProductsChart', chartData, layout, {
        responsive: true,
        displayModeBar: false
    });
}

function loadSafetyIncidents() {
    fetch('/api/execute_sql', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            sql: `
                SELECT 
                    date,
                    description,
                    severity,
                    department,
                    resolved
                FROM safety_incidents 
                ORDER BY date DESC 
                LIMIT 10
            `
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            createSafetyTable(data.data);
        } else {
            document.getElementById('safetyTable').innerHTML = 
                '<div class="no-data">Нет данных об инцидентах</div>';
        }
    })
    .catch(error => {
        console.error('Error loading safety incidents:', error);
        document.getElementById('safetyTable').innerHTML = 
            '<div class="error">Ошибка загрузки данных</div>';
    });
}

function createSafetyTable(data) {
    if (!data || data.length === 0) {
        document.getElementById('safetyTable').innerHTML = 
            '<div class="no-data">Нет данных об инцидентах</div>';
        return;
    }
    
    let html = `
        <table class="data-table">
            <thead>
                <tr>
                    <th>Дата</th>
                    <th>Описание</th>
                    <th>Уровень</th>
                    <th>Отдел</th>
                    <th>Статус</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    data.forEach(incident => {
        const severityClass = getSeverityClass(incident.severity);
        const statusText = incident.resolved ? 'Решен' : 'В работе';
        const statusClass = incident.resolved ? 'status-solved' : 'status-pending';
        
        // Обрезаем длинное описание
        const description = incident.description || '';
        const shortDescription = description.length > 50 ? 
            description.substring(0, 50) + '...' : description;
        
        html += `
            <tr>
                <td>${incident.date || '—'}</td>
                <td title="${description}">${shortDescription}</td>
                <td><span class="severity-badge ${severityClass}">${incident.severity || '—'}</span></td>
                <td>${incident.department || '—'}</td>
                <td><span class="status-badge ${statusClass}">${statusText}</span></td>
            </tr>
        `;
    });
    
    html += `
            </tbody>
        </table>
        <div class="table-footer">
            <small>Показано ${data.length} последних инцидентов</small>
        </div>
    `;
    
    document.getElementById('safetyTable').innerHTML = html;
}

function getSeverityClass(severity) {
    switch(severity?.toLowerCase()) {
        case 'критический': return 'severity-critical';
        case 'высокий': return 'severity-high';
        case 'средний': return 'severity-medium';
        case 'низкий': return 'severity-low';
        default: return 'severity-unknown';
    }
}

function loadTopEmployees() {
    fetch('/api/execute_sql', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            sql: `
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
            `
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            createEmployeesTable(data.data);
        } else {
            document.getElementById('employeesTable').innerHTML = 
                '<div class="no-data">Нет данных о сотрудниках</div>';
        }
    })
    .catch(error => {
        console.error('Error loading top employees:', error);
        document.getElementById('employeesTable').innerHTML = 
            '<div class="error">Ошибка загрузки данных</div>';
    });
}

function createEmployeesTable(data) {
    if (!data || data.length === 0) {
        document.getElementById('employeesTable').innerHTML = 
            '<div class="no-data">Нет данных о сотрудниках</div>';
        return;
    }
    
    let html = `
        <table class="data-table">
            <thead>
                <tr>
                    <th>Сотрудник</th>
                    <th>Отдел</th>
                    <th>Должность</th>
                    <th>Оценка</th>
                    <th>Зарплата</th>
                </tr>
            </thead>
            <tbody>
    `;
    
    data.forEach(employee => {
        const score = employee.performance_score || 0;
        const scoreClass = score >= 90 ? 'score-excellent' : 
                          score >= 75 ? 'score-good' : 
                          score >= 60 ? 'score-average' : 'score-poor';
        
        html += `
            <tr>
                <td>${employee.full_name || '—'}</td>
                <td>${employee.department || '—'}</td>
                <td>${employee.position || '—'}</td>
                <td><span class="score-badge ${scoreClass}">${score}</span></td>
                <td>${formatCurrency(employee.salary || 0)}</td>
            </tr>
        `;
    });
    
    html += `
            </tbody>
        </table>
        <div class="table-footer">
            <small>Топ-${data.length} сотрудников по эффективности</small>
        </div>
    `;
    
    document.getElementById('employeesTable').innerHTML = html;
}

function loadAIInsights() {
    fetch('/api/chat', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            query: "Проанализируй текущие данные компании и дай 3 ключевых инсайта"
        })
    })
    .then(response => response.json())
    .then(data => {
        if (data.success && data.text_analysis) {
            displayAIInsights(data.text_analysis);
        } else {
            document.getElementById('aiInsights').innerHTML = 
                '<p>Не удалось сгенерировать аналитику</p>';
        }
    })
    .catch(error => {
        console.error('Error loading AI insights:', error);
        document.getElementById('aiInsights').innerHTML = 
            '<p>Ошибка загрузки AI аналитики</p>';
    });
}

function displayAIInsights(analysis) {
    // Упрощаем анализ для отображения в дашборде
    const insights = extractInsightsFromAnalysis(analysis);
    
    let html = '<div class="insights-list">';
    
    insights.forEach((insight, index) => {
        html += `
            <div class="insight-item">
                <div class="insight-icon">💡</div>
                <div class="insight-text">${insight}</div>
            </div>
        `;
    });
    
    html += '</div>';
    document.getElementById('aiInsights').innerHTML = html;
}

function extractInsightsFromAnalysis(analysis) {
    // Простая логика для извлечения инсайтов из текста анализа
    const lines = analysis.split('\n').filter(line => 
        line.trim() && 
        !line.startsWith('#') && 
        !line.startsWith('**') &&
        line.length > 20
    );
    
    // Берем первые 3 значимые строки
    return lines.slice(0, 3).map(line => 
        line.replace(/^\d+\.\s*/, '').trim()
    );
}

// Функции для кнопок
function applyFilters() {
    const department = document.getElementById('departmentFilter').value;
    const period = document.getElementById('dateFilter').value;
    const project = document.getElementById('projectFilter').value;
    
    console.log('Applying filters:', { department, period, project });
    
    // Показываем индикатор загрузки
    showLoadingState();
    
    // Здесь можно добавить логику фильтрации данных
    // Пока просто перезагружаем все данные
    setTimeout(() => {
        loadDashboardData();
        showToast('Фильтры применены');
    }, 500);
}

function resetFilters() {
    document.getElementById('departmentFilter').value = 'all';
    document.getElementById('dateFilter').value = 'last_month';
    document.getElementById('projectFilter').value = 'all';
    
    showToast('Фильтры сброшены');
    applyFilters();
}

function refreshSafetyIncidents() {
    showToast('Обновление данных об инцидентах...');
    loadSafetyIncidents();
}

function refreshTopEmployees() {
    showToast('Обновление данных о сотрудниках...');
    loadTopEmployees();
}

function generateAIInsights() {
    showToast('Генерация новых инсайтов...');
    loadAIInsights();
}

// Вспомогательные функции
function formatCurrency(value) {
    if (!value && value !== 0) return '—';
    
    const num = parseFloat(value);
    if (isNaN(num)) return '—';
    
    if (Math.abs(num) >= 1000000) {
        return (num / 1000000).toFixed(1) + ' млн ₽';
    } else if (Math.abs(num) >= 1000) {
        return (num / 1000).toFixed(1) + ' тыс ₽';
    } else {
        return Math.round(num).toLocaleString('ru-RU') + ' ₽';
    }
}

function showLoadingState() {
    // Можно добавить спиннеры или затемнение
    document.querySelectorAll('.chart-container, .table-container').forEach(el => {
        el.classList.add('loading');
    });
}

function hideLoadingState() {
    document.querySelectorAll('.chart-container, .table-container').forEach(el => {
        el.classList.remove('loading');
    });
}

function showToast(message) {
    // Создаем или находим контейнер для уведомлений
    let toastContainer = document.getElementById('toast-container');
    if (!toastContainer) {
        toastContainer = document.createElement('div');
        toastContainer.id = 'toast-container';
        toastContainer.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            z-index: 9999;
        `;
        document.body.appendChild(toastContainer);
    }
    
    // Создаем уведомление
    const toast = document.createElement('div');
    toast.className = 'toast-message';
    toast.textContent = message;
    toast.style.cssText = `
        background: #667eea;
        color: white;
        padding: 12px 20px;
        margin-bottom: 10px;
        border-radius: 4px;
        animation: slideIn 0.3s ease-out;
    `;
    
    toastContainer.appendChild(toast);
    
    // Удаляем через 3 секунды
    setTimeout(() => {
        toast.style.animation = 'slideOut 0.3s ease-out';
        setTimeout(() => {
            if (toast.parentNode) {
                toast.parentNode.removeChild(toast);
            }
        }, 300);
    }, 3000);
}

function showError(message) {
    const errorDiv = document.createElement('div');
    errorDiv.className = 'dashboard-error';
    errorDiv.innerHTML = `
        <div class="error-content">
            <span style="color: #f56565">⚠️</span>
            <span>${message}</span>
            <button onclick="this.parentNode.parentNode.remove()" style="margin-left: auto; background: none; border: none; color: white; cursor: pointer">×</button>
        </div>
    `;
    errorDiv.style.cssText = `
        position: fixed;
        top: 20px;
        left: 50%;
        transform: translateX(-50%);
        background: #f56565;
        color: white;
        padding: 10px 20px;
        border-radius: 4px;
        z-index: 9999;
        min-width: 300px;
        text-align: center;
    `;
    
    document.body.appendChild(errorDiv);
    
    // Удаляем через 5 секунд
    setTimeout(() => {
        if (errorDiv.parentNode) {
            errorDiv.parentNode.removeChild(errorDiv);
        }
    }, 5000);
}

// Добавляем CSS для анимаций
const style = document.createElement('style');
style.textContent = `
    @keyframes slideIn {
        from { transform: translateX(100%); opacity: 0; }
        to { transform: translateX(0); opacity: 1; }
    }
    
    @keyframes slideOut {
        from { transform: translateX(0); opacity: 1; }
        to { transform: translateX(100%); opacity: 0; }
    }
    
    .dashboard-error .error-content {
        display: flex;
        align-items: center;
        gap: 10px;
    }
    
    .severity-badge {
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 600;
        color: white;
    }
    
    .severity-critical { background: #f56565; }
    .severity-high { background: #ed8936; }
    .severity-medium { background: #ecc94b; color: #2d3748; }
    .severity-low { background: #48bb78; }
    .severity-unknown { background: #a0aec0; }
    
    .status-badge {
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 600;
    }
    
    .status-solved { background: #c6f6d5; color: #22543d; }
    .status-pending { background: #fed7d7; color: #742a2a; }
    
    .score-badge {
        display: inline-block;
        padding: 2px 8px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 600;
        color: white;
        min-width: 30px;
        text-align: center;
    }
    
    .score-excellent { background: #48bb78; }
    .score-good { background: #38b2ac; }
    .score-average { background: #ed8936; }
    .score-poor { background: #f56565; }
    
    .no-data, .error {
        text-align: center;
        padding: 40px;
        color: #a0aec0;
        font-style: italic;
    }
    
    .error {
        color: #f56565;
    }
    
    .insights-list {
        display: flex;
        flex-direction: column;
        gap: 15px;
    }
    
    .insight-item {
        display: flex;
        align-items: flex-start;
        gap: 10px;
        padding: 10px;
        background: #f7fafc;
        border-radius: 8px;
        border-left: 4px solid #667eea;
    }
    
    .insight-icon {
        font-size: 20px;
    }
    
    .insight-text {
        flex: 1;
        line-height: 1.5;
    }
`;
document.head.appendChild(style);