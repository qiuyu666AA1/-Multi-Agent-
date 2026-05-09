// Multi-Agent System - Frontend JavaScript
// Handles real-time dashboard updates, event streaming, and user interactions

const API = {
    async get(url) {
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        return resp.json();
    },

    async post(url, data) {
        const resp = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data),
        });
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        return resp.json();
    },

    // Convenience methods
    status() { return this.get('/api/status'); },
    agents() { return this.get('/api/agents'); },
    tasks(params = {}) {
        const qs = new URLSearchParams(params).toString();
        return this.get(`/api/tasks${qs ? '?' + qs : ''}`);
    },
    createTask(data) { return this.post('/api/tasks', data); },
    cancelTask(id) { return this.post(`/api/tasks/${id}/cancel`); },
    workflows() { return this.get('/api/workflows'); },
    createWorkflow(data) { return this.post('/api/workflows', data); },
    workflowProgress(id) { return this.get(`/api/workflows/${id}`); },
    events(limit = 50) { return this.get(`/api/events?limit=${limit}`); },
    kbFacts() { return this.get('/api/kb/facts'); },
    kbStats() { return this.get('/api/kb/stats'); },
};

// Event stream management
class EventStream {
    constructor() {
        this.source = null;
        this.listeners = [];
        this.reconnectDelay = 1000;
        this.maxReconnectDelay = 30000;
    }

    connect() {
        if (this.source) this.source.close();

        this.source = new EventSource('/api/events/stream');

        this.source.onmessage = (event) => {
            try {
                const msg = JSON.parse(event.data);
                this.listeners.forEach(fn => fn(msg));
            } catch (e) {
                console.error('Failed to parse SSE message:', e);
            }
        };

        this.source.onerror = () => {
            this.source.close();
            setTimeout(() => this.connect(), this.reconnectDelay);
            this.reconnectDelay = Math.min(this.reconnectDelay * 1.5, this.maxReconnectDelay);
        };

        this.source.onopen = () => {
            this.reconnectDelay = 1000; // Reset on successful connection
        };
    }

    onMessage(fn) {
        this.listeners.push(fn);
    }

    disconnect() {
        if (this.source) {
            this.source.close();
            this.source = null;
        }
    }
}

// Dashboard manager
class DashboardManager {
    constructor() {
        this.events = new EventStream();
        this.updateInterval = null;
    }

    start(intervalMs = 3000) {
        this.events.connect();
        this.events.onMessage(msg => {
            console.debug('[SSE]', msg.type, msg.sender, msg.topic);
        });
        this.updateInterval = setInterval(() => this.refresh(), intervalMs);
        this.refresh();
    }

    stop() {
        this.events.disconnect();
        if (this.updateInterval) {
            clearInterval(this.updateInterval);
            this.updateInterval = null;
        }
    }

    async refresh() {
        try {
            const status = await API.status();
            this.updateStatusDisplay(status);
        } catch (e) {
            console.error('Dashboard refresh error:', e);
        }
    }

    updateStatusDisplay(data) {
        // Update status indicator in navbar
        const sysStatus = document.getElementById('sys-status');
        const sysText = document.getElementById('sys-status-text');
        if (sysStatus && sysText && data.system) {
            sysStatus.className = 'status-dot active';
            sysText.textContent = `${data.system.agent_count} agents running`;
        }

        // Update stat counters
        this.setText('agent-count', data.system?.agent_count || '--');
        this.setText('task-total', data.tasks?.total || '--');
        this.setText('task-pending', data.tasks?.by_status?.pending || '--');
        this.setText('task-completed', data.tasks?.by_status?.completed || '--');
    }

    setText(id, value) {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    }
}

// Initialize dashboard when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new DashboardManager();
    window.dashboard.start();
});

// Workflow submission helper
async function submitWorkflow(type) {
    const configs = {
        health_check: { type: 'health_check' },
        data_analysis: { type: 'data_analysis', data_source: 'system' },
        incident_response: { type: 'incident_response', incident_type: 'error_spike', severity: 'high' },
    };

    const config = configs[type] || { type };

    try {
        const result = await API.createWorkflow(config);
        console.log('Workflow submitted:', result);
        return result;
    } catch (e) {
        console.error('Failed to submit workflow:', e);
        throw e;
    }
}
