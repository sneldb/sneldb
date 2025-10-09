(() => {
  const $ = (id) => document.getElementById(id);
  const cmd = $('command');
  const out = $('output');
  const runBtn = $('run');
  const clrBtn = $('clear');
  const status = $('status');
  const hl = $('hl');
  const emptyState = $('emptyState');
  const chartControls = $('chartControls');
  const viewControls = $('viewControls');
  const chartCanvas = $('chart');
  const dataTable = $('dataTable');
  const themeToggle = $('themeToggle');

  let currentChart = null;
  let currentData = null;
  let currentChartType = 'bar';
  let currentColorScheme = 'multicolor';

  // Theme management
  function getTheme() {
    const stored = localStorage.getItem('theme');
    if (stored) return stored;
    return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
  }

  function setTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('theme', theme);

    // Update chart colors if a chart is displayed
    if (currentChart && currentData) {
      renderChart(currentData, currentChartType);
    }
  }

  function toggleTheme() {
    const current = document.documentElement.getAttribute('data-theme') || getTheme();
    const newTheme = current === 'dark' ? 'light' : 'dark';
    setTheme(newTheme);
  }

  // Initialize theme
  setTheme(getTheme());

  function setStatus(text, type = 'ready') {
    const statusText = status.querySelector('.status-text');
    if (statusText) {
      statusText.textContent = text;
    } else {
      status.textContent = text;
    }
    status.className = 'status ' + type;
  }

  function setOutput(text) {
    if (window.hljs) {
      try {
        const json = JSON.parse(text);
        const pretty = JSON.stringify(json, null, 2)
          .replaceAll('&', '&amp;')
          .replaceAll('<', '&lt;')
          .replaceAll('>', '&gt;');
        out.innerHTML = pretty;
        hljs.highlightElement(out);
        return;
      } catch (_) {
        // fall through to plain text
      }
    }
    out.textContent = text;
  }

  function formatTimestamp(timestamp) {
    try {
      const date = new Date(timestamp * 1000);
      return date.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    } catch (e) {
      return timestamp;
    }
  }

  const colorSchemes = {
    multicolor: {
      name: 'Multicolor',
      colors: [
        '#4f46e5', '#06b6d4', '#10b981', '#f59e0b', '#ef4444',
        '#8b5cf6', '#ec4899', '#14b8a6', '#f97316', '#6366f1'
      ]
    },
    blues: {
      name: 'Blues',
      colors: [
        '#1e3a8a', '#1e40af', '#2563eb', '#3b82f6', '#60a5fa',
        '#93c5fd', '#bfdbfe', '#dbeafe', '#0ea5e9', '#0284c7'
      ]
    },
    greens: {
      name: 'Greens',
      colors: [
        '#065f46', '#047857', '#059669', '#10b981', '#34d399',
        '#6ee7b7', '#a7f3d0', '#d1fae5', '#14b8a6', '#0d9488'
      ]
    },
    purples: {
      name: 'Purples',
      colors: [
        '#581c87', '#6b21a8', '#7c3aed', '#8b5cf6', '#a78bfa',
        '#c4b5fd', '#ddd6fe', '#ede9fe', '#a855f7', '#9333ea'
      ]
    },
    warm: {
      name: 'Warm',
      colors: [
        '#991b1b', '#dc2626', '#f97316', '#f59e0b', '#eab308',
        '#fbbf24', '#fcd34d', '#fde68a', '#fb923c', '#fdba74'
      ]
    },
    earth: {
      name: 'Earth',
      colors: [
        '#78350f', '#92400e', '#b45309', '#d97706', '#f59e0b',
        '#78716c', '#a8a29e', '#d6d3d1', '#44403c', '#57534e'
      ]
    },
    ocean: {
      name: 'Ocean',
      colors: [
        '#164e63', '#0e7490', '#0891b2', '#06b6d4', '#22d3ee',
        '#67e8f9', '#a5f3fc', '#cffafe', '#155e75', '#0c4a6e'
      ]
    },
    sunset: {
      name: 'Sunset',
      colors: [
        '#7c2d12', '#9a3412', '#c2410c', '#ea580c', '#f97316',
        '#fb923c', '#fdba74', '#fed7aa', '#dc2626', '#ef4444'
      ]
    },
    forest: {
      name: 'Forest',
      colors: [
        '#14532d', '#15803d', '#16a34a', '#22c55e', '#4ade80',
        '#86efac', '#bbf7d0', '#dcfce7', '#166534', '#047857'
      ]
    },
    monochrome: {
      name: 'Monochrome',
      colors: [
        '#18181b', '#27272a', '#3f3f46', '#52525b', '#71717a',
        '#a1a1aa', '#d4d4d8', '#e4e4e7', '#3f3f46', '#52525b'
      ]
    },
    pastel: {
      name: 'Pastel',
      colors: [
        '#ddd6fe', '#fae8ff', '#fbcfe8', '#fecdd3', '#fed7aa',
        '#fef3c7', '#d9f99d', '#bbf7d0', '#a7f3d0', '#bfdbfe'
      ]
    },
    vibrant: {
      name: 'Vibrant',
      colors: [
        '#dc2626', '#ea580c', '#d97706', '#ca8a04', '#65a30d',
        '#16a34a', '#059669', '#0891b2', '#0284c7', '#2563eb'
      ]
    }
  };

  function generateColors(count, scheme = currentColorScheme) {
    const palette = colorSchemes[scheme] || colorSchemes.multicolor;
    const baseColors = palette.colors;

    if (count <= baseColors.length) {
      return baseColors.slice(0, count);
    }

    // For more colors than in palette, interpolate or repeat
    const colors = [];
    for (let i = 0; i < count; i++) {
      colors.push(baseColors[i % baseColors.length]);
    }
    return colors;
  }

  function getIsDarkMode() {
    const theme = document.documentElement.getAttribute('data-theme');
    if (theme) {
      return theme === 'dark';
    }
    return window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches;
  }

  function renderChart(data, type = 'bar') {
    if (!data || !data.columns || !data.rows) return;

    currentData = data;
    currentChartType = type;

    emptyState.style.display = 'none';
    chartControls.style.display = 'flex';
    viewControls.style.display = 'flex';

    if (currentChart) {
      currentChart.destroy();
    }

    const isDarkMode = getIsDarkMode();
    const textColor = isDarkMode ? '#f9fafb' : '#111827';
    const gridColor = isDarkMode ? '#374151' : '#e5e7eb';

    // Detect data structure: 2 columns vs 3+ columns (with grouping)
    const isGrouped = data.columns.length >= 3;

    if (isGrouped) {
      // Multi-dimensional data: bucket, group_by, value, ...
      // Example: [timestamp, plan, count] or [timestamp, category, subcategory, count]
      renderGroupedChart(data, type, isDarkMode, textColor, gridColor);
    } else {
      // Simple 2-column data: x-axis, y-axis
      renderSimpleChart(data, type, isDarkMode, textColor, gridColor);
    }
  }

  function renderSimpleChart(data, type, isDarkMode, textColor, gridColor) {
    const xColumn = data.columns[0];
    const yColumn = data.columns[1];

    const labels = data.rows.map(row => {
      const value = row[0];
      if (xColumn.type === 'Timestamp') {
        return formatTimestamp(value);
      }
      return String(value);
    });

    const values = data.rows.map(row => row[1]);

    const colors = generateColors(data.rows.length, currentColorScheme);

    const chartConfig = {
      type: type,
      data: {
        labels: labels,
        datasets: [{
          label: yColumn.name,
          data: values,
          backgroundColor: type === 'bar' || type === 'line'
            ? colors[0] + '80'
            : colors,
          borderColor: colors[0],
          borderWidth: type === 'line' ? 2 : 1,
          tension: 0.3,
          fill: type === 'line'
        }]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: {
            display: type === 'pie' || type === 'doughnut',
            position: 'bottom',
            labels: {
              color: textColor,
              padding: 15,
              font: {
                size: 12
              }
            }
          },
          tooltip: {
            backgroundColor: isDarkMode ? '#1f2937' : '#ffffff',
            titleColor: textColor,
            bodyColor: textColor,
            borderColor: gridColor,
            borderWidth: 1,
            padding: 12,
            displayColors: true,
            callbacks: {
              label: function(context) {
                return `${yColumn.name}: ${context.parsed.y || context.parsed}`;
              }
            }
          }
        },
        scales: type === 'bar' || type === 'line' ? {
          x: {
            ticks: {
              color: textColor,
              maxRotation: 45,
              minRotation: 0
            },
            grid: {
              color: gridColor,
              drawBorder: false
            }
          },
          y: {
            ticks: {
              color: textColor
            },
            grid: {
              color: gridColor,
              drawBorder: false
            },
            beginAtZero: true
          }
        } : undefined
      }
    };

    currentChart = new Chart(chartCanvas, chartConfig);
  }

  function renderGroupedChart(data, type, isDarkMode, textColor, gridColor) {
    // For grouped data: first column is x-axis (bucket),
    // middle columns are grouping dimensions, last column is the value
    const xColumn = data.columns[0];
    const valueColumn = data.columns[data.columns.length - 1];
    const groupColumns = data.columns.slice(1, -1);

    // Build a composite key for grouping
    const seriesMap = new Map();
    const xAxisValues = new Set();

    data.rows.forEach(row => {
      const xValue = row[0];
      const yValue = row[row.length - 1];

      // Create series key from middle columns
      const seriesKey = groupColumns.map((col, idx) => row[idx + 1]).join(' | ');

      xAxisValues.add(xValue);

      if (!seriesMap.has(seriesKey)) {
        seriesMap.set(seriesKey, new Map());
      }
      seriesMap.get(seriesKey).set(xValue, yValue);
    });

    // Convert x-axis values to sorted array
    const sortedXValues = Array.from(xAxisValues).sort((a, b) => a - b);
    const labels = sortedXValues.map(value => {
      if (xColumn.type === 'Timestamp') {
        return formatTimestamp(value);
      }
      return String(value);
    });

    // Create datasets for each series
    const colors = generateColors(seriesMap.size, currentColorScheme);
    const datasets = [];
    let colorIndex = 0;

    seriesMap.forEach((dataPoints, seriesKey) => {
      const color = colors[colorIndex % colors.length];
      const data = sortedXValues.map(xValue => dataPoints.get(xValue) || 0);

      datasets.push({
        label: seriesKey,
        data: data,
        backgroundColor: type === 'bar' || type === 'line' ? color + '80' : color,
        borderColor: color,
        borderWidth: type === 'line' ? 2 : 1,
        tension: 0.3,
        fill: false
      });

      colorIndex++;
    });

    // For pie/doughnut charts with grouped data, aggregate across all buckets
    if (type === 'pie' || type === 'doughnut') {
      const aggregated = new Map();
      seriesMap.forEach((dataPoints, seriesKey) => {
        let sum = 0;
        dataPoints.forEach(value => sum += value);
        aggregated.set(seriesKey, sum);
      });

      const pieLabels = Array.from(aggregated.keys());
      const pieValues = Array.from(aggregated.values());
      const pieColors = generateColors(pieLabels.length, currentColorScheme);

      const chartConfig = {
        type: type,
        data: {
          labels: pieLabels,
          datasets: [{
            label: valueColumn.name,
            data: pieValues,
            backgroundColor: pieColors,
            borderColor: pieColors.map(c => c),
            borderWidth: 1
          }]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: {
              display: true,
              position: 'right',
              labels: {
                color: textColor,
                padding: 15,
                font: {
                  size: 12
                }
              }
            },
            tooltip: {
              backgroundColor: isDarkMode ? '#1f2937' : '#ffffff',
              titleColor: textColor,
              bodyColor: textColor,
              borderColor: gridColor,
              borderWidth: 1,
              padding: 12,
              displayColors: true
            }
          }
        }
      };

      currentChart = new Chart(chartCanvas, chartConfig);
      return;
    }

    // Bar and Line charts
    const chartConfig = {
      type: type,
      data: {
        labels: labels,
        datasets: datasets
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: {
          mode: 'index',
          intersect: false
        },
        plugins: {
          legend: {
            display: true,
            position: 'top',
            labels: {
              color: textColor,
              padding: 15,
              font: {
                size: 12
              },
              usePointStyle: true
            }
          },
          tooltip: {
            backgroundColor: isDarkMode ? '#1f2937' : '#ffffff',
            titleColor: textColor,
            bodyColor: textColor,
            borderColor: gridColor,
            borderWidth: 1,
            padding: 12,
            displayColors: true
          }
        },
        scales: {
          x: {
            stacked: type === 'bar',
            ticks: {
              color: textColor,
              maxRotation: 45,
              minRotation: 0
            },
            grid: {
              color: gridColor,
              drawBorder: false
            }
          },
          y: {
            stacked: type === 'bar',
            ticks: {
              color: textColor
            },
            grid: {
              color: gridColor,
              drawBorder: false
            },
            beginAtZero: true
          }
        }
      }
    };

    currentChart = new Chart(chartCanvas, chartConfig);
  }

  function renderTable(data) {
    if (!data || !data.columns || !data.rows) return;

    dataTable.innerHTML = '';

    const thead = document.createElement('thead');
    const headerRow = document.createElement('tr');
    data.columns.forEach(col => {
      const th = document.createElement('th');
      th.textContent = `${col.name} (${col.type})`;
      headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    dataTable.appendChild(thead);

    const tbody = document.createElement('tbody');
    data.rows.forEach(row => {
      const tr = document.createElement('tr');
      row.forEach((cell, idx) => {
        const td = document.createElement('td');
        if (data.columns[idx].type === 'Timestamp') {
          td.textContent = formatTimestamp(cell);
        } else {
          td.textContent = cell;
        }
        tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    dataTable.appendChild(tbody);
  }

  function handleQueryResult(text) {
    try {
      // Strip TCP response headers if present (e.g., "200 OK\n")
      let jsonText = text.trim();
      const lines = jsonText.split('\n');
      if (lines.length > 1 && lines[0].match(/^\d{3}\s+\w+$/)) {
        // First line looks like a status code, skip it
        jsonText = lines.slice(1).join('\n').trim();
      }

      const json = JSON.parse(jsonText);

      // Check if data is in the top level (columns/rows)
      let data = null;
      if (json.columns && json.rows && Array.isArray(json.columns) && Array.isArray(json.rows)) {
        data = json;
      }
      // Check if data is nested in results array (SnelDB response format)
      else if (json.results && Array.isArray(json.results) && json.results.length > 0) {
        const firstResult = json.results[0];
        if (firstResult.columns && firstResult.rows &&
            Array.isArray(firstResult.columns) && Array.isArray(firstResult.rows)) {
          data = firstResult;
        }
      }

      if (data) {
        renderChart(data, currentChartType);
        renderTable(data);
        setOutput(JSON.stringify(json, null, 2));

        switchView('chart');
      } else {
        emptyState.style.display = 'flex';
        chartControls.style.display = 'none';
        viewControls.style.display = 'none';
        setOutput(text);
      }
    } catch (e) {
      emptyState.style.display = 'flex';
      chartControls.style.display = 'none';
      viewControls.style.display = 'none';
      setOutput(text);
    }
  }

  async function run() {
    const input = cmd.value.trim();
    if (!input) {
      setStatus('Empty command', 'error');
      return;
    }

    setStatus('Running...', 'running');

    const headers = { 'Content-Type': 'text/plain' };
    if (window.SNELDB_AUTH_TOKEN) {
      headers['Authorization'] = 'Bearer ' + window.SNELDB_AUTH_TOKEN;
    }

    const lines = input.split(/\n+/).map(s => s.trim()).filter(Boolean);
    const outputs = [];

    for (const line of lines) {
      try {
        const res = await fetch('/command', { method: 'POST', headers, body: line });
        const text = await res.text();

        if (!res.ok) {
          outputs.push(text);
          outputs.push(`-- Error ${res.status}`);
          setStatus(`Error ${res.status}`, 'error');
          break;
        }

        outputs.push(text);
      } catch (e) {
        outputs.push(String(e));
        setStatus('Request failed', 'error');
        break;
      }
    }

    const result = outputs.join('\n');
    handleQueryResult(result);

    if (status.className.indexOf('error') === -1) {
      setStatus('Done', 'ready');
    }
  }

  function switchView(viewName) {
    document.querySelectorAll('.view-content').forEach(el => {
      el.classList.remove('active');
    });
    document.querySelectorAll('.view-btn').forEach(btn => {
      btn.classList.remove('active');
    });

    const viewEl = $(viewName + 'View');
    const btnEl = $(viewName + 'ViewBtn');

    if (viewEl) viewEl.classList.add('active');
    if (btnEl) btnEl.classList.add('active');
  }

  runBtn.addEventListener('click', run);

  clrBtn.addEventListener('click', () => {
    setOutput('');
    emptyState.style.display = 'flex';
    chartControls.style.display = 'none';
    viewControls.style.display = 'none';
    if (currentChart) {
      currentChart.destroy();
      currentChart = null;
    }
    dataTable.innerHTML = '';
    currentData = null;
  });

  cmd.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
      e.preventDefault();
      run();
    }
  });

  document.querySelectorAll('.view-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      const view = btn.dataset.view;
      switchView(view);
    });
  });

  document.querySelectorAll('.chart-type-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.chart-type-btn').forEach(b => {
        b.classList.remove('active');
      });
      btn.classList.add('active');

      const type = btn.dataset.type;
      if (currentData) {
        renderChart(currentData, type);
      }
    });
  });

  const colorSchemeSelect = $('colorSchemeSelect');
  if (colorSchemeSelect) {
    colorSchemeSelect.addEventListener('change', (e) => {
      currentColorScheme = e.target.value;
      if (currentData) {
        renderChart(currentData, currentChartType);
      }
    });
  }

  if (themeToggle) {
    themeToggle.addEventListener('click', toggleTheme);
  }

  function syncHighlight() {
    const escaped = cmd.value
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;');
    hl.innerHTML = escaped;
    if (window.hljs) {
      if (typeof hljs.highlightElement === 'function') {
        hljs.highlightElement(hl);
      } else if (typeof hljs.highlightBlock === 'function') {
        hljs.highlightBlock(hl);
      }
    }
  }

  cmd.addEventListener('input', syncHighlight);
  syncHighlight();
})();


