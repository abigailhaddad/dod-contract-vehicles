/**
 * Shared utilities for USAJobs Historical Data Viewer
 * Provides: ServerSideFilterManager, URL sync, toast notifications, chart integration
 */

// ============================================
// Utility Functions
// ============================================

function getCleanURL() {
    const url = new URL(window.location);
    if (url.pathname.endsWith('/index.html')) {
        url.pathname = url.pathname.replace(/\/index\.html$/, '/');
    }
    return url;
}

function escapeHtml(text) {
    if (!text) return '';
    const map = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#039;' };
    return String(text).replace(/[&<>"']/g, m => map[m]);
}

function formatCurrency(value) {
    if (value == null || isNaN(value)) return '';
    const num = Number(value);
    if (num >= 1e6) return '$' + (num / 1e6).toFixed(1) + 'M';
    if (num >= 1e3) return '$' + Math.round(num / 1e3).toLocaleString() + 'K';
    return '$' + num.toLocaleString();
}

function formatNumber(value) {
    if (value == null || isNaN(value)) return '';
    const num = Number(value);
    if (num >= 1e6) return (num / 1e6).toFixed(1) + 'M';
    return num.toLocaleString();
}

// ============================================
// Lightweight Modal
// ============================================

function createModal(options = {}) {
    const overlay = document.createElement('div');
    overlay.className = 'filter-modal ' + (options.className || '');

    // Content is built from escaped values via escapeHtml() by all callers
    const inner = document.createElement('div');
    inner.innerHTML = options.content || ''; // nosemgrep: all dynamic values pre-escaped via escapeHtml()
    while (inner.firstChild) {
        overlay.appendChild(inner.firstChild);
    }

    overlay.addEventListener('click', (e) => {
        if (e.target === overlay) closeModal(overlay);
    });

    const escHandler = (e) => {
        if (e.key === 'Escape') {
            closeModal(overlay);
            document.removeEventListener('keydown', escHandler);
        }
    };
    document.addEventListener('keydown', escHandler);

    document.body.appendChild(overlay);
    return overlay;
}

function closeModal(modal) {
    if (modal && modal.parentNode) {
        modal.parentNode.removeChild(modal);
    }
}

// ============================================
// Toast Notifications
// ============================================

// ============================================
// CSV Download (client-side)
// ============================================

/**
 * Download an array of objects as a CSV file.
 * @param {Array<Object>} rows - records to export
 * @param {Array<{field: string, name: string}>} columns - ordered fields; field is the key, name is the header
 * @param {string} filename - filename to suggest
 */
function downloadCSV(rows, columns, filename) {
    const cols = columns.filter(c => c && c.field);
    const esc = (v) => {
        if (v == null) return '';
        if (Array.isArray(v)) v = v.join('; ');
        const s = String(v);
        if (/[",\n\r]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
        return s;
    };
    const header = cols.map(c => esc(c.name || c.field)).join(',');
    const body = rows.map(r => cols.map(c => esc(r[c.field])).join(',')).join('\n');
    const csv = header + '\n' + body;
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename || 'export.csv';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}

function showToast(message, isError = false) {
    const toast = document.createElement('div');
    toast.className = 'toast' + (isError ? ' toast-error' : ' toast-success');
    toast.textContent = message;
    document.body.appendChild(toast);

    requestAnimationFrame(() => toast.classList.add('show'));

    setTimeout(() => {
        toast.classList.remove('show');
        setTimeout(() => toast.remove(), 300);
    }, 2000);
}

// ============================================
// ServerSideFilterManager
// ============================================

class ServerSideFilterManager {
    /**
     * @param {Object} options
     * @param {string} options.tableSelector - jQuery selector for the DataTable
     * @param {Array} options.columns - Column config array with {name, field, filterType, index, format?}
     * @param {string} options.filterBarId - ID of the filter bar container
     * @param {boolean} options.syncURL - Whether to sync filters to URL (default true)
     * @param {boolean} options.showCopyLinkButton - Whether to show copy-link button (default true)
     * @param {Function} options.onFilterChange - Callback when filters change (for chart updates)
     * @param {boolean} options.clientSide - When true, compute options and apply filters client-side from options.data
     * @param {Array} options.data - Array of records for client-side mode
     * @param {Function} options.optionsProvider - Optional override: (field) => string[] returning filter options
     */
    constructor(options) {
        this.tableSelector = options.tableSelector;
        this.columns = options.columns || [];
        this.filterBarId = options.filterBarId || null;
        this.syncURL = options.syncURL !== false;
        this.showCopyLinkButton = options.showCopyLinkButton !== false;
        this.onFilterChange = options.onFilterChange || null;
        this.clientSide = options.clientSide === true;
        this.data = options.data || null;
        this.optionsProvider = options.optionsProvider || null;
        this.table = null;
        this.activeFilters = {};
        this._optionsCache = {};

        // Parse URL filters early so they're available for the first DataTable AJAX call
        if (this.syncURL) {
            this._applyFiltersFromURL();
        }
    }

    /**
     * Update the in-memory data set (client-side mode).
     * Clears the options cache so filter dialogs recompute.
     */
    setData(data) {
        this.data = data;
        this._optionsCache = {};
    }

    /**
     * Return the filtered subset of the client-side data based on activeFilters.
     * Useful for updating charts/stats from the same filter state.
     */
    getFilteredData() {
        if (!this.clientSide || !this.data) return [];
        return this.data.filter(row => this._rowMatches(row));
    }

    _rowMatches(row) {
        for (const [field, filter] of Object.entries(this.activeFilters)) {
            const val = row[field];
            if (filter.type === 'multiselect') {
                if (Array.isArray(val)) {
                    const anyMatch = val.some(v => filter.values.includes(String(v)));
                    if (!anyMatch) return false;
                } else {
                    if (val == null || !filter.values.includes(String(val))) return false;
                }
            } else if (filter.type === 'range') {
                const num = val == null || val === '' ? null : Number(val);
                if (num === null || isNaN(num)) return false;
                if (filter.min != null && num < filter.min) return false;
                if (filter.max != null && num > filter.max) return false;
            } else if (filter.type === 'daterange') {
                if (!val) return false;
                const v = String(val);
                if (filter.min && v < filter.min) return false;
                if (filter.max && v > filter.max) return false;
            } else if (filter.type === 'text') {
                if (val == null) return false;
                const hay = String(val).toLowerCase();
                const terms = String(filter.value).split(',').map(t => t.trim().toLowerCase()).filter(t => t);
                const anyMatch = terms.some(t => hay.includes(t));
                if (!anyMatch) return false;
            }
        }
        return true;
    }

    init(dataTable) {
        this.table = dataTable;
        if (this.filterBarId) {
            this._setupFilterBar();
        }



        // Update filter bar UI (filters were already parsed in constructor)
        if (Object.keys(this.activeFilters).length > 0) {
            this._updateFilterBar();
        }
    }

    // ---- Server Params ----

    /**
     * Returns a flat object of filter params for the server.
     * Multiselect: filter_fieldName = "val1|val2|val3"
     * Range: filter_fieldName_min = 50000, filter_fieldName_max = 120000
     * Text: filter_fieldName = "search text"
     */
    getServerParams() {
        const params = {};
        for (const [field, filter] of Object.entries(this.activeFilters)) {
            if (filter.type === 'multiselect' && filter.values && filter.values.length > 0) {
                params['filter_' + field] = filter.values.join('|');
            } else if (filter.type === 'range' || filter.type === 'daterange') {
                if (filter.min !== null && filter.min !== undefined && filter.min !== '') {
                    params['filter_' + field + '_min'] = filter.min;
                }
                if (filter.max !== null && filter.max !== undefined && filter.max !== '') {
                    params['filter_' + field + '_max'] = filter.max;
                }
            } else if (filter.type === 'text' && filter.value) {
                params['filter_' + field] = filter.value;
            }
        }
        return params;
    }

    /**
     * Returns query string of current filters (for chart API calls).
     */
    getFilterQueryString() {
        const params = this.getServerParams();
        const parts = [];
        for (const [key, val] of Object.entries(params)) {
            parts.push(encodeURIComponent(key) + '=' + encodeURIComponent(val));
        }
        return parts.join('&');
    }

    // ---- Filter Bar Setup ----

    _setupFilterBar() {
        const filterBar = document.getElementById(this.filterBarId);
        if (!filterBar) return;

        const buttonContainer = document.getElementById('toolbarButtons') || filterBar;

        let addBtn = buttonContainer.querySelector('.add-filter-btn');
        if (!addBtn) {
            addBtn = document.createElement('button');
            addBtn.className = 'add-filter-btn';
            addBtn.textContent = '+ Add Filter';
            buttonContainer.appendChild(addBtn);
        }
        addBtn.addEventListener('click', () => this._openFilterSelection());

        let clearBtn = buttonContainer.querySelector('.clear-filters-btn');
        if (!clearBtn) {
            clearBtn = document.createElement('button');
            clearBtn.className = 'clear-filters-btn';
            clearBtn.textContent = 'Clear All';
            clearBtn.title = 'Remove all filters';
            clearBtn.style.display = 'none';
            buttonContainer.appendChild(clearBtn);
            clearBtn.addEventListener('click', () => this.clearAll());
        }

        if (this.syncURL && this.showCopyLinkButton) {
            let copyBtn = buttonContainer.querySelector('.copy-link-btn');
            if (!copyBtn) {
                copyBtn = document.createElement('button');
                copyBtn.className = 'copy-link-btn';
                copyBtn.textContent = 'Copy Link';
                copyBtn.title = 'Copy shareable link with current filters';
                buttonContainer.appendChild(copyBtn);
                copyBtn.addEventListener('click', () => this.copyShareableURL());
            }
        }
    }

    // ---- Filter Selection Dialog ----

    _openFilterSelection() {
        const filterableColumns = this.columns.filter(col => col.filterType !== null);
        const optionItems = filterableColumns.map(col => {
            return '<label class="filter-option">'
                + '<input type="checkbox" value="' + escapeHtml(col.field) + '" data-name="' + escapeHtml(col.name) + '" data-type="' + col.filterType + '">'
                + escapeHtml(col.name)
                + '</label>';
        }).join('');

        const content = '<div class="filter-popover">'
            + '<div class="filter-title">Add filter</div>'
            + '<input type="text" class="filter-search filter-options-search" placeholder="Search columns...">'
            + '<div class="filter-options">'
            + optionItems
            + '</div></div>';

        const modal = createModal({ content });

        $(modal).find('.filter-options-search').on('input', function () {
            const query = this.value.toLowerCase();
            $(modal).find('.filter-option').each(function () {
                const label = this.textContent.toLowerCase();
                this.style.display = label.includes(query) ? '' : 'none';
            });
        }).focus();

        modal.querySelectorAll('input[type="checkbox"]').forEach(checkbox => {
            checkbox.addEventListener('change', () => {
                if (checkbox.checked) {
                    closeModal(modal);
                    const field = checkbox.value;
                    const col = this.columns.find(c => c.field === field);
                    if (col) this._openFilterDialog(col);
                }
            });
        });
    }

    _openFilterDialog(col) {
        if (col.filterType === 'multiselect') {
            this._openMultiselectDialog(col);
        } else if (col.filterType === 'range') {
            this._openRangeDialog(col);
        } else if (col.filterType === 'daterange') {
            this._openDateRangeDialog(col);
        } else if (col.filterType === 'text') {
            this._openTextDialog(col);
        }
    }

    // ---- Multiselect Dialog ----

    async _openMultiselectDialog(col) {
        // Show loading modal first
        const loadingContent = '<div class="filter-popover">'
            + '<div class="filter-title">Filter: ' + escapeHtml(col.name) + '</div>'
            + '<p class="loading-text">Loading...</p>'
            + '</div>';
        const loadingModal = createModal({ content: loadingContent });

        try {
            // Fetch options from server
            const values = await this._fetchFilterOptions(col.field);
            closeModal(loadingModal);

            const currentFilter = this.activeFilters[col.field];
            const selectedValues = currentFilter?.values || [];

            const optionItems = values.map(val => {
                const checked = selectedValues.includes(val) ? ' checked' : '';
                return '<label class="filter-option">'
                    + '<input type="checkbox" value="' + escapeHtml(val) + '"' + checked + '>'
                    + escapeHtml(val)
                    + '</label>';
            }).join('');

            const content = '<div class="filter-popover">'
                + '<div class="filter-title">Filter: ' + escapeHtml(col.name) + '</div>'
                + '<input type="text" class="filter-search filter-options-search" placeholder="Search options...">'
                + '<div class="filter-options">'
                + optionItems
                + '</div>'
                + '<div class="filter-buttons">'
                + '<button class="btn btn-clear">Clear</button>'
                + '<button class="btn btn-apply">Apply</button>'
                + '</div></div>';

            const modal = createModal({ content });
            const $popover = $(modal).find('.filter-popover');

            $popover.find('.filter-options-search').on('input', function () {
                const query = this.value.toLowerCase();
                $popover.find('.filter-option').each(function () {
                    const label = this.textContent.toLowerCase();
                    this.style.display = label.includes(query) ? '' : 'none';
                });
            }).focus();

            $popover.find('.btn-clear').on('click', () => {
                delete this.activeFilters[col.field];
                this._applyAndRedraw();
                closeModal(modal);
            });

            $popover.find('.btn-apply').on('click', () => {
                const checked = [];
                $popover.find('input[type="checkbox"]:checked').each(function () {
                    checked.push($(this).val());
                });

                if (checked.length > 0) {
                    this.activeFilters[col.field] = { type: 'multiselect', values: checked, name: col.name };
                } else {
                    delete this.activeFilters[col.field];
                }
                this._applyAndRedraw();
                closeModal(modal);
            });

        } catch (err) {
            closeModal(loadingModal);
            showToast('Failed to load filter options', true);
            console.error('Failed to fetch filter options:', err);
        }
    }

    async _fetchFilterOptions(field) {
        if (this._optionsCache[field]) {
            return this._optionsCache[field];
        }

        // Client-side: compute from loaded data (or from optionsProvider)
        if (this.clientSide) {
            let values;
            if (this.optionsProvider) {
                values = await Promise.resolve(this.optionsProvider(field));
            } else if (this.data) {
                const set = new Set();
                for (const row of this.data) {
                    const v = row[field];
                    if (v == null || v === '') continue;
                    if (Array.isArray(v)) {
                        for (const item of v) {
                            if (item != null && item !== '') set.add(String(item));
                        }
                    } else {
                        set.add(String(v));
                    }
                }
                values = [...set].sort((a, b) => a.localeCompare(b));
            } else {
                values = [];
            }
            this._optionsCache[field] = values;
            return values;
        }

        // Server-side: original behavior
        const base = window.API_BASE || '';
        const resp = await fetch(base + '/api/filter_options?field=' + encodeURIComponent(field));
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        this._optionsCache[field] = data.values || [];
        return this._optionsCache[field];
    }

    // ---- Text Dialog ----

    _openTextDialog(col) {
        const currentFilter = this.activeFilters[col.field];
        const currentTerms = currentFilter?.value ? currentFilter.value.split(',').map(t => t.trim()).filter(t => t) : [];

        const content = '<div class="filter-popover">'
            + '<div class="filter-title">Filter: ' + escapeHtml(col.name) + '</div>'
            + '<div class="text-tags-container"></div>'
            + '<input type="text" class="filter-text-input filter-search" placeholder="Type a term and press Enter...">'
            + '<div class="filter-buttons">'
            + '<button class="btn btn-clear">Clear</button>'
            + '<button class="btn btn-apply">Apply</button>'
            + '</div></div>';

        const modal = createModal({ content });
        const $popover = $(modal).find('.filter-popover');
        const $input = $popover.find('.filter-text-input');
        const $tags = $popover.find('.text-tags-container');
        const terms = [...currentTerms];

        function renderTags() {
            $tags.empty();
            terms.forEach((term, i) => {
                const tag = $('<span class="text-tag">')
                    .append($('<span class="text-tag-label">').text(term))
                    .append($('<span class="text-tag-remove">').text('\u00d7').on('click', () => {
                        terms.splice(i, 1);
                        renderTags();
                        $input.focus();
                    }));
                $tags.append(tag);
            });
        }

        function addTerm() {
            const val = $input.val().trim();
            if (val && !terms.includes(val)) {
                terms.push(val);
                renderTags();
            }
            $input.val('').focus();
        }

        renderTags();
        $input.focus();

        $input.on('keydown', function (e) {
            if (e.key === 'Enter') {
                e.preventDefault();
                addTerm();
            } else if (e.key === 'Backspace' && !$input.val() && terms.length > 0) {
                terms.pop();
                renderTags();
            }
        });

        $popover.find('.btn-clear').on('click', () => {
            delete this.activeFilters[col.field];
            this._applyAndRedraw();
            closeModal(modal);
        });

        $popover.find('.btn-apply').on('click', () => {
            // Add any remaining text in input
            const val = $input.val().trim();
            if (val && !terms.includes(val)) terms.push(val);

            if (terms.length > 0) {
                this.activeFilters[col.field] = { type: 'text', value: terms.join(','), name: col.name };
            } else {
                delete this.activeFilters[col.field];
            }
            this._applyAndRedraw();
            closeModal(modal);
        });
    }

    // ---- Range Dialog ----

    _openRangeDialog(col) {
        const currentFilter = this.activeFilters[col.field];
        const currentMin = currentFilter?.min ?? '';
        const currentMax = currentFilter?.max ?? '';

        const content = '<div class="filter-popover">'
            + '<div class="filter-title">Filter: ' + escapeHtml(col.name) + '</div>'
            + '<div style="display: flex; gap: 8px; align-items: center; margin-bottom: 12px;">'
            + '<input type="number" class="filter-range-min filter-search" placeholder="Min" value="' + escapeHtml(String(currentMin)) + '" style="flex:1;">'
            + '<span style="color: var(--color-text-muted);">to</span>'
            + '<input type="number" class="filter-range-max filter-search" placeholder="Max" value="' + escapeHtml(String(currentMax)) + '" style="flex:1;">'
            + '</div>'
            + '<div class="filter-buttons">'
            + '<button class="btn btn-clear">Clear</button>'
            + '<button class="btn btn-apply">Apply</button>'
            + '</div></div>';

        const modal = createModal({ content });
        const $popover = $(modal).find('.filter-popover');

        $popover.find('.filter-range-min').focus();

        $popover.find('input[type="number"]').on('keypress', function (e) {
            if (e.key === 'Enter') $popover.find('.btn-apply').click();
        });

        $popover.find('.btn-clear').on('click', () => {
            delete this.activeFilters[col.field];
            this._applyAndRedraw();
            closeModal(modal);
        });

        $popover.find('.btn-apply').on('click', () => {
            const minVal = $popover.find('.filter-range-min').val().trim();
            const maxVal = $popover.find('.filter-range-max').val().trim();
            const min = minVal !== '' ? parseFloat(minVal) : null;
            const max = maxVal !== '' ? parseFloat(maxVal) : null;

            if (min !== null || max !== null) {
                this.activeFilters[col.field] = { type: 'range', min, max, name: col.name, format: col.format };
            } else {
                delete this.activeFilters[col.field];
            }
            this._applyAndRedraw();
            closeModal(modal);
        });
    }

    // ---- Date Range Dialog ----

    _openDateRangeDialog(col) {
        const currentFilter = this.activeFilters[col.field];
        const currentMin = currentFilter?.min ?? '';
        const currentMax = currentFilter?.max ?? '';

        const content = '<div class="filter-popover">'
            + '<div class="filter-title">Filter: ' + escapeHtml(col.name) + '</div>'
            + '<div style="display: flex; gap: 8px; align-items: center; margin-bottom: 12px;">'
            + '<input type="date" class="filter-date-min filter-search" value="' + escapeHtml(String(currentMin)) + '" style="flex:1;">'
            + '<span style="color: var(--color-text-muted);">to</span>'
            + '<input type="date" class="filter-date-max filter-search" value="' + escapeHtml(String(currentMax)) + '" style="flex:1;">'
            + '</div>'
            + '<div class="filter-buttons">'
            + '<button class="btn btn-clear">Clear</button>'
            + '<button class="btn btn-apply">Apply</button>'
            + '</div></div>';

        const modal = createModal({ content });
        const $popover = $(modal).find('.filter-popover');

        $popover.find('.filter-date-min').focus();

        $popover.find('input[type="date"]').on('keypress', function (e) {
            if (e.key === 'Enter') $popover.find('.btn-apply').click();
        });

        $popover.find('.btn-clear').on('click', () => {
            delete this.activeFilters[col.field];
            this._applyAndRedraw();
            closeModal(modal);
        });

        $popover.find('.btn-apply').on('click', () => {
            const minVal = $popover.find('.filter-date-min').val().trim();
            const maxVal = $popover.find('.filter-date-max').val().trim();

            if (minVal || maxVal) {
                this.activeFilters[col.field] = {
                    type: 'daterange',
                    min: minVal || null,
                    max: maxVal || null,
                    name: col.name
                };
            } else {
                delete this.activeFilters[col.field];
            }
            this._applyAndRedraw();
            closeModal(modal);
        });
    }

    // ---- Apply Filters and Redraw ----

    _applyAndRedraw() {
        this._updateFilterBar();
        if (this.syncURL) this._updateURL();
        // Redraw the DataTable. In server-side mode, this re-fetches with new params.
        // In client-side mode, the caller typically registers a $.fn.dataTable.ext.search
        // function that reads this.activeFilters, so draw() re-runs it.
        if (this.table) {
            try { this.table.draw(); } catch (e) { /* ignore */ }
        }
        // Notify chart update callback
        if (this.onFilterChange) this.onFilterChange();
    }

    // ---- Filter Bar Chips ----

    _updateFilterBar() {
        if (!this.filterBarId) return;
        const filterBar = document.getElementById(this.filterBarId);
        if (!filterBar) return;

        filterBar.querySelectorAll('.filter-chip.column-filter-chip').forEach(c => c.remove());
        const existingLabel = filterBar.querySelector('.bar-label.filter-label');
        if (existingLabel) existingLabel.remove();

        const hasFilters = Object.keys(this.activeFilters).length > 0;

        const emptyMsg = filterBar.querySelector('.filters-bar-empty');
        if (emptyMsg) emptyMsg.style.display = hasFilters ? 'none' : '';

        const clearBtn = document.querySelector('.clear-filters-btn');
        if (clearBtn) clearBtn.style.display = hasFilters ? '' : 'none';

        if (hasFilters) {
            const label = document.createElement('span');
            label.className = 'bar-label filter-label';
            label.textContent = 'Filtered by:';
            filterBar.insertBefore(label, filterBar.firstChild);

            Object.entries(this.activeFilters).forEach(([field, filter]) => {
                const chip = document.createElement('div');
                chip.className = 'filter-chip column-filter-chip';

                let displayValue;
                if (filter.type === 'multiselect') {
                    displayValue = filter.values.join(', ');
                } else if (filter.type === 'range') {
                    const fmt = filter.format === 'currency' ? formatCurrency : (v) => v.toLocaleString();
                    const parts = [];
                    if (filter.min !== null) parts.push(fmt(filter.min));
                    parts.push('\u2013');
                    if (filter.max !== null) parts.push(fmt(filter.max));
                    displayValue = parts.join(' ');
                } else if (filter.type === 'daterange') {
                    const parts = [];
                    if (filter.min) parts.push(filter.min);
                    parts.push('\u2013');
                    if (filter.max) parts.push(filter.max);
                    displayValue = parts.join(' ');
                } else {
                    displayValue = filter.value;
                }

                const chipLabel = document.createElement('span');
                chipLabel.className = 'filter-chip-label';
                chipLabel.textContent = filter.name + ':';

                const chipValue = document.createElement('span');
                chipValue.className = 'filter-chip-value';
                chipValue.textContent = displayValue;

                const chipRemove = document.createElement('span');
                chipRemove.className = 'filter-chip-remove';
                chipRemove.textContent = '\u00d7';
                chipRemove.addEventListener('click', () => {
                    delete this.activeFilters[field];
                    this._applyAndRedraw();
                });

                // Click chip label/value to edit the filter
                const editHandler = () => {
                    const col = this.columns.find(c => c.field === field);
                    if (col) this._openFilterDialog(col);
                };
                chipLabel.style.cursor = 'pointer';
                chipValue.style.cursor = 'pointer';
                chipLabel.addEventListener('click', editHandler);
                chipValue.addEventListener('click', editHandler);

                chip.appendChild(chipLabel);
                chip.appendChild(chipValue);
                chip.appendChild(chipRemove);
                filterBar.appendChild(chip);
            });
        }

    }

    // ---- URL Sync ----

    _updateURL() {
        const url = getCleanURL();
        url.search = '';

        Object.entries(this.activeFilters).forEach(([field, filter]) => {
            if (filter.type === 'multiselect') {
                url.searchParams.set(field, filter.values.join(','));
            } else if (filter.type === 'daterange') {
                const min = filter.min || '';
                const max = filter.max || '';
                url.searchParams.set(field, min + '~' + max);
            } else if (filter.type === 'range') {
                const min = filter.min !== null ? filter.min : '';
                const max = filter.max !== null ? filter.max : '';
                url.searchParams.set(field, min + '-' + max);
            } else {
                url.searchParams.set(field, filter.value);
            }
        });

        window.history.replaceState({}, '', url);
    }

    _applyFiltersFromURL() {
        const params = new URLSearchParams(window.location.search);
        if (params.toString() === '') return;

        const fieldToColumn = {};
        this.columns.forEach(col => {
            if (col.filterType) {
                fieldToColumn[col.field] = col;
            }
        });

        let hasFilters = false;

        params.forEach((value, key) => {
            const col = fieldToColumn[key];
            if (!col) return;

            if (col.filterType === 'multiselect') {
                const values = value.split(',').map(v => v.trim()).filter(v => v);
                if (values.length > 0) {
                    this.activeFilters[col.field] = { type: 'multiselect', values, name: col.name };
                    hasFilters = true;
                }
            } else if (col.filterType === 'daterange') {
                const parts = value.split('~');
                const min = parts[0] || null;
                const max = parts.length > 1 && parts[1] ? parts[1] : null;
                if (min || max) {
                    this.activeFilters[col.field] = { type: 'daterange', min, max, name: col.name };
                    hasFilters = true;
                }
            } else if (col.filterType === 'range') {
                const parts = value.split('-');
                const min = parts[0] !== '' ? parseFloat(parts[0]) : null;
                const max = parts.length > 1 && parts[1] !== '' ? parseFloat(parts[1]) : null;
                if (min !== null || max !== null) {
                    this.activeFilters[col.field] = { type: 'range', min, max, name: col.name, format: col.format };
                    hasFilters = true;
                }
            } else if (col.filterType === 'text') {
                if (value) {
                    this.activeFilters[col.field] = { type: 'text', value, name: col.name };
                    hasFilters = true;
                }
            }
        });

        if (hasFilters) {
            this._updateFilterBar();
            // Table will pick up params on first draw via ajax.data callback
        }
    }


    clearAll() {
        this.activeFilters = {};
        this._applyAndRedraw();
    }

    copyShareableURL() {
        navigator.clipboard.writeText(window.location.href).then(() => {
            showToast('Link copied to clipboard!');
        }).catch(() => {
            showToast('Failed to copy link', true);
        });
    }
}
