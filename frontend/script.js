/**
 * Fetches housing affordability data from the local API endpoint (/api/data)
 * and dynamically renders an HTML table in the data-table element.
 */
async function fetchData() {
    // 1. Fetch data from the assumed API endpoint
    // NOTE: This endpoint assumes your Python backend is running and routing '/api/data'
    // This will work when deployed to a server/service that hosts both frontend and backend.
    try {
        const res = await fetch('/api/data');
        const json = await res.json();
        
        // Handle potential nested data structure (e.g., if the API returns { data: [...] })
        const data = json.data || json;

        if (!data || data.length === 0) {
            document.getElementById('rows').innerHTML = '<tr><td colspan="100%" class="p-4 text-center text-gray-400">No data loaded. Please ensure your ETL (fetch_data.py) has run.</td></tr>';
            return;
        }

        // 2. Clear existing content and prepare table references
        const header = document.getElementById('header');
        const rows = document.getElementById('rows');
        header.innerHTML = '';
        rows.innerHTML = '';
        
        // 3. Render Table Header (using keys from the first object)
        Object.keys(data[0]).forEach(col => {
            const th = document.createElement('th');
            th.className = 'px-4 py-2 font-semibold text-gray-700 uppercase tracking-wider';
            th.textContent = col.replace(/_/g, ' '); // Replace underscores for display
            header.appendChild(th);
        });
        
        // 4. Render Table Rows
        data.forEach(row => {
            const tr = document.createElement('tr');
            tr.className = 'hover:bg-indigo-50 transition-colors duration-150';

            Object.values(row).forEach(v => {
                const td = document.createElement('td');
                // Format the value slightly, if it looks like a number
                let displayValue = (v === null || v === undefined) ? '' : String(v);

                // Add simple styling for table cells
                td.className = 'border-t border-gray-100 px-4 py-3 whitespace-nowrap';
                
                td.textContent = displayValue;
                tr.appendChild(td);
            });
            rows.appendChild(tr);
        });
    } catch (error) {
        console.error("Error fetching data:", error);
        document.getElementById('rows').innerHTML = '<tr><td colspan="100%" class="p-4 text-center text-red-500">Failed to connect to the API. Check backend status.</td></tr>';
    }
}

// Kick off the data fetching process when the script loads
document.addEventListener('DOMContentLoaded', fetchData);