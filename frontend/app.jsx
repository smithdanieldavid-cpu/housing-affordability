import React, { useState, useEffect, useCallback } from 'react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

// Component must be named 'App' and be the default export
export default function App() {
    // FIX: Simplified API_BASE_URL to rely on relative path for local testing
    // If running in the same environment (e.g., localhost:8000), this ensures the call works.
    // This should be updated to your actual Azure URL after deployment.
    const API_BASE_URL = window.location.host.includes('localhost') || window.location.port === '8000'
        ? '' // Use relative path (e.g., just /api/government_terms)
        : 'https://api.example.com'; // Placeholder for deployed URL (You MUST replace this with your actual Azure URL)
    
    const API_ENDPOINT = '/api/government_terms';
    
    const [termsData, setTermsData] = useState([]);
    const [isLoading, setIsLoading] = useState(true);
    const [error, setError] = useState(null);

    // --- Data Types (for clarity) ---
    /**
     * @typedef {object} AffordabilityMetric
     * @property {number} year
     * @property {number} median_price
     * @property {number} median_income
     * @property {number} affordability_index
     * @property {number} interest_rate
     * @property {string} government_party
     * @property {number} gphi_score
     */
    
    /**
     * @typedef {object} GovernmentTermSummary
     * @property {string} party
     * @property {number} start_year
     * @property {number} end_year
     * @property {number} duration_years
     * @property {number} average_gphi_score
     * @property {AffordabilityMetric[]} annual_metrics
     */

    // --- Helper Functions ---
    const formatCurrency = (value) => {
        if (typeof value !== 'number') return 'N/A';
        return new Intl.NumberFormat('en-AU', {
            style: 'currency',
            currency: 'AUD',
            maximumFractionDigits: 0,
        }).format(value);
    };

    const formatGPHIScore = (score) => {
        if (typeof score !== 'number') return 'N/A';
        return score.toFixed(2);
    };
    
    const getPartyColor = (party) => {
        return party.toLowerCase() === 'labor' 
            ? 'bg-red-700 hover:bg-red-800 border-red-900' 
            : 'bg-blue-700 hover:bg-blue-800 border-blue-900';
    };
    
    const getChartColor = (party) => {
        return party.toLowerCase() === 'labor' ? '#dc2626' : '#2563eb'; // Tailwind 700 shades
    };

    // --- API Call and Data Fetching ---
    const fetchTermsData = useCallback(async () => {
        setIsLoading(true);
        setError(null);
        try {
            // Implement exponential backoff for robustness
            const maxRetries = 3;
            for (let attempt = 0; attempt < maxRetries; attempt++) {
                const url = `${API_BASE_URL}${API_ENDPOINT}`;
                console.log(`Fetching data from: ${url} (Attempt ${attempt + 1})`); // DEBUG LOG
                
                const response = await fetch(url);
                
                if (response.ok) {
                    const data = await response.json();
                    setTermsData(data);
                    return;
                }
                // If not OK, retry after delay
                if (attempt < maxRetries - 1) {
                    const delay = Math.pow(2, attempt) * 1000;
                    console.warn(`Fetch failed (Status: ${response.status}). Retrying in ${delay / 1000}s...`);
                    await new Promise(resolve => setTimeout(resolve, delay));
                }
            }
            throw new Error("Failed to fetch data after multiple retries.");

        } catch (err) {
            console.error("Error fetching data:", err);
            setError("Could not load data. Ensure the backend is running and accessible.");
        } finally {
            setIsLoading(false);
        }
    }, [API_BASE_URL, API_ENDPOINT]);

    useEffect(() => {
        fetchTermsData();
    }, [fetchTermsData]);

    // --- Components ---

    /**
     * Renders a card for a single government term.
     * @param {GovernmentTermSummary} term 
     */
    const TermCard = ({ term }) => {
        const partyColor = getPartyColor(term.party);
        const chartColor = getChartColor(term.party);
        
        // Calculate the starting GPHI and ending GPHI for the summary
        const startGPHI = term.annual_metrics[0]?.gphi_score;
        const endGPHI = term.annual_metrics[term.annual_metrics.length - 1]?.gphi_score;

        return (
            <div className={`shadow-2xl rounded-xl overflow-hidden mb-12 transform hover:scale-[1.01] transition-transform duration-300 ${partyColor} text-white`}>
                
                {/* Header (The Bracket) */}
                <div className="p-6 md:p-8 border-b border-white/20">
                    <h2 className="text-4xl font-extrabold tracking-tight mb-2 flex items-center">
                        {term.party.toUpperCase()}
                        <span className="text-lg font-medium ml-4 opacity-75">
                            {term.start_year} &mdash; {term.end_year}
                        </span>
                    </h2>
                    <p className="text-sm opacity-80 italic">
                        {term.duration_years} years in government
                    </p>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 p-6 md:p-8 bg-white/10">
                    
                    {/* Summary Metrics */}
                    <div className="lg:col-span-1 space-y-4">
                        <div className="p-4 bg-white/20 rounded-lg">
                            <p className="text-xl font-bold">Average GPHI Score</p>
                            <p className="text-5xl font-extrabold text-yellow-300">
                                {formatGPHIScore(term.average_gphi_score)}
                            </p>
                            <p className="text-sm opacity-75 mt-2">
                                (Lower score indicates worse perceived performance on housing.)
                            </p>
                        </div>
                        
                        <SummaryStat 
                            title="Starting GPHI" 
                            value={formatGPHIScore(startGPHI)} 
                            year={term.start_year} 
                        />
                        <SummaryStat 
                            title="Ending GPHI" 
                            value={formatGPHIScore(endGPHI)} 
                            year={term.end_year} 
                        />
                    </div>
                    
                    {/* GPHI Trend Chart */}
                    <div className="lg:col-span-2 bg-white rounded-lg p-4 shadow-xl h-96">
                        <h3 className="text-xl font-semibold text-gray-800 mb-4">GPHI Score Trend during Term</h3>
                        <ResponsiveContainer width="100%" height="80%">
                            <LineChart data={term.annual_metrics} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                                <CartesianGrid strokeDasharray="3 3" stroke="#e0e0e0" />
                                <XAxis 
                                    dataKey="year" 
                                    tick={{ fill: '#4b5563', fontSize: 12 }} 
                                    padding={{ left: 10, right: 10 }} 
                                />
                                <YAxis 
                                    domain={['auto', 'auto']}
                                    tick={{ fill: '#4b5563', fontSize: 12 }} 
                                />
                                <Tooltip content={<CustomTooltip />} />
                                <Line 
                                    type="monotone" 
                                    dataKey="gphi_score" 
                                    stroke={chartColor} 
                                    strokeWidth={3}
                                    dot={{ stroke: chartColor, strokeWidth: 2, r: 4 }}
                                    activeDot={{ r: 8 }} 
                                />
                            </LineChart>
                        </ResponsiveContainer>
                        <p className="text-xs text-center text-gray-500 mt-2">Year</p>
                    </div>
                </div>

                {/* Annual Data Table (Collapsible - Optional for future) */}
                {/* Leaving out the detailed table for initial clarity, focusing on the summary and chart */}
            </div>
        );
    };

    const SummaryStat = ({ title, value, year }) => (
        <div className="p-4 bg-white/10 rounded-lg">
            <p className="text-sm opacity-75">{title}</p>
            <p className="text-3xl font-bold">{value}</p>
            <p className="text-xs opacity-60 mt-1">in {year}</p>
        </div>
    );

    const CustomTooltip = ({ active, payload, label }) => {
        if (active && payload && payload.length) {
            const data = payload[0].payload;
            return (
                <div className="p-4 bg-white border border-gray-200 rounded-lg shadow-lg text-gray-800 text-sm">
                    <p className="font-bold text-lg mb-1">{`Year: ${label}`}</p>
                    <p>GPHI Score: <span className="font-semibold">{formatGPHIScore(data.gphi_score)}</span></p>
                    <p>Median Price: <span className="font-semibold">{formatCurrency(data.median_price)}</span></p>
                    <p>Median Income: <span className="font-semibold">{formatCurrency(data.median_income)}</span></p>
                    <p>Interest Rate: <span className="font-semibold">{data.interest_rate.toFixed(1)}%</span></p>
                </div>
            );
        }
        return null;
    };

    // --- Main Render Logic ---
    if (isLoading) {
        return (
            <div className="flex items-center justify-center min-h-screen bg-gray-50 p-8">
                <div className="text-center p-8 bg-white shadow-xl rounded-xl">
                    <svg className="animate-spin h-8 w-8 text-blue-600 mx-auto mb-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                    </svg>
                    <p className="text-lg font-medium text-gray-700">Loading historical data by government term...</p>
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div className="flex items-center justify-center min-h-screen bg-red-50 p-8">
                <div className="text-center p-8 bg-white border-4 border-red-500 shadow-xl rounded-xl">
                    <p className="text-xl font-bold text-red-700 mb-4">Data Error</p>
                    <p className="text-gray-600">{error}</p>
                    <button 
                        onClick={fetchTermsData} 
                        className="mt-4 px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition"
                    >
                        Try Reloading
                    </button>
                </div>
            </div>
        );
    }
    
    return (
        <div className="min-h-screen bg-gray-50 font-sans">
            <header className="py-8 bg-white shadow-lg sticky top-0 z-10">
                <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8">
                    <h1 className="text-4xl font-extrabold text-gray-900 tracking-tight">
                        Housing Performance by Government Term
                    </h1>
                    <p className="mt-2 text-lg text-gray-500">
                        Comparing the average **GPHI Score** (Government Performance Housing Index) across major political eras (1980–2023).
                    </p>
                </div>
            </header>

            <main className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-12">
                <div className="space-y-12">
                    {termsData.map((term, index) => (
                        <TermCard key={index} term={term} />
                    ))}
                </div>
                
                <footer className="mt-16 text-center text-sm text-gray-500 py-8 border-t border-gray-200">
                    Data Source: Mock Historical Metrics (1980-2023). Visualization by Party Term.
                </footer>
            </main>
        </div>
    );
}

// Global Recharts definition for single-file use
const Recharts = { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer };
const { LineChart: RechartsLineChart, Line: RechartsLine, XAxis: RechartsXAxis, YAxis: RechartsYAxis, CartesianGrid: RechartsCartesianGrid, Tooltip: RechartsTooltip, ResponsiveContainer: RechartsResponsiveContainer } = Recharts;
window.Recharts = Recharts;