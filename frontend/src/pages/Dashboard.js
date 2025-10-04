// src/pages/Dashboard.js
import React, { useState, useEffect } from 'react';
import MetricCard from '../components/MetricCard';
import ProductList from '../components/ProductList';
import FraudAlert from '../components/FraudAlert';
// Import the new fetch function for recommendations
import { fetchSummaryMetrics, fetchTopProducts, fetchFraudAlerts, fetchRecommendations } from '../services/api';

const Dashboard = () => {
    const [summary, setSummary] = useState(null);
    const [products, setProducts] = useState([]);
    const [alerts, setAlerts] = useState([]);
    const [recommendations, setRecommendations] = useState([]); // <-- NEW STATE for GenAI
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    // Function to fetch ALL data
    const fetchData = async () => {
        try {
            // Include fetchRecommendations() in the parallel fetch
            const [summaryData, productsData, alertsData, recsData] = await Promise.all([
                fetchSummaryMetrics(),
                fetchTopProducts(),
                fetchFraudAlerts(),
                fetchRecommendations() // <-- NEW FETCH
            ]);
            
            setSummary(summaryData);
            setProducts(productsData.slice(0, 5)); // Show top 5 products
            setAlerts(alertsData.slice(0, 3)); // Show top 3 alerts
            setRecommendations(recsData); // <-- SET NEW STATE
            setError(null);

        } catch (err) {
            console.error('API Fetch Error:', err);
            setError('Failed to connect to Django API or fetch data. Is the backend running?');
        } finally {
            setLoading(false);
        }
    };

    // useEffect hook to set up the polling interval
    useEffect(() => {
        fetchData(); // Fetch immediately on load

        // Set up polling (5-second interval to match Spark's trigger)
        // NOTE: The recommendations won't change every 5s, but we fetch them with the dashboard refresh.
        const intervalId = setInterval(fetchData, 5000); 

        // Cleanup function to clear the interval when the component unmounts
        return () => clearInterval(intervalId); 
    }, []);

    if (loading && !summary) return <div className="p-8 text-center text-lg">Loading Real-Time Dashboard...</div>;
    if (error) return <div className="p-8 text-center text-red-600 font-bold">{error}</div>;

    return (
        <div className="min-h-screen bg-gray-100 p-8">
            <h1 className="text-3xl font-bold text-gray-800 mb-6 border-b pb-2">
                Real-Time E-commerce Analytics Dashboard
            </h1>

            {/* --- Metric Cards --- */}
            <div className="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
                <MetricCard 
                    title="Total Revenue" 
                    value={summary?.total_revenue || '0.00'} 
                    unit="$"
                    description="Real-time revenue tracking"
                />
                <MetricCard 
                    title="Total Orders" 
                    value={summary?.total_orders || '0'} 
                    description="Orders processed successfully"
                />
                <MetricCard 
                    title="Avg Order Value" 
                    value={summary?.avg_order_value || '0.00'} 
                    unit="$"
                    description="Average spending per order"
                />
                <MetricCard 
                    title="Total Events" 
                    value={summary?.total_events || '0'} 
                    description="Raw events processed by Spark"
                />
            </div>
            
            {/* --- NEW: Generative AI Recommendations Section --- */}
            <div className="bg-white p-6 rounded-lg shadow-md mb-8">
                <h2 className="text-xl font-bold text-indigo-700 mb-4">
                    ✨ Personalized Recommendations (User 1001)
                </h2>
                <div className="flex space-x-4">
                    {recommendations.length > 0 ? (
                        recommendations.map((rec, index) => (
                            <div key={index} className="p-4 border border-indigo-200 rounded-lg w-1/3 bg-indigo-50">
                                <p className="font-semibold text-lg text-indigo-800">{rec.item}</p>
                                <p className="text-sm text-gray-600 mt-1">Why: {rec.reason || 'AI Insight.'}</p>
                            </div>
                        ))
                    ) : (
                        <p className="text-gray-500">Generating initial recommendations or AI is offline...</p>
                    )}
                </div>
            </div>


            {/* --- List/Alerts Section --- */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                <div className="lg:col-span-2">
                    <ProductList products={products} />
                </div>
                <div>
                    <FraudAlert alerts={alerts} />
                </div>
            </div>
            
            <p className="mt-8 text-center text-sm text-gray-500">Data updates every 5 seconds (matched to Spark micro-batch trigger).</p>
        </div>
    );
};

export default Dashboard;