// src/services/api.js

const BASE_URL = 'http://127.0.0.1:8000/api/metrics/'; // Django backend URL for metrics endpoints

// --- Configuration for GenAI ---
const DEMO_USER_ID = 1001; // Use a specific User ID for the GenAI demonstration

// Fetch summary metrics (Total Revenue, Total Orders, etc.)
export const fetchSummaryMetrics = async () => {
    const response = await fetch(`${BASE_URL}summary/`);
    if (!response.ok) {
        throw new Error('Failed to fetch summary metrics');
    }
    return response.json();
};

// Fetch top products list
export const fetchTopProducts = async () => {
    const response = await fetch(`${BASE_URL}top-products/`);
    if (!response.ok) {
        throw new Error('Failed to fetch top products');
    }
    return response.json();
};

// Fetch fraud alerts
export const fetchFraudAlerts = async () => {
    const response = await fetch(`${BASE_URL}fraud-alerts/`);
    if (!response.ok) {
        throw new Error('Failed to fetch fraud alerts');
    }
    return response.json();
};

// --- NEW FUNCTION: Fetch Personalized Recommendations ---
export const fetchRecommendations = async () => {
    // Calls the Django endpoint: /api/metrics/recommend/1001/
    const response = await fetch(`${BASE_URL}recommend/${DEMO_USER_ID}/`); 
    if (!response.ok) {
        // Log the full response status for easier debugging
        console.error('GenAI Recommendation API Error:', response.status, response.statusText);
        // Fallback or throw an error
        return []; 
    }
    // Note: The GenAI endpoint returns a JSON array of recommendations
    return response.json(); 
};