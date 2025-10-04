// src/components/MetricCard.js

import React from 'react';

const MetricCard = ({ title, value, unit = '', description }) => {
    // Determine the color based on the metric for visual distinction
    const color = title.includes('Revenue') ? 'border-green-500' 
                : title.includes('Orders') ? 'border-blue-500' 
                : title.includes('Avg') ? 'border-purple-500'
                : 'border-gray-500';

    return (
        <div className={`bg-white p-6 rounded-lg shadow-md border-t-4 ${color}`}>
            <h3 className="text-sm font-medium text-gray-500">{title}</h3>
            <div className="mt-1 flex items-center justify-between">
                <p className="text-3xl font-bold text-gray-900">
                    {unit}{value}
                </p>
                {/* Standard Icon - Set to w-8 h-8 */}
                <svg className="w-8 h-8 text-indigo-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
                </svg>
            </div>
            <p className="mt-2 text-sm text-gray-500">{description}</p>
        </div>
    );
};

export default MetricCard;