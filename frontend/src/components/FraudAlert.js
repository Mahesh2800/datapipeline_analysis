// src/components/FraudAlert.js

import React from 'react';

const FraudAlert = ({ alerts }) => {
    const getLevelClasses = (level) => {
        if (level === 'high') return 'bg-red-200 text-red-800 border-red-500';
        if (level === 'medium') return 'bg-yellow-200 text-yellow-800 border-yellow-500';
        return 'bg-gray-200 text-gray-700 border-gray-500';
    };

    return (
        <div className="bg-white p-6 rounded-lg shadow-md border-t-4 border-red-500">
            <h2 className="text-lg font-semibold text-gray-800 mb-2">Fraud Alerts</h2>
            <p className="text-sm text-gray-500 mb-4">Real-time anomaly detection based on rules.</p>
            <div className="space-y-4">
                {alerts.length === 0 ? (
                    <p className="text-gray-500">No active alerts.</p>
                ) : (
                    alerts.map(alert => (
                        <div key={alert.id} className="p-3 border rounded-lg flex justify-between items-start space-x-4 
                                            bg-yellow-50 border-yellow-300">
                            
                            {/* Alert Icon and Details */}
                            <div className="flex items-start space-x-2">
                                <svg className="w-5 h-5 text-yellow-600 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.3 16c-.77 1.333.192 3 1.732 3z" />
                                </svg>
                                <div>
                                    <p className="font-bold text-sm text-gray-800">High value order: ${alert.value}</p>
                                    <p className="text-xs text-gray-600">{alert.timestamp}</p>
                                </div>
                            </div>

                            {/* Level Badge */}
                            <span className={`px-2 py-0.5 text-xs font-semibold rounded-full ${getLevelClasses(alert.level)}`}>
                                {alert.level}
                            </span>
                        </div>
                    ))
                )}
            </div>
        </div>
    );
};

export default FraudAlert;