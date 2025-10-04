// src/components/ProductList.js

import React from 'react';

const ProductList = ({ products }) => {
    return (
        <div className="bg-white p-6 rounded-lg shadow-md lg:col-span-2">
            <h2 className="text-lg font-semibold text-gray-800 mb-2">Top Products</h2>
            <p className="text-sm text-gray-500 mb-4">Best performing products by revenue.</p>
            <div className="space-y-4">
                {products.length === 0 ? (
                    <p className="text-gray-500">No product data available yet.</p>
                ) : (
                    products.map((product, index) => (
                        <div key={index} className="flex items-center justify-between border-b pb-3 pt-3">
                            {/* Product Name and Rank (Matching Screenshot Style) */}
                            <div className="flex items-center space-x-3">
                                <span className={`w-8 h-8 flex items-center justify-center text-lg font-bold rounded-full 
                                    ${index === 0 ? 'bg-orange-400 text-white' : index === 1 ? 'bg-gray-300' : 'bg-gray-200 text-gray-700'}`}>
                                    {index + 1}
                                </span>
                                <span className="font-medium text-gray-700">{product.product_name}</span>
                            </div>
                            
                            {/* Quantity and Sales Value */}
                            <div className="text-right">
                                <p className="font-semibold text-gray-900">${product.total_sales.toFixed(2)}</p>
                                <p className="text-sm text-gray-500">Qty: {product.total_orders}</p>
                            </div>
                        </div>
                    ))
                )}
            </div>
        </div>
    );
};

export default ProductList;