# orders/views.py

from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework.exceptions import APIException
from django.db import connection
from decimal import Decimal
import json
import os
import random # Used for fallback recommendations

# --- Import GenAI SDK (Ensure google-genai is installed) ---
try:
    from google import genai
    # Get API key from environment variable
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
    if GEMINI_API_KEY:
        genai_client = genai.Client(api_key=GEMINI_API_KEY)
    else:
        genai_client = None
except ImportError:
    genai_client = None
except Exception as e:
    print(f"GenAI Client Initialization Error: {e}")
    genai_client = None


# --- Kafka Producer Initialization ---
from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ----------------------------------------------------
#                   UTILITY FUNCTIONS FOR GENAI
# ----------------------------------------------------

def get_user_history_and_catalog(user_id):
    """Fetches a user's spending data and the global product catalog."""
    
    with connection.cursor() as cursor:
        # 1. Fetch User Analytics (Assuming user_analytics table exists)
        cursor.execute("SELECT total_spent, total_orders, user_segment FROM user_analytics WHERE user_id = %s;", [user_id])
        user_data = cursor.fetchone()
        
        # 2. Fetch User's Recent Purchases (Top 3 from orders_processed)
        cursor.execute("""
            SELECT product_name, COUNT(*) as num_purchases 
            FROM orders_processed 
            WHERE user_id = %s 
            GROUP BY product_name 
            ORDER BY num_purchases DESC 
            LIMIT 3;
        """, [user_id])
        recent_purchases = cursor.fetchall()
        
        # 3. Fetch Global Product Catalog (for possible recommendations)
        cursor.execute("SELECT product_name FROM product_analytics;")
        catalog = [row[0] for row in cursor.fetchall() if row[0]] # Filter out any NULLs

    return user_data, recent_purchases, catalog

# ----------------------------------------------------
#                   STANDARD API ENDPOINTS
# ----------------------------------------------------

@api_view(['GET'])
def api_root(request):
    """Root API endpoint for testing connection."""
    return Response({"message": "Welcome to the E-commerce Data Pipeline API"})

@api_view(['POST'])
def create_order(request):
    """Handles order creation and publishes event to Kafka."""
    # NOTE: Assuming OrderSerializer and Order model are defined elsewhere.
    data = request.data.copy()
    
    # Using raw data for Kafka send since model save logic is not fully provided:
    order_id = data.get("order_id")
    order_price = data.get("price")
    
    try:
        producer.send('orders', {
            "order_id": order_id,
            "user_id": data.get("user_id"),
            "product_name": data.get("product_name"),
            "price": str(order_price),
        })
        producer.flush()
        return Response({"status": "Order created and sent to Kafka", "order_id": order_id}, status=201)
    except Exception as e:
        print(f"Kafka Send Error: {e}")
        return Response({"error": "Failed to send event to Kafka", "details": str(e)}, status=500)


@api_view(['GET'])
def get_summary_metrics(request):
    """Fetches high-level aggregated metrics."""
    # ... (existing code, unchanged) ...
    try:
        with connection.cursor() as cursor:
            # Query 1: Fetching Total Events from raw table
            cursor.execute("SELECT COUNT(*) FROM orders_raw;")
            total_events = cursor.fetchone()[0]
            
            # Query 2: Current Totals (aggregating all processed data)
            cursor.execute("""
                SELECT 
                    COALESCE(SUM(total_revenue), 0) AS total_revenue, 
                    COALESCE(SUM(total_orders), 0) AS total_orders, 
                    COALESCE(AVG(avg_order_value), 0) AS avg_order_value
                FROM hourly_sales;
            """)
            summary_data = cursor.fetchone()
            
            total_revenue = Decimal(summary_data[0]) if summary_data[0] is not None else Decimal(0)
            total_orders = summary_data[1] if summary_data[1] is not None else 0
            avg_order_value = Decimal(summary_data[2]) if summary_data[2] is not None else Decimal(0)
                
            return Response({
                "total_revenue": f"{total_revenue:,.2f}",
                "total_orders": total_orders,
                "avg_order_value": f"{avg_order_value:,.2f}",
                "total_events": total_events
            })

    except Exception as e:
        print(f"Database error in get_summary_metrics: {e}")
        raise APIException(f"Could not fetch summary data: {e}")


@api_view(['GET'])
def get_top_products(request):
    """Fetches the Top Products list."""
    # ... (existing code, unchanged) ...
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT 
                    product_name, 
                    total_orders, 
                    total_sales
                FROM top_products;
            """)
            
            columns = [col[0] for col in cursor.description]
            products = [
                dict(zip(columns, (row[0], row[1], float(row[2]) if row[2] else 0.0)))
                for row in cursor.fetchall()
            ]

            return Response(products)

    except Exception as e:
        print(f"Database error in get_top_products: {e}")
        raise APIException(f"Could not fetch top products: {e}")


@api_view(['GET'])
def get_fraud_alerts(request):
    """Mocks fraud alert data."""
    # ... (existing code, unchanged) ...
    alerts = [
        {"id": 1, "order_id": 1003, "value": "2100.50", "timestamp": "5:04 PM", "level": "medium", "reason": "High single-order value."},
        {"id": 2, "order_id": 1004, "value": "3559.90", "timestamp": "5:05 PM", "level": "high", "reason": "New user, large transaction."},
        {"id": 3, "order_id": 1005, "value": "3339.93", "timestamp": "5:06 PM", "level": "high", "reason": "Multiple large orders in 5 mins."},
    ]
    return Response(alerts)

# ----------------------------------------------------
#               GENERATIVE AI ENDPOINT (NEW)
# ----------------------------------------------------

@api_view(['GET'])
def get_recommendations(request, user_id):
    """
    Generates personalized product recommendations using Generative AI.
    """
    user_data, recent_purchases, catalog = get_user_history_and_catalog(user_id)
    
    # Fallback in case GenAI is not configured or key is missing
    if not genai_client or not catalog:
        # Provide a randomized fallback using existing products
        fallback_products = random.sample(catalog, min(3, len(catalog))) if catalog else ["Laptop", "Smart Watch", "Shoes"]
        return Response([
            {"item": p, "reason": "Default best-seller recommendation (AI offline)."} for p in fallback_products
        ], status=200)

    # Convert user data to readable prompt format
    history_str = ", ".join([f"{name} ({count}x)" for name, count in recent_purchases])
    catalog_str = ", ".join(catalog)
    
    prompt = f"""
    You are an AI personalization engine for an e-commerce platform.
    Analyze the customer's profile and recommend exactly 3 products from the AVAILABLE CATALOG.
    
    USER PROFILE:
    - User ID: {user_id}
    - Total Spent: ${user_data[0]:.2f}
    - Total Orders: {user_data[1]}
    - Segment: {user_data[2]}
    - Recent Purchases: {history_str if history_str else 'None'}

    AVAILABLE CATALOG: {catalog_str}

    Your output MUST be a clean, simple JSON array of 3 objects, ONLY containing the recommended items.
    Format example: [ {{"item": "Recommended Product Name", "reason": "Short reason for recommendation"}}, ... ]
    """

    try:
        response = genai_client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt
        )
        
        # The model should output valid JSON text
        recommendations = json.loads(response.text.strip())
        
        return Response(recommendations, status=200)
        
    except Exception as e:
        print(f"Gemini API or Parsing Error: {e}")
        # Return a fallback response on failure
        return Response([
            {"item": "Default Best Seller", "reason": "AI service unavailable."}
        ], status=200)