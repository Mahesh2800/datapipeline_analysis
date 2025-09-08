# database_writer.py
import psycopg2

def insert_order(order):
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname="ecommerse_db",
            user="postgres",
            password="123",
            host="localhost",   # or "db" if using Docker
            port="5432"
        )
        cursor = conn.cursor()

        # Insert into the same table as Django (orders_order)
        cursor.execute(
            """
            INSERT INTO orders_order (order_id, user_id, product_name, price)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (order_id) DO NOTHING
            """,
            (order['order_id'], order['user_id'], order['product_name'], order['price'])
        )

        conn.commit()
        cursor.close()
        conn.close()
        print(f"✅ Order inserted into DB: {order}")

    except Exception as e:
        print(f"❌ Error inserting order: {e}")
