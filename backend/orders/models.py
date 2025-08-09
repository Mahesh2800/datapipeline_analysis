from django.db import models

class Order(models.Model):
    order_id = models.IntegerField(primary_key=True)  # Tell Django this is the PK
    user_id = models.IntegerField()
    product_name = models.CharField(max_length=100)
    price = models.DecimalField(max_digits=10, decimal_places=2)

    class Meta:
        db_table = 'orders_order'  # Tell Django the exact table name in PostgreSQL

    def __str__(self):
        return f"Order {self.order_id} - {self.product_name}"
