"""
Data generator for creating realistic, continuous, configurable mock data.
"""

import random
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass
import psycopg2
from faker import Faker

from config import config

fake = Faker()

logger = logging.getLogger(__name__)


@dataclass
class GeneratorStats:
    """Statistics for data generation."""
    customers_created: int = 0
    products_created: int = 0
    orders_created: int = 0
    order_items_created: int = 0
    start_time: Optional[datetime] = None
    
    def to_dict(self) -> Dict:
        return {
            "customers_created": self.customers_created,
            "products_created": self.products_created,
            "orders_created": self.orders_created,
            "order_items_created": self.order_items_created,
            "runtime_seconds": (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        }


class DataGenerator:
    """Generate realistic mock data for the ETL system."""
    
    def __init__(self):
        self.connection_string = config.database.connection_string
        self.stats = GeneratorStats(start_time=datetime.now())
        self.product_categories = [
            "Electronics", "Clothing", "Books", "Home & Garden", 
            "Sports", "Toys", "Beauty", "Automotive", "Food", "Health"
        ]
        self.order_statuses = ["pending", "processing", "shipped", "delivered", "cancelled"]
        
    def get_connection(self):
        """Get database connection."""
        return psycopg2.connect(self.connection_string)
    
    def generate_customer(self) -> Dict:
        """Generate a realistic customer record."""
        return {
            "name": fake.name(),
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "address": fake.address().replace('\n', ', ')
        }
    
    def generate_product(self) -> Dict:
        """Generate a realistic product record."""
        category = random.choice(self.product_categories)
        
        # Category-specific product names and pricing
        if category == "Electronics":
            names = ["Smartphone", "Laptop", "Tablet", "Headphones", "Speaker", "Smartwatch"]
            price_range = (50, 2000)
        elif category == "Clothing":
            names = ["T-Shirt", "Jeans", "Dress", "Jacket", "Sneakers", "Hat"]
            price_range = (20, 300)
        elif category == "Books":
            names = ["Novel", "Textbook", "Cookbook", "Biography", "Manual", "Dictionary"]
            price_range = (10, 100)
        else:
            names = [f"{category} Item", f"Premium {category}", f"Basic {category}"]
            price_range = (15, 500)
        
        name = f"{random.choice(names)} {fake.word().title()}"
        price = round(random.uniform(*price_range), 2)
        stock = random.randint(0, 500)
        
        return {
            "name": name,
            "category": category,
            "price": price,
            "stock_quantity": stock
        }
    
    def create_customer(self, conn) -> int:
        """Create a customer record and return the ID."""
        customer_data = self.generate_customer()
        
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO etl_demo.customers (name, email, phone, address)
                VALUES (%(name)s, %(email)s, %(phone)s, %(address)s)
                RETURNING id
            """, customer_data)
            customer_id = cursor.fetchone()[0]
            conn.commit()
            
        self.stats.customers_created += 1
        logger.debug(f"Created customer {customer_id}: {customer_data['name']}")
        return customer_id
    
    def create_product(self, conn) -> int:
        """Create a product record and return the ID."""
        product_data = self.generate_product()
        
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO etl_demo.products (name, category, price, stock_quantity)
                VALUES (%(name)s, %(category)s, %(price)s, %(stock_quantity)s)
                RETURNING id
            """, product_data)
            product_id = cursor.fetchone()[0]
            conn.commit()
            
        self.stats.products_created += 1
        logger.debug(f"Created product {product_id}: {product_data['name']}")
        return product_id
    
    def get_random_customer_id(self, conn) -> Optional[int]:
        """Get a random existing customer ID."""
        with conn.cursor() as cursor:
            cursor.execute("SELECT id FROM etl_demo.customers ORDER BY RANDOM() LIMIT 1")
            result = cursor.fetchone()
            return result[0] if result else None
    
    def get_random_product_ids(self, conn, count: int = None) -> List[int]:
        """Get random existing product IDs."""
        if count is None:
            count = random.randint(1, 5)
            
        with conn.cursor() as cursor:
            cursor.execute(f"SELECT id FROM etl_demo.products ORDER BY RANDOM() LIMIT {count}")
            return [row[0] for row in cursor.fetchall()]
    
    def create_order_with_items(self, conn) -> int:
        """Create an order with items and return the order ID."""
        customer_id = self.get_random_customer_id(conn)
        if not customer_id:
            # Create a customer if none exist
            customer_id = self.create_customer(conn)
        
        product_ids = self.get_random_product_ids(conn)
        if not product_ids:
            # Create some products if none exist
            for _ in range(5):
                self.create_product(conn)
            product_ids = self.get_random_product_ids(conn)
        
        # Create order
        status = random.choice(self.order_statuses)
        # Simulate order dates within the last 30 days
        order_date = fake.date_time_between(start_date='-30d', end_date='now')
        
        total_amount = 0
        order_items = []
        
        # Get product prices and create order items
        with conn.cursor() as cursor:
            for product_id in product_ids:
                cursor.execute("SELECT price FROM etl_demo.products WHERE id = %s", (product_id,))
                result = cursor.fetchone()
                if result:
                    unit_price = result[0]
                    quantity = random.randint(1, 10)
                    item_total = unit_price * quantity
                    total_amount += item_total
                    
                    order_items.append({
                        "product_id": product_id,
                        "quantity": quantity,
                        "unit_price": unit_price
                    })
        
        # Insert order
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO etl_demo.orders (customer_id, order_date, total_amount, status)
                VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (customer_id, order_date, total_amount, status))
            order_id = cursor.fetchone()[0]
            
            # Insert order items
            for item in order_items:
                cursor.execute("""
                    INSERT INTO etl_demo.order_items (order_id, product_id, quantity, unit_price)
                    VALUES (%s, %s, %s, %s)
                """, (order_id, item["product_id"], item["quantity"], item["unit_price"]))
                self.stats.order_items_created += 1
            
            conn.commit()
        
        self.stats.orders_created += 1
        logger.debug(f"Created order {order_id} with {len(order_items)} items for customer {customer_id}")
        return order_id
    
    def update_random_customer(self, conn):
        """Update a random customer to trigger CDC."""
        with conn.cursor() as cursor:
            # Get random customer
            cursor.execute("SELECT id FROM etl_demo.customers ORDER BY RANDOM() LIMIT 1")
            result = cursor.fetchone()
            if result:
                customer_id = result[0]
                new_phone = fake.phone_number()
                cursor.execute("""
                    UPDATE etl_demo.customers 
                    SET phone = %s, updated_at = CURRENT_TIMESTAMP 
                    WHERE id = %s
                """, (new_phone, customer_id))
                conn.commit()
                logger.debug(f"Updated customer {customer_id} phone to {new_phone}")
    
    def update_random_product(self, conn):
        """Update a random product to trigger CDC."""
        with conn.cursor() as cursor:
            # Get random product
            cursor.execute("SELECT id FROM etl_demo.products ORDER BY RANDOM() LIMIT 1")
            result = cursor.fetchone()
            if result:
                product_id = result[0]
                # Randomly increase or decrease stock
                stock_change = random.randint(-50, 100)
                cursor.execute("""
                    UPDATE etl_demo.products 
                    SET stock_quantity = GREATEST(0, stock_quantity + %s), 
                        updated_at = CURRENT_TIMESTAMP 
                    WHERE id = %s
                """, (stock_change, product_id))
                conn.commit()
                logger.debug(f"Updated product {product_id} stock by {stock_change}")
    
    def generate_continuous_data(self, duration_minutes: int = 60):
        """Generate continuous data for a specified duration."""
        logger.info(f"Starting continuous data generation for {duration_minutes} minutes")
        
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        
        try:
            with self.get_connection() as conn:
                # Initial batch of customers and products
                for _ in range(10):
                    self.create_customer(conn)
                for _ in range(20):
                    self.create_product(conn)
                
                while datetime.now() < end_time:
                    # Generate various activities
                    activity = random.choices(
                        ['create_order', 'create_customer', 'create_product', 'update_customer', 'update_product'],
                        weights=[50, 10, 15, 15, 10],
                        k=1
                    )[0]
                    
                    try:
                        if activity == 'create_order':
                            self.create_order_with_items(conn)
                        elif activity == 'create_customer':
                            self.create_customer(conn)
                        elif activity == 'create_product':
                            self.create_product(conn)
                        elif activity == 'update_customer':
                            self.update_random_customer(conn)
                        elif activity == 'update_product':
                            self.update_random_product(conn)
                    except Exception as e:
                        logger.error(f"Error during {activity}: {e}")
                        continue
                    
                    # Wait before next activity
                    time.sleep(config.processing.data_generator_interval)
                    
                    # Log stats periodically
                    if self.stats.orders_created % 10 == 0:
                        logger.info(f"Generated data: {self.stats.to_dict()}")
        
        except Exception as e:
            logger.error(f"Error in continuous data generation: {e}")
        finally:
            logger.info(f"Data generation completed. Final stats: {self.stats.to_dict()}")
    
    def generate_batch_data(self, 
                          customers: int = 50, 
                          products: int = 100, 
                          orders: int = 200):
        """Generate a batch of data."""
        logger.info(f"Generating batch data: {customers} customers, {products} products, {orders} orders")
        
        try:
            with self.get_connection() as conn:
                # Create customers
                for _ in range(customers):
                    self.create_customer(conn)
                
                # Create products
                for _ in range(products):
                    self.create_product(conn)
                
                # Create orders
                for _ in range(orders):
                    self.create_order_with_items(conn)
        
        except Exception as e:
            logger.error(f"Error in batch data generation: {e}")
        
        logger.info(f"Batch data generation completed. Stats: {self.stats.to_dict()}")


def main():
    """Main function for running the data generator."""
    import argparse
    
    parser = argparse.ArgumentParser(description="ETL Data Generator")
    parser.add_argument("--mode", choices=["batch", "continuous"], default="continuous",
                      help="Generation mode")
    parser.add_argument("--duration", type=int, default=60,
                      help="Duration in minutes for continuous mode")
    parser.add_argument("--customers", type=int, default=50,
                      help="Number of customers for batch mode")
    parser.add_argument("--products", type=int, default=100,
                      help="Number of products for batch mode")
    parser.add_argument("--orders", type=int, default=200,
                      help="Number of orders for batch mode")
    parser.add_argument("--log-level", default="INFO",
                      help="Logging level")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    generator = DataGenerator()
    
    if args.mode == "continuous":
        generator.generate_continuous_data(duration_minutes=args.duration)
    else:
        generator.generate_batch_data(
            customers=args.customers,
            products=args.products,
            orders=args.orders
        )


if __name__ == "__main__":
    main()