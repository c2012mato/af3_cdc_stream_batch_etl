"""
Batch Processor for ETL system.
Handles 60-second intervals, customer segmentation, product analytics, and revenue analysis.
"""

import logging
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, asdict
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor

from config import config

logger = logging.getLogger(__name__)


@dataclass
class BatchMetrics:
    """Metrics for batch processing."""
    batches_processed: int = 0
    customers_segmented: int = 0
    products_analyzed: int = 0
    revenue_records_created: int = 0
    total_processing_time: float = 0.0
    start_time: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        runtime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        return {
            **asdict(self),
            "runtime_seconds": runtime,
            "avg_batch_time_seconds": (self.total_processing_time / self.batches_processed) 
                                    if self.batches_processed > 0 else 0
        }


@dataclass
class CustomerSegment:
    """Customer segmentation data."""
    customer_id: int
    segment: str
    total_orders: int
    total_spent: float
    avg_order_value: float
    last_order_date: Optional[datetime]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "customer_id": self.customer_id,
            "segment": self.segment,
            "total_orders": self.total_orders,
            "total_spent": float(self.total_spent),
            "avg_order_value": float(self.avg_order_value),
            "last_order_date": self.last_order_date.isoformat() if self.last_order_date else None
        }


@dataclass
class ProductAnalytics:
    """Product analytics data."""
    product_id: int
    date_analyzed: datetime
    units_sold: int
    revenue: float
    avg_selling_price: float
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "product_id": self.product_id,
            "date_analyzed": self.date_analyzed.date().isoformat(),
            "units_sold": self.units_sold,
            "revenue": float(self.revenue),
            "avg_selling_price": float(self.avg_selling_price)
        }


@dataclass
class RevenueSummary:
    """Revenue summary data."""
    date_analyzed: datetime
    total_revenue: float
    total_orders: int
    avg_order_value: float
    unique_customers: int
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "date_analyzed": self.date_analyzed.date().isoformat(),
            "total_revenue": float(self.total_revenue),
            "total_orders": self.total_orders,
            "avg_order_value": float(self.avg_order_value),
            "unique_customers": self.unique_customers
        }


class BatchProcessor:
    """Process data in batches for analytics and reporting."""
    
    def __init__(self):
        self.database_config = config.database
        self.processing_config = config.processing
        self.metrics = BatchMetrics(start_time=datetime.now())
        
        # Segment thresholds
        self.segment_thresholds = {
            "vip": {"min_spent": 5000, "min_orders": 20},
            "regular": {"min_spent": 500, "min_orders": 5},
            "new": {"min_spent": 0, "min_orders": 1}
        }
    
    def get_database_connection(self):
        """Get database connection."""
        return psycopg2.connect(
            self.database_config.connection_string,
            cursor_factory=RealDictCursor
        )
    
    def calculate_customer_segments(self) -> List[CustomerSegment]:
        """Calculate customer segmentation based on purchase behavior."""
        logger.info("Calculating customer segments")
        
        segments = []
        
        try:
            with self.get_database_connection() as conn:
                with conn.cursor() as cursor:
                    # Get customer statistics
                    cursor.execute("""
                        SELECT 
                            c.id as customer_id,
                            c.name as customer_name,
                            COUNT(o.id) as total_orders,
                            COALESCE(SUM(o.total_amount), 0) as total_spent,
                            COALESCE(AVG(o.total_amount), 0) as avg_order_value,
                            MAX(o.order_date) as last_order_date
                        FROM etl_demo.customers c
                        LEFT JOIN etl_demo.orders o ON c.id = o.customer_id
                        GROUP BY c.id, c.name
                    """)
                    
                    customers = cursor.fetchall()
                    
                    for customer in customers:
                        # Determine segment
                        total_spent = float(customer['total_spent'] or 0)
                        total_orders = customer['total_orders'] or 0
                        
                        if (total_spent >= self.segment_thresholds["vip"]["min_spent"] and 
                            total_orders >= self.segment_thresholds["vip"]["min_orders"]):
                            segment = "vip"
                        elif (total_spent >= self.segment_thresholds["regular"]["min_spent"] and 
                              total_orders >= self.segment_thresholds["regular"]["min_orders"]):
                            segment = "regular"
                        elif total_orders >= self.segment_thresholds["new"]["min_orders"]:
                            segment = "new"
                        else:
                            segment = "inactive"
                        
                        segments.append(CustomerSegment(
                            customer_id=customer['customer_id'],
                            segment=segment,
                            total_orders=total_orders,
                            total_spent=total_spent,
                            avg_order_value=float(customer['avg_order_value'] or 0),
                            last_order_date=customer['last_order_date']
                        ))
        
        except Exception as e:
            logger.error(f"Error calculating customer segments: {e}")
        
        logger.info(f"Calculated {len(segments)} customer segments")
        return segments
    
    def save_customer_segments(self, segments: List[CustomerSegment]):
        """Save customer segments to database."""
        logger.info(f"Saving {len(segments)} customer segments")
        
        try:
            with self.get_database_connection() as conn:
                with conn.cursor() as cursor:
                    # Clear existing segments for update
                    cursor.execute("DELETE FROM etl_demo.customer_segments")
                    
                    # Insert new segments
                    for segment in segments:
                        cursor.execute("""
                            INSERT INTO etl_demo.customer_segments 
                            (customer_id, segment, total_orders, total_spent, avg_order_value, last_order_date)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (
                            segment.customer_id,
                            segment.segment,
                            segment.total_orders,
                            segment.total_spent,
                            segment.avg_order_value,
                            segment.last_order_date
                        ))
                    
                    conn.commit()
                    self.metrics.customers_segmented = len(segments)
        
        except Exception as e:
            logger.error(f"Error saving customer segments: {e}")
    
    def calculate_product_analytics(self, analysis_date: Optional[datetime] = None) -> List[ProductAnalytics]:
        """Calculate product analytics for a specific date."""
        if analysis_date is None:
            analysis_date = datetime.now().date()
        else:
            analysis_date = analysis_date.date()
        
        logger.info(f"Calculating product analytics for {analysis_date}")
        
        analytics = []
        
        try:
            with self.get_database_connection() as conn:
                with conn.cursor() as cursor:
                    # Get product sales data for the date
                    cursor.execute("""
                        SELECT 
                            p.id as product_id,
                            p.name as product_name,
                            COALESCE(SUM(oi.quantity), 0) as units_sold,
                            COALESCE(SUM(oi.quantity * oi.unit_price), 0) as revenue,
                            COALESCE(AVG(oi.unit_price), 0) as avg_selling_price
                        FROM etl_demo.products p
                        LEFT JOIN etl_demo.order_items oi ON p.id = oi.product_id
                        LEFT JOIN etl_demo.orders o ON oi.order_id = o.id
                        WHERE DATE(o.order_date) = %s OR o.order_date IS NULL
                        GROUP BY p.id, p.name
                    """, (analysis_date,))
                    
                    products = cursor.fetchall()
                    
                    for product in products:
                        analytics.append(ProductAnalytics(
                            product_id=product['product_id'],
                            date_analyzed=datetime.combine(analysis_date, datetime.min.time()),
                            units_sold=product['units_sold'] or 0,
                            revenue=float(product['revenue'] or 0),
                            avg_selling_price=float(product['avg_selling_price'] or 0)
                        ))
        
        except Exception as e:
            logger.error(f"Error calculating product analytics: {e}")
        
        logger.info(f"Calculated analytics for {len(analytics)} products")
        return analytics
    
    def save_product_analytics(self, analytics: List[ProductAnalytics]):
        """Save product analytics to database."""
        logger.info(f"Saving analytics for {len(analytics)} products")
        
        try:
            with self.get_database_connection() as conn:
                with conn.cursor() as cursor:
                    for analytic in analytics:
                        # Use upsert to handle duplicates
                        cursor.execute("""
                            INSERT INTO etl_demo.product_analytics 
                            (product_id, date_analyzed, units_sold, revenue, avg_selling_price)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (product_id, date_analyzed) 
                            DO UPDATE SET
                                units_sold = EXCLUDED.units_sold,
                                revenue = EXCLUDED.revenue,
                                avg_selling_price = EXCLUDED.avg_selling_price,
                                created_at = CURRENT_TIMESTAMP
                        """, (
                            analytic.product_id,
                            analytic.date_analyzed.date(),
                            analytic.units_sold,
                            analytic.revenue,
                            analytic.avg_selling_price
                        ))
                    
                    conn.commit()
                    self.metrics.products_analyzed = len(analytics)
        
        except Exception as e:
            logger.error(f"Error saving product analytics: {e}")
    
    def calculate_revenue_summary(self, analysis_date: Optional[datetime] = None) -> RevenueSummary:
        """Calculate revenue summary for a specific date."""
        if analysis_date is None:
            analysis_date = datetime.now().date()
        else:
            analysis_date = analysis_date.date()
        
        logger.info(f"Calculating revenue summary for {analysis_date}")
        
        try:
            with self.get_database_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT 
                            COALESCE(SUM(total_amount), 0) as total_revenue,
                            COUNT(*) as total_orders,
                            COALESCE(AVG(total_amount), 0) as avg_order_value,
                            COUNT(DISTINCT customer_id) as unique_customers
                        FROM etl_demo.orders
                        WHERE DATE(order_date) = %s
                    """, (analysis_date,))
                    
                    result = cursor.fetchone()
                    
                    if result:
                        return RevenueSummary(
                            date_analyzed=datetime.combine(analysis_date, datetime.min.time()),
                            total_revenue=float(result['total_revenue'] or 0),
                            total_orders=result['total_orders'] or 0,
                            avg_order_value=float(result['avg_order_value'] or 0),
                            unique_customers=result['unique_customers'] or 0
                        )
        
        except Exception as e:
            logger.error(f"Error calculating revenue summary: {e}")
        
        # Return empty summary on error
        return RevenueSummary(
            date_analyzed=datetime.combine(analysis_date, datetime.min.time()),
            total_revenue=0.0,
            total_orders=0,
            avg_order_value=0.0,
            unique_customers=0
        )
    
    def save_revenue_summary(self, summary: RevenueSummary):
        """Save revenue summary to database."""
        logger.info(f"Saving revenue summary for {summary.date_analyzed.date()}")
        
        try:
            with self.get_database_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        INSERT INTO etl_demo.revenue_summary 
                        (date_analyzed, total_revenue, total_orders, avg_order_value, unique_customers)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (date_analyzed) 
                        DO UPDATE SET
                            total_revenue = EXCLUDED.total_revenue,
                            total_orders = EXCLUDED.total_orders,
                            avg_order_value = EXCLUDED.avg_order_value,
                            unique_customers = EXCLUDED.unique_customers,
                            created_at = CURRENT_TIMESTAMP
                    """, (
                        summary.date_analyzed.date(),
                        summary.total_revenue,
                        summary.total_orders,
                        summary.avg_order_value,
                        summary.unique_customers
                    ))
                    
                    conn.commit()
                    self.metrics.revenue_records_created += 1
        
        except Exception as e:
            logger.error(f"Error saving revenue summary: {e}")
    
    def process_batch(self, analysis_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Process a single batch of analytics."""
        start_time = time.time()
        
        logger.info(f"Starting batch processing for {analysis_date or 'current date'}")
        
        results = {
            "analysis_date": (analysis_date or datetime.now()).isoformat(),
            "segments_processed": 0,
            "products_analyzed": 0,
            "revenue_summary_created": False,
            "processing_time_seconds": 0,
            "errors": []
        }
        
        try:
            # Calculate and save customer segments
            segments = self.calculate_customer_segments()
            self.save_customer_segments(segments)
            results["segments_processed"] = len(segments)
            
        except Exception as e:
            error_msg = f"Error processing customer segments: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
        
        try:
            # Calculate and save product analytics
            product_analytics = self.calculate_product_analytics(analysis_date)
            self.save_product_analytics(product_analytics)
            results["products_analyzed"] = len(product_analytics)
            
        except Exception as e:
            error_msg = f"Error processing product analytics: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
        
        try:
            # Calculate and save revenue summary
            revenue_summary = self.calculate_revenue_summary(analysis_date)
            self.save_revenue_summary(revenue_summary)
            results["revenue_summary_created"] = True
            
        except Exception as e:
            error_msg = f"Error processing revenue summary: {e}"
            logger.error(error_msg)
            results["errors"].append(error_msg)
        
        # Update metrics
        processing_time = time.time() - start_time
        self.metrics.batches_processed += 1
        self.metrics.total_processing_time += processing_time
        results["processing_time_seconds"] = processing_time
        
        logger.info(f"Batch processing completed in {processing_time:.2f} seconds")
        return results
    
    def run_continuous_processing(self, duration_minutes: int = 60):
        """Run continuous batch processing with configurable intervals."""
        logger.info(f"Starting continuous batch processing for {duration_minutes} minutes")
        logger.info(f"Batch interval: {self.processing_config.batch_interval_seconds} seconds")
        
        end_time = datetime.now() + timedelta(minutes=duration_minutes)
        
        try:
            while datetime.now() < end_time:
                # Process current batch
                batch_results = self.process_batch()
                
                # Log results
                logger.info(f"Batch results: {batch_results}")
                
                # Log current metrics
                if self.metrics.batches_processed % 5 == 0:
                    logger.info(f"Processing metrics: {self.metrics.to_dict()}")
                
                # Wait for next batch interval
                logger.debug(f"Waiting {self.processing_config.batch_interval_seconds} seconds for next batch")
                time.sleep(self.processing_config.batch_interval_seconds)
        
        except KeyboardInterrupt:
            logger.info("Batch processing interrupted by user")
        except Exception as e:
            logger.error(f"Error in continuous processing: {e}")
        finally:
            logger.info(f"Batch processing completed. Final metrics: {self.metrics.to_dict()}")
    
    def process_historical_data(self, start_date: datetime, end_date: datetime):
        """Process historical data for a date range."""
        logger.info(f"Processing historical data from {start_date.date()} to {end_date.date()}")
        
        current_date = start_date
        
        while current_date <= end_date:
            try:
                batch_results = self.process_batch(current_date)
                logger.info(f"Processed {current_date.date()}: {batch_results}")
                
            except Exception as e:
                logger.error(f"Error processing {current_date.date()}: {e}")
            
            current_date += timedelta(days=1)
        
        logger.info(f"Historical processing completed. Metrics: {self.metrics.to_dict()}")
    
    def get_analytics_summary(self) -> Dict[str, Any]:
        """Get a summary of current analytics."""
        try:
            with self.get_database_connection() as conn:
                with conn.cursor() as cursor:
                    # Customer segment summary
                    cursor.execute("""
                        SELECT segment, COUNT(*) as count, AVG(total_spent) as avg_spent
                        FROM etl_demo.customer_segments
                        GROUP BY segment
                    """)
                    segment_summary = [dict(row) for row in cursor.fetchall()]
                    
                    # Recent revenue summary
                    cursor.execute("""
                        SELECT * FROM etl_demo.revenue_summary
                        ORDER BY date_analyzed DESC
                        LIMIT 7
                    """)
                    recent_revenue = [dict(row) for row in cursor.fetchall()]
                    
                    # Top products by revenue
                    cursor.execute("""
                        SELECT 
                            p.name as product_name,
                            pa.revenue,
                            pa.units_sold
                        FROM etl_demo.product_analytics pa
                        JOIN etl_demo.products p ON pa.product_id = p.id
                        WHERE pa.date_analyzed = CURRENT_DATE
                        ORDER BY pa.revenue DESC
                        LIMIT 10
                    """)
                    top_products = [dict(row) for row in cursor.fetchall()]
                    
                    return {
                        "segment_summary": segment_summary,
                        "recent_revenue": recent_revenue,
                        "top_products": top_products,
                        "processing_metrics": self.metrics.to_dict()
                    }
        
        except Exception as e:
            logger.error(f"Error getting analytics summary: {e}")
            return {"error": str(e)}


def main():
    """Main function for running the batch processor."""
    import argparse
    
    parser = argparse.ArgumentParser(description="ETL Batch Processor")
    parser.add_argument("--mode", choices=["single", "continuous", "historical"], 
                      default="single", help="Processing mode")
    parser.add_argument("--duration", type=int, default=60,
                      help="Duration in minutes for continuous mode")
    parser.add_argument("--start-date", type=str,
                      help="Start date for historical mode (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str,
                      help="End date for historical mode (YYYY-MM-DD)")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    
    args = parser.parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    processor = BatchProcessor()
    
    if args.mode == "single":
        results = processor.process_batch()
        print(f"Batch processing results: {results}")
    elif args.mode == "continuous":
        processor.run_continuous_processing(duration_minutes=args.duration)
    elif args.mode == "historical":
        if not args.start_date or not args.end_date:
            parser.error("Historical mode requires --start-date and --end-date")
        
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
        processor.process_historical_data(start_date, end_date)


if __name__ == "__main__":
    main()