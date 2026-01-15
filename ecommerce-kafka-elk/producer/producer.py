import json
import time
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import os

class EcommerceProducer:
    def __init__(self, bootstrap_servers='kafka:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            acks='all',
            retries=3,
            max_in_flight_requests_per_connection=1
        )
        self.topic = 'ecommerce-orders'
    
    def convert_to_serializable(self, obj):
        """Convert pandas types to native Python types"""
        import numpy as np
        if isinstance(obj, (np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.float64, np.float32)):
            return float(obj)
        elif pd.isna(obj):
            return None
        return obj
        
    def load_datasets(self):
        """Load all CSV datasets"""
        data_path = '/app/data/'
        
        # Olist veri seti için dosya isimleri
        try:
            self.orders = pd.read_csv(f'{data_path}olist_orders_dataset.csv')
        except:
            self.orders = pd.read_csv(f'{data_path}orders.csv')
            
        try:
            self.order_items = pd.read_csv(f'{data_path}olist_order_items_dataset.csv')
        except:
            self.order_items = pd.read_csv(f'{data_path}order_items.csv')
            
        try:
            self.customers = pd.read_csv(f'{data_path}olist_customers_dataset.csv')
        except:
            self.customers = pd.read_csv(f'{data_path}customers.csv')
            
        try:
            self.products = pd.read_csv(f'{data_path}olist_products_dataset.csv')
        except:
            self.products = pd.read_csv(f'{data_path}products.csv')
            
        try:
            self.sellers = pd.read_csv(f'{data_path}olist_sellers_dataset.csv')
        except:
            self.sellers = pd.read_csv(f'{data_path}sellers.csv')
            
        try:
            self.payments = pd.read_csv(f'{data_path}olist_order_payments_dataset.csv')
        except:
            self.payments = pd.read_csv(f'{data_path}payments.csv')
            
        try:
            self.reviews = pd.read_csv(f'{data_path}olist_order_reviews_dataset.csv')
        except:
            self.reviews = pd.read_csv(f'{data_path}reviews.csv')
            
        try:
            self.geolocation = pd.read_csv(f'{data_path}olist_geolocation_dataset.csv')
        except:
            pass
        
        print("All datasets loaded successfully!")
        print(f"Orders: {len(self.orders)}, Items: {len(self.order_items)}, Customers: {len(self.customers)}")
        
    def merge_order_data(self, order_id):
        """Merge all related data for a single order"""
        try:
            # Get order info
            order = self.orders[self.orders['order_id'] == order_id].iloc[0]
            
            # Get customer info
            customer = self.customers[
                self.customers['customer_id'] == order['customer_id']
            ].iloc[0] if not self.customers[
                self.customers['customer_id'] == order['customer_id']
            ].empty else None
            
            # Get order items
            items = self.order_items[
                self.order_items['order_id'] == order_id
            ]
            
            # Get payment info
            payment = self.payments[
                self.payments['order_id'] == order_id
            ]
            
            # Get review info
            review = self.reviews[
                self.reviews['order_id'] == order_id
            ]
            
            # Create enriched order document
            order_doc = {
                'order_id': order_id,
                'customer_id': order['customer_id'],
                'order_status': order['order_status'],
                'order_purchase_timestamp': order['order_purchase_timestamp'],
                'order_approved_at': order.get('order_approved_at', None),
                'order_delivered_timestamp': order.get('order_delivered_carrier_date', None),
                'order_estimated_delivery_date': order.get('order_estimated_delivery_date', None),
                'timestamp': datetime.now().isoformat(),
                
                # Customer information
                'customer': {
                    'city': customer['customer_city'] if customer is not None else None,
                    'state': customer['customer_state'] if customer is not None else None,
                    'zip_code': customer['customer_zip_code_prefix'] if customer is not None else None
                } if customer is not None else None,
                
                # Order items
                'items': [],
                
                # Payment information
                'payment': {
                    'total_value': 0,
                    'payment_types': []
                },
                
                # Review information
                'review': None
            }
            
            # Add items with product and seller info
            total_price = 0
            for _, item in items.iterrows():
                product = self.products[
                    self.products['product_id'] == item['product_id']
                ].iloc[0] if not self.products[
                    self.products['product_id'] == item['product_id']
                ].empty else None
                
                seller = self.sellers[
                    self.sellers['seller_id'] == item['seller_id']
                ].iloc[0] if not self.sellers[
                    self.sellers['seller_id'] == item['seller_id']
                ].empty else None
                
                item_doc = {
                    'order_item_id': self.convert_to_serializable(item['order_item_id']),
                    'product_id': item['product_id'],
                    'seller_id': item['seller_id'],
                    'price': self.convert_to_serializable(item['price']),
                    'freight_value': self.convert_to_serializable(item['freight_value']),
                    'product_category': product['product_category_name'] if product is not None else None,
                    'seller_city': seller['seller_city'] if seller is not None else None,
                    'seller_state': seller['seller_state'] if seller is not None else None
                }
                
                total_price += float(item['price'])
                order_doc['items'].append(item_doc)
            
            # Add payment info
            payment_types = []
            payment_value = 0
            for _, pay in payment.iterrows():
                payment_types.append(pay['payment_type'])
                payment_value += float(pay['payment_value'])
            
            order_doc['payment'] = {
                'total_value': self.convert_to_serializable(payment_value),
                'payment_types': list(set(payment_types)),
                'installments': self.convert_to_serializable(payment.iloc[0]['payment_installments']) if len(payment) > 0 else 0
            }
            
            # Add review info
            if len(review) > 0:
                rev = review.iloc[0]
                order_doc['review'] = {
                    'score': self.convert_to_serializable(rev['review_score']) if pd.notna(rev['review_score']) else None,
                    'comment_title': rev['review_comment_title'] if pd.notna(rev.get('review_comment_title')) else None,
                    'comment_message': rev['review_comment_message'] if pd.notna(rev.get('review_comment_message')) else None
                }
            
            return order_doc
            
        except Exception as e:
            print(f"Error processing order {order_id}: {e}")
            return None
    
    def produce_orders(self, delay=1):
        """Produce orders to Kafka topic"""
        print(f"Starting to produce orders to topic: {self.topic}")
        
        order_ids = self.orders['order_id'].unique()
        total_orders = len(order_ids)
        
        for idx, order_id in enumerate(order_ids):
            order_doc = self.merge_order_data(order_id)
            
            if order_doc:
                try:
                    future = self.producer.send(self.topic, order_doc)
                    record_metadata = future.get(timeout=10)
                    
                    if (idx + 1) % 100 == 0:
                        print(f"Produced {idx + 1}/{total_orders} orders - "
                              f"Topic: {record_metadata.topic}, "
                              f"Partition: {record_metadata.partition}, "
                              f"Offset: {record_metadata.offset}")
                    
                    time.sleep(delay)
                    
                except Exception as e:
                    print(f"Error sending order {order_id}: {e}")
            
        print(f"Finished producing {total_orders} orders")
        
    def close(self):
        """Close producer"""
        self.producer.flush()
        self.producer.close()
        print("Producer closed")

if __name__ == "__main__":
    # Wait for Kafka to be ready
    print("Waiting for Kafka to be ready...")
    time.sleep(20)
    
    producer = EcommerceProducer(bootstrap_servers='kafka:9092')
    
    try:
        producer.load_datasets()
        producer.produce_orders(delay=0.5)  # 0.5 second delay between messages
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        producer.close()