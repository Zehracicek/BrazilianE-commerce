import json
import time
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from datetime import datetime

class EcommerceConsumer:
    def __init__(self, 
                 kafka_bootstrap_servers='kafka:9092',
                 elasticsearch_host='elasticsearch:9200'):
        
        print("Initializing Elasticsearch connection...")
        self.es = Elasticsearch(
            [elasticsearch_host],
            verify_certs=False,
            request_timeout=30
        )
        
        # Wait for Elasticsearch to be ready
        max_retries = 30
        for i in range(max_retries):
            try:
                if self.es.ping():
                    print("Elasticsearch is ready!")
                    break
            except Exception as e:
                print(f"Waiting for Elasticsearch... ({i+1}/{max_retries})")
                time.sleep(2)
        
        # Create index with mapping
        self.index_name = 'ecommerce-orders'
        self.create_index()
        
        print("Initializing Kafka consumer...")
        self.consumer = KafkaConsumer(
            'ecommerce-orders',
            bootstrap_servers=kafka_bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='ecommerce-consumer-group',
            max_poll_records=100,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        
        print("Consumer initialized successfully!")
        
    def create_index(self):
        """Create Elasticsearch index with proper mapping"""
        
        mapping = {
            "mappings": {
                "properties": {
                    "order_id": {"type": "keyword"},
                    "customer_id": {"type": "keyword"},
                    "order_status": {"type": "keyword"},
                    "order_purchase_timestamp": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss||epoch_millis"
                    },
                    "order_approved_at": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss||epoch_millis"
                    },
                    "order_delivered_timestamp": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss||epoch_millis"
                    },
                    "order_estimated_delivery_date": {
                        "type": "date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss||epoch_millis"
                    },
                    "timestamp": {"type": "date"},
                    
                    "customer": {
                        "properties": {
                            "city": {"type": "keyword"},
                            "state": {"type": "keyword"},
                            "zip_code": {"type": "keyword"}
                        }
                    },
                    
                    "items": {
                        "type": "nested",
                        "properties": {
                            "order_item_id": {"type": "integer"},
                            "product_id": {"type": "keyword"},
                            "seller_id": {"type": "keyword"},
                            "price": {"type": "float"},
                            "freight_value": {"type": "float"},
                            "product_category": {"type": "keyword"},
                            "seller_city": {"type": "keyword"},
                            "seller_state": {"type": "keyword"}
                        }
                    },
                    
                    "payment": {
                        "properties": {
                            "total_value": {"type": "float"},
                            "payment_types": {"type": "keyword"},
                            "installments": {"type": "integer"}
                        }
                    },
                    
                    "review": {
                        "properties": {
                            "score": {"type": "integer"},
                            "comment_title": {"type": "text"},
                            "comment_message": {"type": "text"}
                        }
                    }
                }
            }
        }
        
        try:
            if self.es.indices.exists(index=self.index_name):
                print(f"Index '{self.index_name}' already exists")
            else:
                self.es.indices.create(index=self.index_name, body=mapping)
                print(f"Index '{self.index_name}' created successfully!")
        except Exception as e:
            print(f"Error creating index: {e}")
    
    def process_message(self, message):
        """Process and index a message from Kafka"""
        try:
            order_data = message.value
            order_id = order_data['order_id']
            
            # Index document in Elasticsearch
            response = self.es.index(
                index=self.index_name,
                id=order_id,
                document=order_data
            )
            
            return True, response
            
        except Exception as e:
            print(f"Error processing message: {e}")
            return False, None
    
    def consume(self):
        """Start consuming messages"""
        print(f"Starting to consume messages from topic: ecommerce-orders")
        
        message_count = 0
        
        try:
            for message in self.consumer:
                success, response = self.process_message(message)
                
                if success:
                    message_count += 1
                    
                    if message_count % 100 == 0:
                        print(f"Processed {message_count} messages - "
                              f"Latest offset: {message.offset}")
                
        except KeyboardInterrupt:
            print("Consumer interrupted by user")
        except Exception as e:
            print(f"Error in consumer loop: {e}")
        finally:
            self.close()
    
    def close(self):
        """Close consumer and Elasticsearch connection"""
        self.consumer.close()
        self.es.close()
        print("Consumer closed")

if __name__ == "__main__":
    # Wait for Kafka to be ready
    print("Waiting for Kafka to be ready...")
    time.sleep(30)
    
    consumer = EcommerceConsumer(
        kafka_bootstrap_servers='kafka:9092',
        elasticsearch_host='http://elasticsearch:9200'
    )
    
    consumer.consume()