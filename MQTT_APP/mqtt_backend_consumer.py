import asyncio
import json
import os
from datetime import datetime
from typing import Dict, Any
from paho.mqtt import client as mqtt_client
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from pymongo import MongoClient
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MQTTBackendConsumer:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # MQTT Client
        self.mqtt_client = mqtt_client.Client(
            client_id=config['mqtt']['client_id'],
            protocol=mqtt_client.MQTTv5
        )
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_disconnect = self.on_disconnect
        
        # InfluxDB Client
        self.influx_client = InfluxDBClient(
            url=config['influxdb']['url'],
            token=config['influxdb']['token'],
            org=config['influxdb']['org']
        )
        self.write_api = self.influx_client.write_api(write_options=SYNCHRONOUS)
        
        # MongoDB Client
        self.mongo_client = MongoClient(config['mongodb']['uri'])
        self.mongo_db = self.mongo_client[config['mongodb']['database']]
        self.config_collection = self.mongo_db['device_configs']
        self.metadata_collection = self.mongo_db['device_metadata']
        
        self.message_count = 0
        
    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info("✓ Backend connected to MQTT broker")
            # Subscribe to topics
            for topic in self.config['mqtt']['topics']:
                client.subscribe(topic, qos=1)
                logger.info(f"✓ Subscribed to topic: {topic}")
        else:
            logger.error(f"✗ Failed to connect: {reason_code}")
    
    def on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        logger.warning(f"✗ Backend disconnected: {reason_code}")
    
    def on_message(self, client, userdata, msg):
        """Handle incoming MQTT messages"""
        try:
            self.message_count += 1
            payload = json.loads(msg.payload.decode())
            
            logger.info(f"Received message #{self.message_count} from {payload.get('device_id', 'unknown')}")
            
            # Process data synchronously (MQTT callback is not async)
            self.process_message_sync(payload, msg.topic)
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    async def process_message(self, payload: Dict[str, Any], topic: str):
        """Process message and store in databases"""
        try:
            # Store time-series data in InfluxDB
            await asyncio.to_thread(self.store_timeseries_data, payload)
            
            # Update device metadata in MongoDB
            await asyncio.to_thread(self.update_device_metadata, payload)
            
        except Exception as e:
            logger.error(f"Error in process_message: {e}")
    
    def process_message_sync(self, payload: Dict[str, Any], topic: str):
        """Process message synchronously (called from MQTT callback)"""
        try:
            # Store time-series data in InfluxDB
            self.store_timeseries_data(payload)
            
            # Update device metadata in MongoDB
            self.update_device_metadata(payload)
            
        except Exception as e:
            logger.error(f"Error in process_message_sync: {e}")
    
    def store_timeseries_data(self, payload: Dict[str, Any]):
        """Store sensor data in InfluxDB"""
        try:
            device_id = payload.get('device_id')
            timestamp = payload.get('timestamp')
            sensors = payload.get('sensors', {})
            status = payload.get('status', {})
            
            # Create points for each sensor reading
            points = []
            
            # Sensor measurements
            for sensor_name, value in sensors.items():
                point = Point("sensor_data") \
                    .tag("device_id", device_id) \
                    .tag("sensor_type", sensor_name) \
                    .field("value", float(value)) \
                    .time(timestamp, WritePrecision.NS)
                points.append(point)
            
            # Status measurements
            for status_name, value in status.items():
                point = Point("device_status") \
                    .tag("device_id", device_id) \
                    .tag("status_type", status_name) \
                    .field("value", float(value)) \
                    .time(timestamp, WritePrecision.NS)
                points.append(point)
            
            # Write to InfluxDB
            self.write_api.write(
                bucket=self.config['influxdb']['bucket'],
                org=self.config['influxdb']['org'],
                record=points
            )
            
            logger.info(f"✓ Stored {len(points)} data points to InfluxDB for {device_id}")
            
        except Exception as e:
            logger.error(f"Error storing to InfluxDB: {e}")
    
    def update_device_metadata(self, payload: Dict[str, Any]):
        """Update device metadata in MongoDB"""
        try:
            device_id = payload.get('device_id')
            
            # Update last seen and metadata
            metadata = {
                'device_id': device_id,
                'last_seen': datetime.utcnow(),
                'last_payload': payload,
                'status': payload.get('status', {}),
                'updated_at': datetime.utcnow()
            }
            
            self.metadata_collection.update_one(
                {'device_id': device_id},
                {'$set': metadata},
                upsert=True
            )
            
            logger.info(f"✓ Updated metadata in MongoDB for {device_id}")
            
        except Exception as e:
            logger.error(f"Error updating MongoDB: {e}")
    
    def get_device_config(self, device_id: str) -> Dict[str, Any]:
        """Retrieve device configuration from MongoDB"""
        config = self.config_collection.find_one({'device_id': device_id})
        return config if config else {}
    
    def update_device_config(self, device_id: str, config: Dict[str, Any]):
        """Update device configuration in MongoDB"""
        self.config_collection.update_one(
            {'device_id': device_id},
            {'$set': {**config, 'updated_at': datetime.utcnow()}},
            upsert=True
        )
    
    def start(self):
        """Start the backend consumer"""
        logger.info("Starting MQTT Backend Consumer...")
        
        try:
            # Connect to MQTT broker
            self.mqtt_client.connect(
                self.config['mqtt']['broker'],
                self.config['mqtt']['port'],
                keepalive=60
            )
            
            # Start MQTT loop
            self.mqtt_client.loop_start()
            
            logger.info("✓ Backend consumer started successfully")
            
            # Keep running
            try:
                while True:
                    import time
                    time.sleep(1)
            except KeyboardInterrupt:
                logger.info("\nShutting down...")
                self.stop()
                
        except Exception as e:
            logger.error(f"Failed to start consumer: {e}")
            self.stop()
    
    def stop(self):
        """Stop the backend consumer"""
        logger.info("Stopping backend consumer...")
        
        if self.mqtt_client:
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
        
        if self.influx_client:
            self.influx_client.close()
        
        if self.mongo_client:
            self.mongo_client.close()
        
        logger.info("✓ Backend consumer stopped")


def load_config() -> Dict[str, Any]:
    """Load configuration from environment or defaults"""
    return {
        'mqtt': {
            'broker': os.getenv('MQTT_BROKER', 'localhost'),
            'port': int(os.getenv('MQTT_PORT', 1883)),
            'client_id': os.getenv('MQTT_CLIENT_ID', 'backend_consumer'),
            'topics': os.getenv('MQTT_TOPICS', 'sensors/data,sensors/#').split(',')
        },
        'influxdb': {
            'url': os.getenv('INFLUXDB_URL', 'http://localhost:8086'),
            'token': os.getenv('INFLUXDB_TOKEN', 'your-token-here'),
            'org': os.getenv('INFLUXDB_ORG', 'myorg'),
            'bucket': os.getenv('INFLUXDB_BUCKET', 'sensors')
        },
        'mongodb': {
            'uri': os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'),
            'database': os.getenv('MONGODB_DATABASE', 'iot_platform')
        }
    }


if __name__ == "__main__":
    print("""
    ╔════════════════════════════════════════════╗
    ║   MQTT Backend Consumer Service            ║
    ╚════════════════════════════════════════════╝
    """)
    
    config = load_config()
    
    # Display configuration
    logger.info("Configuration:")
    logger.info(f"  MQTT Broker: {config['mqtt']['broker']}:{config['mqtt']['port']}")
    logger.info(f"  MQTT Topics: {config['mqtt']['topics']}")
    logger.info(f"  InfluxDB: {config['influxdb']['url']}")
    logger.info(f"  MongoDB: {config['mongodb']['uri']}")
    
    consumer = MQTTBackendConsumer(config)
    consumer.start()