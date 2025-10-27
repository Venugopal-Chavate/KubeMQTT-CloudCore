import asyncio
import json
import os
import random
import time
from datetime import datetime
from paho.mqtt import client as mqtt_client
import uuid
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class EdgeDeviceSimulator:
    def __init__(self, broker, port, device_id=None):
        self.broker = broker
        self.port = port
        self.device_id = device_id or f"edge_device_{uuid.uuid4().hex[:8]}"
        self.client = mqtt_client.Client(
            client_id=self.device_id,
            protocol=mqtt_client.MQTTv5,
            callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2
        )
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.connected = False
        
    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            print(f"✓ Device {self.device_id} connected to MQTT broker")
            self.connected = True
        else:
            print(f"✗ Failed to connect: {reason_code}")
            
    def on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        print(f"✗ Device {self.device_id} disconnected: {reason_code}")
        self.connected = False
        
    def connect(self):
        try:
            self.client.connect(self.broker, self.port, keepalive=60)
            self.client.loop_start()
            # Wait for connection
            timeout = 10
            while not self.connected and timeout > 0:
                time.sleep(0.5)
                timeout -= 0.5
            return self.connected
        except Exception as e:
            print(f"✗ Connection error: {e}")
            return False
    
    def generate_sensor_data(self):
        """Generate realistic sensor data"""
        return {
            "device_id": self.device_id,
            "timestamp": datetime.utcnow().isoformat(),
            "sensors": {
                "temperature": round(random.uniform(18.0, 28.0), 2),
                "humidity": round(random.uniform(30.0, 70.0), 2),
                "pressure": round(random.uniform(980.0, 1020.0), 2),
                "vibration": round(random.uniform(0.0, 5.0), 3)
            },
            "status": {
                "battery": random.randint(60, 100),
                "signal_strength": random.randint(-80, -40),
                "uptime": random.randint(1000, 100000)
            }
        }
    
    def publish_data(self, topic):
        """Publish sensor data to MQTT topic"""
        if not self.connected:
            print("✗ Not connected to broker")
            return False
            
        data = self.generate_sensor_data()
        payload = json.dumps(data)
        
        result = self.client.publish(
            topic,
            payload,
            qos=1,  # At least once delivery
            retain=False
        )
        
        if result.rc == mqtt_client.MQTT_ERR_SUCCESS:
            print(f"✓ Published data from {self.device_id}")
            return True
        else:
            print(f"✗ Failed to publish: {result.rc}")
            return False
    
    async def run_continuous(self, interval, topic):
        """Continuously publish data at specified interval"""
        print(f"Starting continuous data publishing every {interval} seconds...")
        try:
            while True:
                self.publish_data(topic)
                await asyncio.sleep(interval)
        except KeyboardInterrupt:
            print("\nStopping simulator...")
        finally:
            self.disconnect()
    
    def disconnect(self):
        """Disconnect from broker"""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
            print(f"✓ Device {self.device_id} disconnected")


async def main():
    # Load configuration from environment variables
    BROKER = os.getenv('MQTT_BROKER', 'localhost')
    PORT = int(os.getenv('MQTT_PORT', '1883'))
    TOPIC = os.getenv('MQTT_TOPIC', 'sensors/data')
    INTERVAL = int(os.getenv('PUBLISH_INTERVAL', '5'))
    NUM_DEVICES = int(os.getenv('NUM_DEVICES', '1'))
    
    print(f"""
    ╔════════════════════════════════════════════╗
    ║   MQTT v5 Edge Device Simulator           ║
    ╚════════════════════════════════════════════╝
    
    Broker: {BROKER}:{PORT}
    Topic: {TOPIC}
    Interval: {INTERVAL}s
    Devices: {NUM_DEVICES}
    """)
    
    # Create and connect devices
    devices = []
    for i in range(NUM_DEVICES):
        device = EdgeDeviceSimulator(BROKER, PORT)
        if device.connect():
            devices.append(device)
        else:
            print(f"Failed to connect device {i+1}")
    
    if not devices:
        print("No devices connected. Exiting.")
        return
    
    # Run all devices concurrently
    tasks = [device.run_continuous(INTERVAL, TOPIC) for device in devices]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())