
import json
from datetime import datetime
from confluent_kafka import Consumer, KafkaException

# ==========================================
# KAFKA CONFIGURATION
# ==========================================

KAFKA_BROKER = "localhost:29092"
KAFKA_TOPIC = "snmp"
CONSUMER_GROUP = "snmp-workers"

consumer = Consumer({
    "bootstrap.servers": KAFKA_BROKER,
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True
})


# ==========================================
# PROCESS SNMP TRAFFIC
# ==========================================

def process_message(data):

    print("\n" + "=" * 55)
    print("SNMP Traffic Message Received")
    print("=" * 55)

    print(f"Timestamp   : {data['timestamp']}")
    print(f"Router IP   : {data['router_ip']}")
    print(f"Interface   : {data['interface']}")
    print(f"Index       : {data['interface_index']}")

    print("-" * 55)

    print(f"Incoming    : {data['in_mbps']:.6f} Mbps")
    print(f"Outgoing    : {data['out_mbps']:.6f} Mbps")

    print(f"IN Octets   : {data['in_octets']}")
    print(f"OUT Octets  : {data['out_octets']}")

    print(f"Interval    : {data['interval_seconds']} seconds")

    print("=" * 55)


# ==========================================
# MAIN WORKER
# ==========================================

def main():

    print("=" * 55)
    print("Kafka SNMP Traffic Consumer")
    print(f"Kafka Broker : {KAFKA_BROKER}")
    print(f"Kafka Topic  : {KAFKA_TOPIC}")
    print(f"Consumer Group: {CONSUMER_GROUP}")
    print("=" * 55)

    try:
        consumer.subscribe([KAFKA_TOPIC])

        print("[Worker] Waiting for SNMP traffic messages...")

        while True:

            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print(f"[Kafka Error] {msg.error()}")
                continue

            try:
                data = json.loads(
                    msg.value().decode("utf-8")
                )

                print(
                    f"\n[Kafka] Message received "
                    f"from partition={msg.partition()} "
                    f"offset={msg.offset()}"
                )

                process_message(data)

            except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
                print(f"[Worker Error] Invalid message: {e}")

    except KeyboardInterrupt:
        print("\n[Worker] Stopping consumer...")

    except KafkaException as e:
        print(f"[Kafka Error] {e}")

    finally:
        consumer.close()
        print("[Worker] Kafka consumer closed.")


if __name__ == "__main__":
    main()
