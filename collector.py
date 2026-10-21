
import asyncio
import json
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from pysnmp.hlapi.v3arch.asyncio import (
    SnmpEngine,
    CommunityData,
    UdpTransportTarget,
    ContextData,
    ObjectType,
    ObjectIdentity,
    get_cmd,
)

# ==========================================
# CONFIGURATION
# ==========================================

ROUTER_IP = "192.168.233.143"
COMMUNITY = "public"
INTERFACE = "FastEthernet1/1"
INTERFACE_INDEX = 3

KAFKA_BROKER = "localhost:29092"
KAFKA_TOPIC = "snmp"

POLL_INTERVAL = 5

# 64-bit SNMP traffic counters
IN_OID = f"1.3.6.1.2.1.31.1.1.1.6.{INTERFACE_INDEX}"
OUT_OID = f"1.3.6.1.2.1.31.1.1.1.10.{INTERFACE_INDEX}"


# ==========================================
# KAFKA PRODUCER
# ==========================================

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "snmp-collector",
    "acks": "all",
})


def delivery_report(err, msg):
    if err:
        print(f"[Kafka] Delivery failed: {err}")
    else:
        print(
            f"[Kafka] Delivered to {msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


# ==========================================
# SNMP COLLECTION
# ==========================================

async def get_traffic(engine, target):
    error_indication, error_status, error_index, var_binds = await get_cmd(
        engine,
        CommunityData(COMMUNITY, mpModel=1),
        target,
        ContextData(),
        ObjectType(ObjectIdentity(IN_OID)),
        ObjectType(ObjectIdentity(OUT_OID)),
    )

    if error_indication:
        raise RuntimeError(str(error_indication))

    if error_status:
        raise RuntimeError(error_status.prettyPrint())

    if len(var_binds) != 2:
        raise RuntimeError("Unexpected SNMP response")

    return int(var_binds[0][1]), int(var_binds[1][1])


# ==========================================
# MAIN COLLECTOR
# ==========================================

async def main():
    engine = SnmpEngine()

    target = await UdpTransportTarget.create(
        (ROUTER_IP, 161),
        timeout=2,
        retries=2
    )

    previous_in = None
    previous_out = None
    previous_time = None

    print("=" * 55)
    print("SNMP Traffic Collector + Kafka Producer")
    print(f"Router       : {ROUTER_IP}")
    print(f"Interface    : {INTERFACE}")
    print(f"Index        : {INTERFACE_INDEX}")
    print(f"Kafka Broker : {KAFKA_BROKER}")
    print(f"Kafka Topic  : {KAFKA_TOPIC}")
    print(f"Interval     : {POLL_INTERVAL} seconds")
    print("=" * 55)

    try:
        while True:
            try:
                in_octets, out_octets = await get_traffic(
                    engine, target
                )

                current_time = time.monotonic()

                if previous_time is not None:
                    elapsed = current_time - previous_time

                    delta_in = (
                        in_octets - previous_in
                    ) % (2 ** 64)

                    delta_out = (
                        out_octets - previous_out
                    ) % (2 ** 64)

                    in_mbps = (
                        delta_in * 8 / elapsed / 1_000_000
                    )

                    out_mbps = (
                        delta_out * 8 / elapsed / 1_000_000
                    )

                    # JSON message for Kafka
                    message = {
                        "timestamp": datetime.now(
                            timezone.utc
                        ).isoformat(),
                        "router_ip": ROUTER_IP,
                        "interface": INTERFACE,
                        "interface_index": INTERFACE_INDEX,
                        "in_octets": in_octets,
                        "out_octets": out_octets,
                        "in_mbps": round(in_mbps, 6),
                        "out_mbps": round(out_mbps, 6),
                        "interval_seconds": round(elapsed, 3)
                    }

                    # Send message to Kafka
                    producer.produce(
                        topic=KAFKA_TOPIC,
                        key=f"{ROUTER_IP}-{INTERFACE}",
                        value=json.dumps(message).encode("utf-8"),
                        callback=delivery_report
                    )

                    # Process delivery callbacks
                    producer.poll(0)

                    print("-" * 55)
                    print(f"Incoming : {in_mbps:.6f} Mbps")
                    print(f"Outgoing : {out_mbps:.6f} Mbps")
                    print(f"IN Bytes : {in_octets}")
                    print(f"OUT Bytes: {out_octets}")
                    print("[Kafka] Message queued for delivery")

                else:
                    print("Initial SNMP counters collected...")

                previous_in = in_octets
                previous_out = out_octets
                previous_time = current_time

            except BufferError as e:
                print(f"Kafka queue full: {e}")
                producer.poll(1)

            except Exception as e:
                print(f"Collector Error: {e}")
                previous_time = None
                previous_in = None
                previous_out = None

            # Allow Kafka delivery callbacks to execute
            producer.poll(0)

            await asyncio.sleep(POLL_INTERVAL)

    finally:
        print("\nFlushing Kafka messages...")
        remaining = producer.flush(10)
        if remaining:
            print(f"[Kafka] {remaining} messages not delivered")
        engine.close_dispatcher()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nCollector stopped.")
