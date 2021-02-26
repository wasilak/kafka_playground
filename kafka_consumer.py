from kafka import KafkaConsumer
from aiokafka import AIOKafkaConsumer
import logging
import json
import asyncio

kafka_settings = {
    # "bootstrap_servers": ["msk-dev-1.stepstone.tools:9092"],
    "bootstrap_servers": ["msk-1.stepstone.tools:9092"],
    # "topic": "wasilp01_test",
    "topic": "services_live",
    "group_id": "wasilp01_test",
}


def main_sync():
    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)

    consumer = KafkaConsumer(
        kafka_settings["topic"],
        bootstrap_servers=kafka_settings["bootstrap_servers"],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id=kafka_settings["group_id"],
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    for message in consumer:
        # message = message.value
        logging.info(message.value)


async def consume():
    consumer = AIOKafkaConsumer(
        kafka_settings["topic"],
        bootstrap_servers=kafka_settings["bootstrap_servers"],
        group_id=kafka_settings["group_id"]
    )
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            # print("consumed: ", msg.topic, msg.partition, msg.offset, msg.key, msg.value, msg.timestamp)
            content = json.loads(msg.value)
            # print(content["message"])
            if "message" in content and "wasilp01" in content["message"]:
                print(content)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


# async def main():
#     task = asyncio.create_task(consume())
#     # loop.add_signal_handler(signal.SIGINT, task.cancel)

#     await task


if __name__ == "__main__":
    # main_sync()
    asyncio.run(consume())
