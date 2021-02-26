from kafka import KafkaProducer
import logging
import json
import time

# json_example = {
#     "name": "t9mGYu5",
#     "cluster_name": "elasticsearch",
#     "cluster_uuid": "xq-6d4QpSDa-kiNE4Ph-Cg",
#     "version": {
#         "number": "5.5.0",
#         "build_hash": "260387d",
#         "build_date": "2017-06-30T23:16:05.735Z",
#         "build_snapshot": False,
#         "lucene_version": "6.6.0"
#     },
#     "tagline": "You Know, for Search"
# }


def main():
    logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s", level=logging.INFO)

    kafka_settings = {
        "bootstrap_servers": ["msk-dev-1.stepstone.tools:9092"],
        "topic": "wasilp01_test",
        "group_id": "wasilp01_test",
    }

    producer = KafkaProducer(
        bootstrap_servers=kafka_settings["bootstrap_servers"],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    i = 0

    while True:
        i = i + 1
        data = {'number': i}
        producer.send(kafka_settings["topic"], value=data)
        logging.info(data)
        # time.sleep(5)


if __name__ == "__main__":
    main()
