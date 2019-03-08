import boto3
from elasticsearch import Elasticsearch

INDEX_NAME = "weather"
DOC_TYPE = "weather"
DOC_MAPPING = {
    "properties": {
        "humidity": {
            "type": "float"
        },
        "id": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "location": {
            "type": "text",
            "fields": {
                "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                }
            }
        },
        "pressure": {
            "type": "float"
        },
        "reading_time": {
            "type": "date"
        },
        "temperature": {
            "type": "float"
        }
    }
}

INDEX_SETTINGS = {
    "settings": {
        "index": {
            "number_of_shards": 2,
            "number_of_replicas": 0,
        }
    }
}


def load_from_aws():
    print("Loading from aws...")
    session = boto3.Session(profile_name="weather")
    dynamodb = session.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table("weather")
    readings = []
    for item in table.scan()["Items"]:
        converted = {
            "location": item["location"],
            "reading_time": int(item["reading_time"]) * 1000,
            "temperature": float(item["temperature"]),
            "humidity": float(item["humidity"]),
            "pressure": float(item["pressure"]),
            "id": item["time_location"]
        }
        readings.append(converted)
    print("Read {} from aws".format(len(readings)))
    return readings


def recreate_elasticsearch_index():
    print("Recreating index {} ...".format(INDEX_NAME))
    es = Elasticsearch(hosts=["192.168.2.156"])
    es.indices.delete(index="weather")
    es.indices.create(index="weather", body=INDEX_SETTINGS, ignore=400)
    es.indices.put_mapping(doc_type=DOC_TYPE, body=DOC_MAPPING, index=INDEX_NAME)
    print("Recreated index {}".format(INDEX_NAME))


def write_to_elasticsearch(readings):
    print("Writing into Elasticsearch...")
    es = Elasticsearch(hosts=["192.168.2.156"])
    for reading in readings:
        es.index(index=INDEX_NAME, doc_type=DOC_TYPE, id=reading["id"], body=reading)
    print("Wrote {} into Elasticsearch".format(len(readings)))


def test():
    es = Elasticsearch(hosts=["192.168.2.156"])

    es.indices.create(index="test-index")

    es.index(index="test-index", doc_type="test-doc",
        id=42, body={"hello": "world"}
    )

    a = es.get(index="test-index", doc_type="test-doc", id=42)
    print(a)


if __name__ == "__main__":
    readings = load_from_aws()
    recreate_elasticsearch_index()
    write_to_elasticsearch(readings)
    print("Finished")
