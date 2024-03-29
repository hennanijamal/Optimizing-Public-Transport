"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='com.udacity.turnstile',
    VALUE_FORMAT='AVRO',
    KEY='station_id');
 
CREATE TABLE turnstile_summary 
WITH (KAFKA_TOPIC = 'TURNSTILE_SUMMARY', VALUE_FORMAT='JSON') AS 
    SELECT station_id, COUNT(*) AS count 
    FROM turnstile 
    GROUP BY station_id;
"""



def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    print("Checking of TURNSTILE_SUMMARY topic...")
    print(topic_check.topic_exists("TURNSTILE_SUMMARY"))
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        print("Topic found")
        return

    logging.debug("executing ksql statement...")
    print("Posting data")
    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json",
                "Accept": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    print("Done!")
    resp.raise_for_status()

    

if __name__ == "__main__":
    execute_statement()
