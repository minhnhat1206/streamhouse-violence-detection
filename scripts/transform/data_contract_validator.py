import os
import json
import re
from datetime import datetime, timezone, timedelta

# Note: This is a PyFlink streaming script.
# Requires 'apache-flink' python package and Flink's Kafka connector jar (e.g., flink-connector-kafka-1.18.1.jar)

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.typeinfo import Types
from pyflink.common.restart_strategy import RestartStrategies

def validate_record(record_str):
    """
    Data Contract rules validator.
    Returns the JSON string with injected 'is_valid' and 'violations' keys.
    """
    try:
        record = json.loads(record_str)
        violations = []
        
        # 1. Timestamp check: no future timestamps (>1 min)
        try:
            timestamp_str = record.get('timestamp', '')
            dt = datetime.fromisoformat(timestamp_str)
            if dt > datetime.now(timezone.utc) + timedelta(minutes=1):
                violations.append("FUTURE_TIMESTAMP")
        except Exception:
            violations.append("INVALID_TIMESTAMP_FORMAT")

        # 2. Camera ID check: format cam_xx
        if not re.match(r'^cam_\d{2}$', str(record.get('camera_id', ''))):
            violations.append("INVALID_CAMERA_ID")

        # 3. Risk score range
        risk_score = float(record.get('risk_score', -1))
        if not (0 <= risk_score <= 1):
            violations.append("RISK_SCORE_OUT_OF_RANGE")

        # 4. Confidence range
        confidence = float(record.get('confidence', -1))
        if not (0 <= confidence <= 1):
            violations.append("CONFIDENCE_OUT_OF_RANGE")

        # 5. Event type check
        is_violent = record.get('is_violent')
        event_type = record.get('event_type')
        if is_violent is True and not event_type:
            violations.append("MISSING_EVENT_TYPE")

        # 6. High confidence for critical (Warning only, does not fail contract)
        if event_type in ('STABBING', 'SHOOTING'):
            if confidence < 0.85:
                violations.append("LOW_CONFIDENCE_CRITICAL")

        # 7. Sessionization contract: event violent phải mang incident_uid để
        # downstream đếm đúng số VỤ (Warning only — producer cũ chưa có field này)
        if is_violent is True and not record.get('incident_uid'):
            violations.append("MISSING_INCIDENT_UID")

        # Determine validity (exclude warnings from rejection criteria)
        _WARNINGS = {"LOW_CONFIDENCE_CRITICAL", "MISSING_INCIDENT_UID"}
        critical_violations = [v for v in violations if v not in _WARNINGS]
        is_valid = len(critical_violations) == 0
        
        record['violations'] = violations
        record['is_valid'] = is_valid
        return json.dumps(record)

    except Exception as e:
        # Fallback for completely malformed JSON
        return json.dumps({
            "error": str(e), 
            "raw_data": record_str, 
            "is_valid": False, 
            "violations": ["PARSING_ERROR"]
        })

def filter_valid(record_str):
    return json.loads(record_str).get('is_valid') is True

def filter_invalid(record_str):
    return json.loads(record_str).get('is_valid') is False

def strip_metadata_for_valid(record_str):
    # Remove metadata keys before sending to downstream processing to optimize
    # or keep them based on architecture requirement
    return record_str

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(20, 15000))
    env.enable_checkpointing(30000)

    kafka_broker = os.getenv("KAFKA_BROKER", "kafka:9092")
    input_topic = "urban-safety-alerts"
    valid_topic = "hot-violence-alerts-valid" # Temporary Kafka topic acting as bridge to Fluss/Paimon
    invalid_topic = "urban-safety-quarantine"
    
    # Setup Kafka Source
    properties = {
        'bootstrap.servers': kafka_broker,
        'group.id': 'contract-validator-group',
        'auto.offset.reset': 'latest'
    }
    
    consumer = FlinkKafkaConsumer(
        topics=input_topic,
        deserialization_schema=SimpleStringSchema(),
        properties=properties
    )
    
    stream = env.add_source(consumer)

    # Apply Validation Map
    validated_stream = stream.map(validate_record, output_type=Types.STRING())

    # Branch streams
    valid_stream = validated_stream.filter(filter_valid)
    invalid_stream = validated_stream.filter(filter_invalid)

    # Setup Kafka Sinks
    valid_producer = FlinkKafkaProducer(
        topic=valid_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': kafka_broker}
    )
    
    invalid_producer = FlinkKafkaProducer(
        topic=invalid_topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': kafka_broker}
    )

    # Attach Sinks
    valid_stream.add_sink(valid_producer)
    invalid_stream.add_sink(invalid_producer)

    print(f"Submitting PyFlink Data Contract Validator Job: {input_topic} -> [{valid_topic}, {invalid_topic}]")
    env.execute("Data Contract Validator Job")

if __name__ == '__main__':
    main()
