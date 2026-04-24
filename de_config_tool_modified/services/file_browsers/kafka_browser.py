"""Kafka topic browser - lists topics."""

import asyncio
from typing import Any
from .registry import register

@register("kafka")
async def list_files(connection: dict, max_files: int) -> list[str]:
    """List topics from Kafka cluster.
    
    connection fields:
      - brokers: Comma-separated broker addresses
      - topic: Optional topic prefix/filter
      - authentication: "SASL/PLAIN", "SASL/SCRAM", "mTLS", or "None"
      - sasl_username: For SASL auth
      - sasl_password: For SASL auth
    
    Returns: List of topic names matching prefix
    """
    try:
        from aiokafka import AIOKafkaConsumer
    except ImportError:
        raise ImportError("aiokafka not installed. Run: pip install aiokafka")
    
    async def _list_kafka_topics() -> list[str]:
        brokers = connection.get("brokers", "localhost:9092")
        topic_prefix = connection.get("topic", "")
        auth = connection.get("authentication", "None")
        
        # Parse brokers
        bootstrap_servers = [b.strip() for b in brokers.split(",")]
        
        # Build consumer kwargs
        kwargs = {
            "bootstrap_servers": bootstrap_servers,
            "group_id": "file_browser_consumer",
            "auto_offset_reset": "earliest"
        }
        
        if auth in ["SASL/PLAIN", "SASL/SCRAM"]:
            kwargs["security_protocol"] = "SASL_SSL"
            kwargs["sasl_mechanism"] = "PLAIN" if auth == "SASL/PLAIN" else "SCRAM-SHA-512"
            kwargs["sasl_plain_username"] = connection.get("sasl_username", "")
            kwargs["sasl_plain_password"] = connection.get("sasl_password", "")
        elif auth == "mTLS":
            kwargs["security_protocol"] = "SSL"
        
        try:
            consumer = AIOKafkaConsumer(**kwargs)
            await consumer.start()
            
            # Get all topics
            topics = list(consumer.topics())
            
            # Filter by prefix if provided
            if topic_prefix:
                topics = [t for t in topics if t.startswith(topic_prefix)]
            
            # Sort and limit
            topics = sorted(topics)[:max_files]
            
            return topics
        except Exception as e:
            raise Exception(f"Kafka connection failed: {str(e)}")
        finally:
            try:
                await consumer.stop()
            except:
                pass
    
    return await _list_kafka_topics()
