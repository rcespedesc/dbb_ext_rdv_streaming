CONFIG_SCHEMA = {
    "type": "object",
    "required": ["source", "source_confluent_cloud", "target"],
    "properties": {
        "source": {
            "type": "object",
            "required": ["SBB", "SBB_v", "type"],
            "properties": {
                "SBB": {"type": "string"},
                "SBB_v": {"type": "string"},
                "type": {"type": "string", "enum": ["kafka"]}
            }
        },
        "source_confluent_cloud": {
            "type": "object",
            "required": [
                "PRM_LHCL_AMBIENTE",
                "PRM_LHCL_INCLUDE_HEADERS",
                "PRM_LHCL_USE_SCHEMA_REGISTRY",
                "PRM_LHCL_CHECKPOINT_LOCATION",
                "PRM_LHCL_APPLICATION",
                "PRM_LHCL_INPUT_TOPIC",
                "PRM_LHCL_KAFKA_CONSUMER_GROUP",
                "PRM_LHCL_MAX_OFFSETS_PER_TRIGGER",
                "PRM_LHCL_STARTING_OFFSETS_VALUE",
                "PRM_LHCL_AVRO_OPTION_MODE"
            ],
            "properties": {
                "PRM_LHCL_AMBIENTE": {"type": "string"},
                "PRM_LHCL_INCLUDE_HEADERS": {"type": "boolean"},
                "PRM_LHCL_USE_SCHEMA_REGISTRY": {"type": "boolean"},
                "PRM_LHCL_CHECKPOINT_LOCATION": {"type": "string"},
                "PRM_LHCL_APPLICATION": {"type": "string"},
                "PRM_LHCL_INPUT_TOPIC": {"type": "string"},
                "PRM_LHCL_KAFKA_CONSUMER_GROUP": {"type": "string"},
                "PRM_LHCL_MAX_OFFSETS_PER_TRIGGER": {"type": "integer", "minimum": 1},
                "PRM_LHCL_STARTING_OFFSETS_VALUE": {"type": "string", "enum": ["earliest", "latest"]},
                "PRM_LHCL_AVRO_OPTION_MODE": {"type": "string", "enum": ["PERMISSIVE", "FAILFAST"]},
                "PRM_LHCL_PARTITION_COLUMN_NAME": {"type": "string"}
            }
        },
        "target": {
            "type": "object",
            "required": ["PRM_LHCL_TABLE", "PRM_LHCL_SCHEMA", "PRM_LHCL_TABLE_DESCRIPTION"],
            "properties": {
                "PRM_LHCL_TABLE": {"type": "string"},
                "PRM_LHCL_SCHEMA": {"type": "string"},
                "PRM_LHCL_TABLE_DESCRIPTION": {"type": "string"}
            }
        }
    }
}
