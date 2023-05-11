package org.apache.flink.connector.kinesis.source.model;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.Objects;

/** Internal type for enumerating available metadata. */
public enum Metadata {
    Timestamp("timestamp", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull()),
    SequenceNumber("sequence-number", DataTypes.VARCHAR(128).notNull()),
    ShardId("shard-id", DataTypes.VARCHAR(128).notNull());

    private final String fieldName;
    private final DataType dataType;

    Metadata(String fieldName, DataType dataType) {
        this.fieldName = fieldName;
        this.dataType = dataType;
    }

    public String getFieldName() {
        return this.fieldName;
    }

    public DataType getDataType() {
        return this.dataType;
    }

    public static Metadata of(String fieldName) {
        return Arrays.stream(Metadata.values())
                .filter(m -> Objects.equals(m.fieldName, fieldName))
                .findFirst()
                .orElseThrow(
                        () -> {
                            String msg =
                                    "Cannot find Metadata instance for field name '"
                                            + fieldName
                                            + "'";
                            return new IllegalArgumentException(msg);
                        });
    }
}
