package org.apache.flink.connector.kinesis.source.proxy;

import software.amazon.awssdk.services.kinesis.KinesisClient;

public interface KinesisClientFactoryInterface {
    KinesisClient getKinesisClient(KinesisClientType kinesisClientType);

    public enum KinesisClientType {
        GET_RECORDS,
        GET_SHARD_ITERATOR,
        DESCRIBE_STREAM,
        LIST_SHARDS
    }
}
