/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kinesis.source.proxy;

import org.apache.flink.connector.aws.config.AWSConfigConstants;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.utils.AttributeMap;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.DEFAULT_LIST_SHARDS_BACKOFF_BASE;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.DEFAULT_LIST_SHARDS_BACKOFF_MAX;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.DEFAULT_LIST_SHARDS_RETRIES;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.DEFAULT_SHARD_GETITERATOR_BACKOFF_BASE;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.DEFAULT_SHARD_GETITERATOR_BACKOFF_MAX;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.DEFAULT_SHARD_GETITERATOR_RETRIES;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.DEFAULT_SHARD_GETRECORDS_BACKOFF_BASE;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.DEFAULT_SHARD_GETRECORDS_BACKOFF_MAX;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.DEFAULT_SHARD_GETRECORDS_RETRIES;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF_BASE;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF_MAX;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.DEFAULT_STREAM_DESCRIBE_RETRIES;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.LIST_SHARDS_BACKOFF_BASE;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.LIST_SHARDS_BACKOFF_MAX;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.LIST_SHARDS_RETRIES;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.SHARD_GETITERATOR_BACKOFF_BASE;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.SHARD_GETITERATOR_BACKOFF_MAX;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.SHARD_GETITERATOR_RETRIES;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.SHARD_GETRECORDS_BACKOFF_BASE;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.SHARD_GETRECORDS_BACKOFF_MAX;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.SHARD_GETRECORDS_RETRIES;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.STREAM_DESCRIBE_BACKOFF_BASE;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.STREAM_DESCRIBE_BACKOFF_MAX;
import static org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants.STREAM_DESCRIBE_RETRIES;

class KinesisClientFactoryTest {

    @ParameterizedTest
    @MethodSource("testClientProxyRetryConfig")
    void testConsumerConfigDefaults(
            String kinesisClientType,
            int expectedNumRetries,
            long expectedBaseDelay,
            long expectedMaxBackOffTime
    )
            throws Exception {
        Properties properties = properties();

        KinesisClientFactory clientFactory = new KinesisClientFactory(properties);

        KinesisClient kinesisClient = clientFactory.getKinesisClient(KinesisClientFactoryInterface.KinesisClientType.valueOf(
                kinesisClientType));

        SdkClientConfiguration conf = getField("clientConfiguration", kinesisClient);
        AttributeMap attributeMap = getField("attributes", conf);
        Map<Object, Object> attributes = getField("attributes", attributeMap);

        validateAttributes(attributes, expectedNumRetries, expectedBaseDelay, expectedMaxBackOffTime);
    }

    @ParameterizedTest
    @MethodSource("testClientProxyRetryConfigOverride")
    void testConsumerConfigOverrides(
            String overrideNumRetries,
            String overrideBaseDelay,
            String overrideMaxBackOffTime,
            String kinesisClientType
    )
            throws Exception {
        Properties properties = properties();
        properties.put(overrideNumRetries, 1);
        properties.put(overrideBaseDelay, 2);
        properties.put(overrideMaxBackOffTime, 3);

        KinesisClientFactory clientFactory = new KinesisClientFactory(properties);

        KinesisClient kinesisClient = clientFactory.getKinesisClient(KinesisClientFactoryInterface.KinesisClientType.valueOf(
                kinesisClientType));

        SdkClientConfiguration conf = getField("clientConfiguration", kinesisClient);
        AttributeMap attributeMap = getField("attributes", conf);
        Map<Object, Object> attributes = getField("attributes", attributeMap);

        validateAttributes(attributes, 1, 2, 3);
    }

    private void validateAttributes(
            Map<Object, Object> attributes,
            int expectedNumRetries,
            long expectedBaseDelay,
            long expectedMaxBackOffTime
    ) {
        attributes.values().stream()
                .forEach(
                        obj -> {
                            if (obj instanceof RetryPolicy) {
                                RetryPolicy retryPolicy = (RetryPolicy) obj;
                                Assertions.assertThat(
                                        retryPolicy.numRetries().equals(expectedNumRetries));
                                Assertions.assertThat(
                                        retryPolicy.backoffStrategy()
                                                instanceof FullJitterBackoffStrategy);
                                FullJitterBackoffStrategy fullJitterBackoffStrategy =
                                        (FullJitterBackoffStrategy) retryPolicy.backoffStrategy();
                                Assertions.assertThat(
                                        fullJitterBackoffStrategy
                                                .toBuilder()
                                                .baseDelay()
                                                .equals(expectedBaseDelay));
                                Assertions.assertThat(
                                        fullJitterBackoffStrategy
                                                .toBuilder()
                                                .maxBackoffTime()
                                                .equals(expectedMaxBackOffTime));
                            }
                        });
    }

    private static Stream<Arguments> testClientProxyRetryConfigOverride() throws Exception {
        return Stream.of(
                Arguments.of(
                        LIST_SHARDS_RETRIES, LIST_SHARDS_BACKOFF_BASE, LIST_SHARDS_BACKOFF_MAX,
                        KinesisClientFactoryInterface.KinesisClientType.LIST_SHARDS.toString()
                ),
                Arguments.of(
                        SHARD_GETRECORDS_RETRIES, SHARD_GETRECORDS_BACKOFF_BASE, SHARD_GETRECORDS_BACKOFF_MAX,
                        KinesisClientFactoryInterface.KinesisClientType.GET_RECORDS.toString()
                ),
                Arguments.of(
                        SHARD_GETITERATOR_RETRIES, SHARD_GETITERATOR_BACKOFF_BASE, SHARD_GETITERATOR_BACKOFF_MAX,
                        KinesisClientFactoryInterface.KinesisClientType.GET_SHARD_ITERATOR.toString()
                ),
                Arguments.of(
                        STREAM_DESCRIBE_RETRIES, STREAM_DESCRIBE_BACKOFF_BASE, STREAM_DESCRIBE_BACKOFF_MAX,
                        KinesisClientFactoryInterface.KinesisClientType.DESCRIBE_STREAM.toString()
                )
        );
    }

    private static Stream<Arguments> testClientProxyRetryConfig() throws Exception {
        return Stream.of(
                Arguments.of(
                        KinesisClientFactoryInterface.KinesisClientType.LIST_SHARDS.toString(),
                        DEFAULT_LIST_SHARDS_RETRIES,
                        DEFAULT_LIST_SHARDS_BACKOFF_BASE,
                        DEFAULT_LIST_SHARDS_BACKOFF_MAX
                ),
                Arguments.of(
                        KinesisClientFactoryInterface.KinesisClientType.GET_RECORDS.toString(),
                        DEFAULT_SHARD_GETRECORDS_RETRIES,
                        DEFAULT_SHARD_GETRECORDS_BACKOFF_BASE,
                        DEFAULT_SHARD_GETRECORDS_BACKOFF_MAX
                ),
                Arguments.of(
                        KinesisClientFactoryInterface.KinesisClientType.GET_SHARD_ITERATOR.toString(),
                        DEFAULT_SHARD_GETITERATOR_RETRIES,
                        DEFAULT_SHARD_GETITERATOR_BACKOFF_BASE,
                        DEFAULT_SHARD_GETITERATOR_BACKOFF_MAX
                ),
                Arguments.of(
                        KinesisClientFactoryInterface.KinesisClientType.DESCRIBE_STREAM.toString(),
                        DEFAULT_STREAM_DESCRIBE_RETRIES,
                        DEFAULT_STREAM_DESCRIBE_BACKOFF_BASE,
                        DEFAULT_STREAM_DESCRIBE_BACKOFF_MAX
                )
        );
    }

    private <T> T getField(String fieldName, Object obj) throws Exception {
        Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        return (T) field.get(obj);
    }

    private Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(AWSConfigConstants.AWS_REGION, "eu-west-2");
        return properties;
    }
}
