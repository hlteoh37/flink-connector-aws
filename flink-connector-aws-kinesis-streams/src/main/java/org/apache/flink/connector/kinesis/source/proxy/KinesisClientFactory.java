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

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.aws.util.AWSClientUtil;
import org.apache.flink.connector.aws.util.AWSGeneralUtil;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsConfigConstants;
import org.apache.flink.connector.kinesis.source.config.KinesisStreamsSourceConfigConstants;
import org.apache.flink.connector.kinesis.util.KinesisConfigUtil;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.retry.backoff.FullJitterBackoffStrategy;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.utils.AttributeMap;

import java.time.Duration;
import java.util.Properties;

import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TCP_KEEPALIVE;

/** Implementation of the {@link StreamProxy} for Kinesis data streams. */
@Internal
public class KinesisClientFactory implements KinesisClientFactoryInterface {

    private final KinesisClient kinesisClientListShards;
    private final KinesisClient kinesisClientGetShardsIterator;
    private final KinesisClient kinesisClientGetRecords;
    private final KinesisClient kinesisClientDescribeStream;
    private final KinesisClient defaultKinesisClient;
    private final Properties consumerConfig;

    // ------------------------------------------------------------------------
    //  listShards() related performance settings
    // ------------------------------------------------------------------------

    /** Base backoff millis for the list shards operation. */
    private final long listShardsBaseBackoffMillis;

    /** Maximum backoff millis for the list shards operation. */
    private final long listShardsMaxBackoffMillis;

    /** Maximum retry attempts for the list shards operation. */
    private final int listShardsMaxRetries;

    private final RetryPolicy listShardsRetryPolicy;

    // ------------------------------------------------------------------------
    //  getRecords() related performance settings
    // ------------------------------------------------------------------------

    /** Base backoff millis for the get records operation. */
    private final long getRecordsBaseBackoffMillis;

    /** Maximum backoff millis for the get records operation. */
    private final long getRecordsMaxBackoffMillis;

    /** Maximum retry attempts for the get records operation. */
    private final int getRecordsMaxRetries;

    private final RetryPolicy getRecordsRetryPolicy;

    // ------------------------------------------------------------------------
    //  getShardIterator() related performance settings
    // ------------------------------------------------------------------------

    /** Base backoff millis for the get shard iterator operation. */
    private final long getShardIteratorBaseBackoffMillis;

    /** Maximum backoff millis for the get shard iterator operation. */
    private final long getShardIteratorMaxBackoffMillis;

    /** Maximum retry attempts for the get shard iterator operation. */
    private final int getShardIteratorMaxRetries;

    private final RetryPolicy getShardIteratorRetryPolicy;

    /** Backoff millis for the describe stream operation. */
    private final long describeStreamBaseBackoffMillis;

    /** Maximum backoff millis for the describe stream operation. */
    private final long describeStreamMaxBackoffMillis;

    /** Maximum retry attempts for the describe stream operation. */
    private final int describeStreamMaxRetries;

    private final RetryPolicy describeStreamRetryPolicy;

    public KinesisClientFactory(Properties consumerConfig) {

        this.consumerConfig = consumerConfig;

        listShardsBaseBackoffMillis =
                KinesisConfigUtil.getLongConsumerConfig(
                        consumerConfig,
                        KinesisStreamsSourceConfigConstants.LIST_SHARDS_BACKOFF_BASE,
                        KinesisStreamsSourceConfigConstants.DEFAULT_LIST_SHARDS_BACKOFF_BASE);
        listShardsMaxBackoffMillis =
                KinesisConfigUtil.getLongConsumerConfig(
                        consumerConfig,
                        KinesisStreamsSourceConfigConstants.LIST_SHARDS_BACKOFF_MAX,
                        KinesisStreamsSourceConfigConstants.DEFAULT_LIST_SHARDS_BACKOFF_MAX);
        listShardsMaxRetries =
                KinesisConfigUtil.getIntConsumerConfig(
                        consumerConfig,
                        KinesisStreamsSourceConfigConstants.LIST_SHARDS_RETRIES,
                        KinesisStreamsSourceConfigConstants.DEFAULT_LIST_SHARDS_RETRIES);
        describeStreamBaseBackoffMillis =
                KinesisConfigUtil.getLongConsumerConfig(
                        consumerConfig,
                        KinesisStreamsSourceConfigConstants.STREAM_DESCRIBE_BACKOFF_BASE,
                        KinesisStreamsSourceConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF_BASE);
        describeStreamMaxBackoffMillis =
                KinesisConfigUtil.getLongConsumerConfig(
                        consumerConfig,
                        KinesisStreamsSourceConfigConstants.STREAM_DESCRIBE_BACKOFF_MAX,
                        KinesisStreamsSourceConfigConstants.DEFAULT_STREAM_DESCRIBE_BACKOFF_MAX);
        describeStreamMaxRetries =
                KinesisConfigUtil.getIntConsumerConfig(
                        consumerConfig,
                        KinesisStreamsSourceConfigConstants.STREAM_DESCRIBE_RETRIES,
                        KinesisStreamsSourceConfigConstants
                                .DEFAULT_DESCRIBE_STREAM_CONSUMER_RETRIES);

        getRecordsBaseBackoffMillis =
                KinesisConfigUtil.getLongConsumerConfig(
                        consumerConfig,
                        KinesisStreamsSourceConfigConstants.SHARD_GETRECORDS_BACKOFF_BASE,
                        KinesisStreamsSourceConfigConstants.DEFAULT_SHARD_GETRECORDS_BACKOFF_BASE);
        getRecordsMaxBackoffMillis =
                KinesisConfigUtil.getLongConsumerConfig(
                        consumerConfig,
                        KinesisStreamsSourceConfigConstants.SHARD_GETRECORDS_BACKOFF_MAX,
                        KinesisStreamsSourceConfigConstants.DEFAULT_SHARD_GETRECORDS_BACKOFF_MAX);
        getRecordsMaxRetries =
                KinesisConfigUtil.getIntConsumerConfig(
                        consumerConfig,
                        KinesisStreamsSourceConfigConstants.SHARD_GETRECORDS_RETRIES,
                        KinesisStreamsSourceConfigConstants.DEFAULT_SHARD_GETRECORDS_RETRIES);
        getShardIteratorBaseBackoffMillis =
                KinesisConfigUtil.getLongConsumerConfig(
                        consumerConfig,
                        KinesisStreamsSourceConfigConstants.SHARD_GETITERATOR_BACKOFF_BASE,
                        KinesisStreamsSourceConfigConstants.DEFAULT_SHARD_GETITERATOR_BACKOFF_BASE);
        getShardIteratorMaxBackoffMillis =
                KinesisConfigUtil.getLongConsumerConfig(
                        consumerConfig,
                        KinesisStreamsSourceConfigConstants.SHARD_GETITERATOR_BACKOFF_MAX,
                        KinesisStreamsSourceConfigConstants.DEFAULT_SHARD_GETITERATOR_BACKOFF_MAX);
        getShardIteratorMaxRetries =
                KinesisConfigUtil.getIntConsumerConfig(
                        consumerConfig,
                        KinesisStreamsSourceConfigConstants.SHARD_GETITERATOR_RETRIES,
                        KinesisStreamsSourceConfigConstants.DEFAULT_SHARD_GETITERATOR_RETRIES);

        getRecordsRetryPolicy =
                createRetryPolicy(
                        getRecordsBaseBackoffMillis,
                        getRecordsMaxBackoffMillis,
                        getRecordsMaxRetries);

        listShardsRetryPolicy =
                createRetryPolicy(
                        listShardsBaseBackoffMillis,
                        listShardsMaxBackoffMillis,
                        listShardsMaxRetries);

        getShardIteratorRetryPolicy =
                createRetryPolicy(
                        getShardIteratorBaseBackoffMillis,
                        getShardIteratorMaxBackoffMillis,
                        getShardIteratorMaxRetries);

        describeStreamRetryPolicy =
                createRetryPolicy(
                        describeStreamBaseBackoffMillis,
                        describeStreamMaxBackoffMillis,
                        describeStreamMaxRetries);

        this.kinesisClientGetRecords = createKinesisClient(getRecordsRetryPolicy);
        this.kinesisClientGetShardsIterator = createKinesisClient(getShardIteratorRetryPolicy);
        this.kinesisClientListShards = createKinesisClient(listShardsRetryPolicy);
        this.kinesisClientDescribeStream = createKinesisClient(describeStreamRetryPolicy);

        this.defaultKinesisClient = createKinesisClient(null);
    }

    @Override
    public KinesisClient getKinesisClient(KinesisClientType kinesisClientType) {
        switch (kinesisClientType) {
            case GET_RECORDS:
                return kinesisClientGetRecords;
            case GET_SHARD_ITERATOR:
                return kinesisClientGetShardsIterator;
            case DESCRIBE_STREAM:
                return kinesisClientDescribeStream;
            case LIST_SHARDS:
                return kinesisClientListShards;
            default:
                return defaultKinesisClient;
        }
    }

    private KinesisClient createKinesisClient(RetryPolicy retryPolicy) {
        Preconditions.checkNotNull(consumerConfig);

        final AttributeMap.Builder clientConfiguration = AttributeMap.builder();
        populateDefaultValues(clientConfiguration);

        final SdkHttpClient httpClient =
                AWSGeneralUtil.createSyncHttpClient(
                        clientConfiguration.build(), ApacheHttpClient.builder());

        Properties clientProperties =
                KinesisConfigUtil.getV2ConsumerClientProperties(consumerConfig);

        AWSGeneralUtil.validateAwsCredentials(consumerConfig);

        return AWSClientUtil.createAwsSyncClient(
                clientProperties,
                httpClient,
                KinesisClient.builder(),
                KinesisStreamsConfigConstants.BASE_KINESIS_USER_AGENT_PREFIX_FORMAT,
                KinesisStreamsConfigConstants.KINESIS_CLIENT_USER_AGENT_PREFIX,
                retryPolicy);
    }

    private RetryPolicy createRetryPolicy(
            long baseBackoffMillis, long maxBackoffMillis, int maxRetries) {
        return RetryPolicy.builder()
                .backoffStrategy(
                        FullJitterBackoffStrategy.builder()
                                .baseDelay(Duration.ofMillis(baseBackoffMillis))
                                .maxBackoffTime(Duration.ofMillis(maxBackoffMillis))
                                .build())
                .numRetries(maxRetries)
                .build();
    }

    private static void populateDefaultValues(final AttributeMap.Builder clientConfiguration) {
        clientConfiguration.put(TCP_KEEPALIVE, true);
    }
}
