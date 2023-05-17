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
import org.apache.flink.connector.kinesis.source.split.StartingPosition;

import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.ExpiredIteratorException;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsRequest;
import software.amazon.awssdk.services.kinesis.model.ListShardsResponse;
import software.amazon.awssdk.services.kinesis.model.Shard;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Implementation of the {@link StreamProxy} for Kinesis data streams. */
@Internal
public class KinesisStreamProxy implements StreamProxy {

    private final KinesisClient kinesisClientListShards;
    private final KinesisClient kinesisClientGetShardsIterator;
    private final KinesisClient kinesisClientGetRecords;
    private final KinesisClient kinesisClientDescribeStream;
    private final KinesisClient defaultKinesisClient;

    private final Map<String, String> shardIdToIteratorStore;

    public KinesisStreamProxy(KinesisClientFactoryInterface kinesisClientFactory) {
        this.shardIdToIteratorStore = new ConcurrentHashMap<>();

        this.kinesisClientGetRecords =
                kinesisClientFactory.getKinesisClient(
                        KinesisClientFactory.KinesisClientType.GET_RECORDS);
        this.kinesisClientGetShardsIterator =
                kinesisClientFactory.getKinesisClient(
                        KinesisClientFactory.KinesisClientType.GET_SHARD_ITERATOR);
        this.kinesisClientListShards =
                kinesisClientFactory.getKinesisClient(
                        KinesisClientFactory.KinesisClientType.LIST_SHARDS);
        this.kinesisClientDescribeStream =
                kinesisClientFactory.getKinesisClient(
                        KinesisClientFactory.KinesisClientType.DESCRIBE_STREAM);
        this.defaultKinesisClient = kinesisClientFactory.getKinesisClient(null);
    }

    @Override
    public List<Shard> listShards(String streamArn, @Nullable String lastSeenShardId) {
        List<Shard> shards = new ArrayList<>();

        ListShardsResponse listShardsResponse;
        String nextToken = null;
        do {
            listShardsResponse =
                    kinesisClientListShards.listShards(
                            ListShardsRequest.builder()
                                    .streamARN(streamArn)
                                    .exclusiveStartShardId(
                                            nextToken == null ? lastSeenShardId : null)
                                    .nextToken(nextToken)
                                    .build());

            shards.addAll(listShardsResponse.shards());
            nextToken = listShardsResponse.nextToken();
        } while (nextToken != null);

        return shards;
    }

    /**
     * Retrieves records from the stream.
     *
     * @param streamArn the ARN of the stream
     * @param shardId the shard to subscribe from
     * @param startingPosition the starting position to read from
     * @param maxRecordsToGet the maximum number of records to fetch for this getRecords attempt
     * @return the response with records. Includes both the returned records and the subsequent
     *     shard iterator to use.
     */
    @Override
    public GetRecordsResponse getRecords(
            String streamArn,
            String shardId,
            StartingPosition startingPosition,
            int maxRecordsToGet) {
        String shardIterator =
                shardIdToIteratorStore.computeIfAbsent(
                        shardId, (s) -> getShardIterator(streamArn, s, startingPosition));

        try {
            GetRecordsResponse getRecordsResponse =
                    getRecords(streamArn, shardIterator, maxRecordsToGet);
            if (getRecordsResponse.nextShardIterator() != null) {
                shardIdToIteratorStore.put(shardId, getRecordsResponse.nextShardIterator());
            }
            return getRecordsResponse;
        } catch (ExpiredIteratorException e) {
            // Eagerly retry getRecords() if the iterator is expired
            shardIterator = getShardIterator(streamArn, shardId, startingPosition);
            GetRecordsResponse getRecordsResponse =
                    getRecords(streamArn, shardIterator, maxRecordsToGet);
            if (getRecordsResponse.nextShardIterator() != null) {
                shardIdToIteratorStore.put(shardId, getRecordsResponse.nextShardIterator());
            }
            return getRecordsResponse;
        }
    }

    private String getShardIterator(
            String streamArn, String shardId, StartingPosition startingPosition) {
        GetShardIteratorRequest.Builder requestBuilder =
                GetShardIteratorRequest.builder()
                        .streamARN(streamArn)
                        .shardId(shardId)
                        .shardIteratorType(startingPosition.getShardIteratorType());

        switch (startingPosition.getShardIteratorType()) {
            case TRIM_HORIZON:
            case LATEST:
                break;
            case AT_TIMESTAMP:
                if (startingPosition.getStartingMarker() instanceof Instant) {
                    requestBuilder =
                            requestBuilder.timestamp(
                                    (Instant) startingPosition.getStartingMarker());
                } else {
                    throw new IllegalArgumentException(
                            "Invalid object given for GetShardIteratorRequest() when ShardIteratorType is AT_TIMESTAMP. Must be a Date object.");
                }
                break;
            case AT_SEQUENCE_NUMBER:
            case AFTER_SEQUENCE_NUMBER:
                if (startingPosition.getStartingMarker() instanceof String) {
                    requestBuilder =
                            requestBuilder.startingSequenceNumber(
                                    (String) startingPosition.getStartingMarker());
                } else {
                    throw new IllegalArgumentException(
                            "Invalid object given for GetShardIteratorRequest() when ShardIteratorType is AT_SEQUENCE_NUMBER or AFTER_SEQUENCE_NUMBER. Must be a String.");
                }
        }

        return kinesisClientGetShardsIterator
                .getShardIterator(requestBuilder.build())
                .shardIterator();
    }

    private GetRecordsResponse getRecords(
            String streamArn, String shardIterator, int maxRecordsToGet) {
        return kinesisClientGetRecords.getRecords(
                GetRecordsRequest.builder()
                        .streamARN(streamArn)
                        .shardIterator(shardIterator)
                        .limit(maxRecordsToGet)
                        .build());
    }

    @Override
    public void close() throws IOException {
        kinesisClientListShards.close();
        kinesisClientGetShardsIterator.close();
        kinesisClientGetRecords.close();
        kinesisClientDescribeStream.close();
        defaultKinesisClient.close();
    }
}
