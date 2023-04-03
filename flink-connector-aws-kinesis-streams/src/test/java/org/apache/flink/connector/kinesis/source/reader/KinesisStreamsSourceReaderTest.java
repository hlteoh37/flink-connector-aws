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

package org.apache.flink.connector.kinesis.source.reader;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.model.CompletedShardsEvent;
import org.apache.flink.connector.kinesis.source.model.TestData;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplit;
import org.apache.flink.connector.kinesis.source.split.KinesisShardSplitState;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplit;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.getTestSplitState;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class KinesisStreamsSourceReaderTest {

    @Test
    void testInitializedState() throws Exception {
        KinesisStreamsSourceReader<TestData> sourceReader =
                new KinesisStreamsSourceReader<>(
                        null, null, null, new Configuration(), new TestingReaderContext());
        KinesisShardSplit split = getTestSplit();
        assertThat(sourceReader.initializedState(split))
                .usingRecursiveComparison()
                .isEqualTo(new KinesisShardSplitState(split));
    }

    @Test
    void testToSplitType() throws Exception {
        KinesisStreamsSourceReader<TestData> sourceReader =
                new KinesisStreamsSourceReader<>(
                        null, null, null, new Configuration(), new TestingReaderContext());
        KinesisShardSplitState splitState = getTestSplitState();
        String splitId = splitState.splitId();
        assertThat(sourceReader.toSplitType(splitId, splitState))
                .usingRecursiveComparison()
                .isEqualTo(splitState.getKinesisShardSplit());
    }

    @Test
    void testOnSplitFinishedSendsCompletedSplitsToSplitEnumerator() throws Exception {
        TestingReaderContext testingReaderContext = new TestingReaderContext();
        KinesisStreamsSourceReader<TestData> sourceReader =
                new KinesisStreamsSourceReader<>(
                        null, null, null, new Configuration(), testingReaderContext);
        KinesisShardSplit split1 = getTestSplit("shardId-000000000002");
        KinesisShardSplit split2 = getTestSplit("shardId-000000000003");
        Map<String, KinesisShardSplitState> finishedSplits =
                ImmutableMap.of(
                        "shardId-000000000002",
                        getTestSplitState(split1),
                        "shardId-000000000003",
                        getTestSplitState(split2));

        // When onSplitFinished called()
        sourceReader.onSplitFinished(finishedSplits);

        // Then
        assertThat(testingReaderContext.getSentEvents())
                .usingRecursiveComparison()
                .isEqualTo(
                        ImmutableList.of(
                                new CompletedShardsEvent(
                                        ImmutableSet.of(split1.splitId(), split2.splitId()))));
    }
}
