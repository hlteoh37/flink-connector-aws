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

package org.apache.flink.connector.kinesis.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.testutils.source.reader.TestingReaderContext;

import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.connector.kinesis.source.util.TestUtil.STREAM_ARN;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class KinesisStreamsSourceTest {

    @Test
    void testSupportsContinuousUnboundedOnly() throws Exception {
        final Properties sourceConfig = new Properties();
        final KinesisStreamsSource<String> source =
                new KinesisStreamsSource<>(STREAM_ARN, sourceConfig, new SimpleStringSchema());

        assertThat(source.getBoundedness()).isEqualTo(Boundedness.CONTINUOUS_UNBOUNDED);
    }

    @Test
    void testCreateSourceReaderValidatesAwsCredentials() throws Exception {
        // This is not a thorough check, but instead checks that we call the validate credentials
        // method on AWSGeneralUtil

        final Properties sourceConfig = new Properties();
        sourceConfig.put(AWS_CREDENTIALS_PROVIDER, "BASIC");
        final KinesisStreamsSource<String> source =
                new KinesisStreamsSource<>(STREAM_ARN, sourceConfig, new SimpleStringSchema());

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> source.createReader(new TestingReaderContext()))
                .withMessageContaining("Please set values for AWS Access Key ID");
    }
}
