/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kinesis.util;

import org.apache.flink.connector.kinesis.source.config.SourceConfigUtil;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT;
import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.STREAM_INITIAL_TIMESTAMP;
import static org.apache.flink.connector.kinesis.source.config.SourceConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for KinesisConfigUtil. */
@RunWith(PowerMockRunner.class)
public class SourceConfigUtilTest {

    @Rule public ExpectedException exception = ExpectedException.none();

    @Test
    public void testParseStreamTimestampStartingPositionUsingDefaultFormat() throws Exception {
        String timestamp = "2020-08-13T09:18:00.0+01:00";
        Date expectedTimestamp =
                new SimpleDateFormat(DEFAULT_STREAM_TIMESTAMP_DATE_FORMAT).parse(timestamp);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);

        Date actualimestamp =
                SourceConfigUtil.parseStreamTimestampStartingPosition(consumerProperties);

        assertThat(actualimestamp).isEqualTo(expectedTimestamp);
    }

    @Test
    public void testParseStreamTimestampStartingPositionUsingCustomFormat() throws Exception {
        String format = "yyyy-MM-dd'T'HH:mm";
        String timestamp = "2020-08-13T09:23";
        Date expectedTimestamp = new SimpleDateFormat(format).parse(timestamp);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, timestamp);
        consumerProperties.setProperty(STREAM_TIMESTAMP_DATE_FORMAT, format);

        Date actualimestamp =
                SourceConfigUtil.parseStreamTimestampStartingPosition(consumerProperties);

        assertThat(actualimestamp).isEqualTo(expectedTimestamp);
    }

    @Test
    public void testParseStreamTimestampStartingPositionUsingParseError() {
        exception.expect(NumberFormatException.class);

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(STREAM_INITIAL_TIMESTAMP, "bad");

        SourceConfigUtil.parseStreamTimestampStartingPosition(consumerProperties);
    }

    @Test
    public void testParseStreamTimestampStartingPositionIllegalArgumentException() {
        exception.expect(IllegalArgumentException.class);

        Properties consumerProperties = new Properties();

        SourceConfigUtil.parseStreamTimestampStartingPosition(consumerProperties);
    }
}
