--/*
-- * Licensed to the Apache Software Foundation (ASF) under one
-- * or more contributor license agreements.  See the NOTICE file
-- * distributed with this work for additional information
-- * regarding copyright ownership.  The ASF licenses this file
-- * to you under the Apache License, Version 2.0 (the
-- * "License"); you may not use this file except in compliance
-- * with the License.  You may obtain a copy of the License at
-- *
-- *     http://www.apache.org/licenses/LICENSE-2.0
-- *
-- * Unless required by applicable law or agreed to in writing, software
-- * distributed under the License is distributed on an "AS IS" BASIS,
-- * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- * See the License for the specific language governing permissions and
-- * limitations under the License.
-- */

CREATE TABLE orders (
  `code` STRING,
  `quantity` BIGINT
) WITH (
  'connector' = 'kinesis-legacy',
  'stream' = 'orders',
  'aws.endpoint' = 'https://kinesalite:4567',
  'aws.credentials.provider'='BASIC',
  'aws.credentials.basic.accesskeyid' = 'access key',
  'aws.credentials.basic.secretkey' ='secret key',
  'scan.stream.initpos' = 'TRIM_HORIZON',
  'scan.shard.discovery.intervalmillis' = '1000',
  'scan.shard.adaptivereads' = 'true',
  'format' = 'json'
);

CREATE TABLE large_orders (
  `code` STRING,
  `quantity` BIGINT
) WITH (
  'connector' = 'kinesis',
  'stream.arn' = 'arn:aws:kinesis:ap-southeast-1:000000000000:stream/large_orders',
  'aws.region' = 'us-east-1',
  'aws.endpoint' = 'https://kinesalite:4567',
  'aws.credentials.provider' = 'BASIC',
  'aws.credentials.basic.accesskeyid' = 'access key',
  'aws.credentials.basic.secretkey' ='secret key',
  'aws.trust.all.certificates' = 'true',
  'sink.http-client.protocol.version' = 'HTTP1_1',
  'sink.batch.max-size' = '1',
  'format' = 'json'
);

INSERT INTO large_orders SELECT * FROM orders WHERE quantity > 10;
