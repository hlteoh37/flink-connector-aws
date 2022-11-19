package org.apache.flink.connector.dynamodb.sink;

import org.apache.flink.connector.base.sink.writer.TestSinkInitContext;

import org.junit.Test;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.CredentialProvider.BASIC;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.CredentialProvider.ENV_VAR;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.CredentialProvider.SYS_PROP;
import static org.apache.flink.connector.aws.config.AWSConfigConstants.CredentialProvider.WEB_IDENTITY_TOKEN;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

/** Test for {@link DynamoDbSink}. */
public class DynamoDbSinkTest {

    @Test
    public void testSuccessfullyCreateWithMinimalConfiguration() {
        DynamoDbSink.<Map<String, AttributeValue>>builder()
                .setElementConverter(new TestDynamoDbElementConverter())
                .setTableName("test_table")
                .build();
    }

    @Test
    public void testElementConverterRequired() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                DynamoDbSink.builder()
                                        .setTableName("test_table")
                                        .setFailOnError(true)
                                        .build())
                .withMessageContaining(
                        "ElementConverter must be not null when initializing the AsyncSinkBase.");
    }

    @Test
    public void testTableNameRequired() {
        assertThatExceptionOfType(NullPointerException.class)
                .isThrownBy(
                        () ->
                                DynamoDbSink.<Map<String, AttributeValue>>builder()
                                        .setElementConverter(new TestDynamoDbElementConverter())
                                        .setFailOnError(true)
                                        .build())
                .withMessageContaining(
                        "Destination table name must be set when initializing the DynamoDB Sink.");
    }

    @Test
    public void testTableNameNotEmpty() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                DynamoDbSink.<Map<String, AttributeValue>>builder()
                                        .setElementConverter(new TestDynamoDbElementConverter())
                                        .setTableName("")
                                        .setFailOnError(true)
                                        .build())
                .withMessageContaining(
                        "Destination table name must be set when initializing the DynamoDB Sink.");
    }

    @Test
    public void testInvalidMaxBatchSize() {
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(
                        () ->
                                DynamoDbSink.<Map<String, AttributeValue>>builder()
                                        .setElementConverter(new TestDynamoDbElementConverter())
                                        .setTableName("test_table")
                                        .setMaxBatchSize(50)
                                        .setFailOnError(true)
                                        .build())
                .withMessageContaining(
                        "DynamoDB client supports only up to 25 elements in the batch.");
    }

    @Test
    public void testMaxBatchSizeInBytesThrowsNotImplemented() {
        assertThatExceptionOfType(InvalidConfigurationException.class)
                .isThrownBy(
                        () ->
                                DynamoDbSink.<Map<String, AttributeValue>>builder()
                                        .setElementConverter(new TestDynamoDbElementConverter())
                                        .setTableName("test_table")
                                        .setMaxBatchSizeInBytes(100)
                                        .setFailOnError(true)
                                        .build())
                .withMessageContaining(
                        "Max batch size in bytes is not supported by the DynamoDB sink implementation.");
    }

    @Test
    public void testMaxRecordSizeInBytesThrowsNotImplemented() {
        assertThatExceptionOfType(InvalidConfigurationException.class)
                .isThrownBy(
                        () ->
                                DynamoDbSink.<Map<String, AttributeValue>>builder()
                                        .setElementConverter(new TestDynamoDbElementConverter())
                                        .setTableName("test_table")
                                        .setMaxRecordSizeInBytes(100)
                                        .setFailOnError(true)
                                        .build())
                .withMessageContaining(
                        "Max record size in bytes is not supported by the DynamoDB sink implementation.");
    }

    @Test
    public void testInvalidAwsRegionThrowsException() {
        Properties properties = getDefaultProperties();
        properties.setProperty(AWS_REGION, "some-invalid-region");
        DynamoDbSink<Map<String, AttributeValue>> sink =
                DynamoDbSink.<Map<String, AttributeValue>>builder()
                        .setElementConverter(new TestDynamoDbElementConverter())
                        .setDynamoDbProperties(properties)
                        .setTableName("test_table")
                        .build();

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> sink.createWriter(new TestSinkInitContext()))
                .withMessageContaining("Invalid AWS region set in config.");
    }

    @Test
    public void testIncompleteEnvironmentCredentialsProviderThrowsException() {
        Properties properties = getDefaultProperties();
        properties.put(AWS_CREDENTIALS_PROVIDER, ENV_VAR.toString());
        DynamoDbSink<Map<String, AttributeValue>> sink =
                DynamoDbSink.<Map<String, AttributeValue>>builder()
                        .setElementConverter(new TestDynamoDbElementConverter())
                        .setDynamoDbProperties(properties)
                        .setTableName("test_table")
                        .build();

        assertThatExceptionOfType(SdkClientException.class)
                .isThrownBy(() -> sink.createWriter(new TestSinkInitContext()))
                .withMessageContaining("Unable to load credentials from system settings.");
    }

    @Test
    public void testIncompleteSystemPropertyCredentialsProviderThrowsException() {
        Properties properties = getDefaultProperties();
        properties.put(AWS_CREDENTIALS_PROVIDER, SYS_PROP.toString());
        DynamoDbSink<Map<String, AttributeValue>> sink =
                DynamoDbSink.<Map<String, AttributeValue>>builder()
                        .setElementConverter(new TestDynamoDbElementConverter())
                        .setDynamoDbProperties(properties)
                        .setTableName("test_table")
                        .build();

        assertThatExceptionOfType(SdkClientException.class)
                .isThrownBy(() -> sink.createWriter(new TestSinkInitContext()))
                .withMessageContaining("Unable to load credentials from system settings.");
    }

    @Test
    public void testIncompleteBasicCredentialsProviderThrowsException() {
        Properties properties = getDefaultProperties();
        properties.put(AWS_CREDENTIALS_PROVIDER, BASIC.toString());
        DynamoDbSink<Map<String, AttributeValue>> sink =
                DynamoDbSink.<Map<String, AttributeValue>>builder()
                        .setElementConverter(new TestDynamoDbElementConverter())
                        .setDynamoDbProperties(properties)
                        .setTableName("test_table")
                        .build();

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> sink.createWriter(new TestSinkInitContext()))
                .withMessageContaining(
                        "Please set values for AWS Access Key ID ('aws.credentials.provider.basic.accesskeyid') and Secret Key ('aws.credentials.provider.basic.secretkey') when using the BASIC AWS credential provider type.");
    }

    @Test
    public void testIncompleteWebIdentityTokenCredentialsProviderThrowsException() {
        Properties properties = getDefaultProperties();
        properties.put(AWS_CREDENTIALS_PROVIDER, WEB_IDENTITY_TOKEN.toString());
        DynamoDbSink<Map<String, AttributeValue>> sink =
                DynamoDbSink.<Map<String, AttributeValue>>builder()
                        .setElementConverter(new TestDynamoDbElementConverter())
                        .setDynamoDbProperties(properties)
                        .setTableName("test_table")
                        .build();

        assertThatExceptionOfType(IllegalStateException.class)
                .isThrownBy(() -> sink.createWriter(new TestSinkInitContext()))
                .withMessageContaining(
                        "Either the environment variable AWS_WEB_IDENTITY_TOKEN_FILE or the javaproperty aws.webIdentityTokenFile must be set.");
    }

    @Test
    public void testInvalidCredentialsProviderThrowsException() {
        Properties properties = getDefaultProperties();
        properties.put(AWS_CREDENTIALS_PROVIDER, "INVALID_CREDENTIALS_PROVIDER");
        DynamoDbSink<Map<String, AttributeValue>> sink =
                DynamoDbSink.<Map<String, AttributeValue>>builder()
                        .setElementConverter(new TestDynamoDbElementConverter())
                        .setDynamoDbProperties(properties)
                        .setTableName("test_table")
                        .build();

        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> sink.createWriter(new TestSinkInitContext()))
                .withMessageContaining("Invalid AWS Credential Provider Type set in config.");
    }

    private Properties getDefaultProperties() {
        Properties properties = new Properties();
        properties.setProperty(AWS_REGION, "us-east-1");
        return properties;
    }
}
