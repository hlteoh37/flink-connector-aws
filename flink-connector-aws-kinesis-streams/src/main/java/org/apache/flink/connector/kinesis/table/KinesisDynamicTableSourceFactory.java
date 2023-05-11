package org.apache.flink.connector.kinesis.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.kinesis.table.util.KinesisStreamsConnectorSourceOptionsUtil;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.AWS_REGION;
import static org.apache.flink.connector.kinesis.table.KinesisConnectorOptions.STREAM;
import static org.apache.flink.table.factories.FactoryUtil.FORMAT;

/** Factory for creating {@link KinesisDynamicSource}. */
public class KinesisDynamicTableSourceFactory implements DynamicTableSourceFactory {
    public static final String IDENTIFIER = "kinesis-source";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        ReadableConfig tableOptions = helper.getOptions();

        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, FORMAT);

        ResolvedCatalogTable catalogTable = context.getCatalogTable();

        KinesisStreamsConnectorSourceOptionsUtil kinesisStreamsConnectorSourceOptionsUtil =
                new KinesisStreamsConnectorSourceOptionsUtil(
                        catalogTable.getOptions(), tableOptions.get(STREAM));
        Properties properties =
                kinesisStreamsConnectorSourceOptionsUtil.getValidatedConfigurations();

        KinesisDynamicSource.KinesisDynamicTableSourceBuilder builder =
                new KinesisDynamicSource.KinesisDynamicTableSourceBuilder();

        builder.setStream(tableOptions.get(STREAM))
                .setDecodingFormat(decodingFormat)
                .setConsumedDataType(context.getPhysicalRowDataType())
                .setConsumerProperties(properties);

        return builder.build();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(STREAM);
        options.add(FORMAT);
        options.add(AWS_REGION);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}
