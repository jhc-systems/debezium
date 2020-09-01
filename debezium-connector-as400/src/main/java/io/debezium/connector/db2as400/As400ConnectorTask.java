package io.debezium.connector.db2as400;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.SchemaNameAdjuster;

public class As400ConnectorTask extends BaseSourceTask {
    private static final Logger log = LoggerFactory.getLogger(As400ConnectorTask.class);
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private static final String CONTEXT_NAME = "db2as400-server-connector-task";
    private As400DatabaseSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    public static TopicSelector<TableId> defaultTopicSelector(As400ConnectorConfig connectorConfig) {
        return TopicSelector.defaultSelector(connectorConfig,
                (tableId, prefix, delimiter) -> String.join(delimiter, prefix, tableId.schema(), tableId.table()));
    }

    @Override
    protected ChangeEventSourceCoordinator start(Configuration config) {
        log.debug("start connector task");
        final As400ConnectorConfig connectorConfig = new As400ConnectorConfig(config);
        final SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create(log);

        // TODO get list of DB ids - see Db2TaskContext
        CdcSourceTaskContext ctx = new CdcSourceTaskContext(connectorConfig.getContextName(), connectorConfig.getLogicalName(), null);

        // Set up the task record queue ...
        this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(connectorConfig.getPollInterval())
                .maxBatchSize(connectorConfig.getMaxBatchSize())
                .maxQueueSize(connectorConfig.getMaxQueueSize())
                .loggingContextSupplier(() -> ctx.configureLoggingContext(CONTEXT_NAME))
                .build();

        ErrorHandler errorHandler = new ErrorHandler(As400RpcConnector.class, connectorConfig.getLogicalName(), queue);

        // TODO find current offset
        OffsetContext previousOffset = new As400OffsetContext(connectorConfig, 0, null, null);

        As400RpcConnection rpcConnection = new As400RpcConnection(connectorConfig);
        As400JdbcConnection jdbcConnection = new As400JdbcConnection(config);

        TopicSelector<TableId> topicSelector = defaultTopicSelector(connectorConfig);

        this.schema = new As400DatabaseSchema(connectorConfig, topicSelector,
                schemaNameAdjuster);

        As400EventMetadataProvider metadataProvider = new As400EventMetadataProvider();

        final EventDispatcher<TableId> dispatcher = new EventDispatcher<>(
                connectorConfig, // CommonConnectorConfig
                topicSelector, // TopicSelector
                schema, // DatabaseSchema
                queue, // ChangeEventQueue
                connectorConfig.getTableFilters().dataCollectionFilter(), // DataCollectionFilter
                DataChangeEvent::new, // ! ChangeEventCreator
                metadataProvider,
                schemaNameAdjuster);

        final Clock clock = Clock.system();

        ChangeEventSourceCoordinator coordinator = new ChangeEventSourceCoordinator(
                previousOffset,
                errorHandler,
                As400JdbcConnector.class, // used for thread naming
                connectorConfig,
                new As400ChangeEventSourceFactory(connectorConfig, rpcConnection,
                        jdbcConnection, errorHandler, dispatcher, clock, schema),
                new DefaultChangeEventSourceMetricsFactory(),
                dispatcher,
                schema);

        As400TaskContext taskContext = new As400TaskContext(connectorConfig, schema);

        coordinator.start(taskContext, this.queue, metadataProvider);

        return coordinator;
    }

    @Override
    protected List<SourceRecord> doPoll() throws InterruptedException {
        final List<DataChangeEvent> records = queue.poll();

        final List<SourceRecord> sourceRecords = records.stream()
                .map(DataChangeEvent::getRecord)
                .collect(Collectors.toList());

        return sourceRecords;
    }

    @Override
    protected void doStop() {
        // TODO Auto-generated method stub

    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return As400ConnectorConfig.ALL_FIELDS;
    }

}
