package io.debezium.connector.db2as400;

import io.debezium.connector.common.CdcSourceTaskContext;

public class As400TaskContext extends CdcSourceTaskContext {

    public As400TaskContext(As400ConnectorConfig config, As400DatabaseSchema schema) {
        super(config.getContextName(), config.getLogicalName(), schema::tableIds);
    }
}
