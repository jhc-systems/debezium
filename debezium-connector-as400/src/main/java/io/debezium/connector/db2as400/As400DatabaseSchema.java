/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.db2as400.adaptors.FieldDescriptionToTable;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

public class As400DatabaseSchema extends RelationalDatabaseSchema {

    private static final Logger log = LoggerFactory.getLogger(As400DatabaseSchema.class);
    private final As400ConnectorConfig config;

    public As400DatabaseSchema(As400ConnectorConfig config,
                               TopicSelector<TableId> topicSelector,
                               SchemaNameAdjuster schemaNameAdjuster) {
        super(config, topicSelector, config.getTableFilters().dataCollectionFilter(),
                config.getColumnFilter(), new TableSchemaBuilder(
                        new As400ValueConverters(),
                        schemaNameAdjuster,
                        config.customConverterRegistry(),
                        config.getSourceInfoStructMaker().schema(),
                        config.getSanitizeFieldNames()),
                false, config.getKeyMapper());
        this.config = config;
    }
    
    public void addSchema(TableId tableId, DynamicRecordFormat format) {
        Table table = FieldDescriptionToTable.toTable(tableId, format);
        this.buildAndRegisterSchema(table);
    }
    
    public void addSchema(Table table) {
        this.buildAndRegisterSchema(table);
    }
    
    public String getSchemaName() {
    	return config.getJournalFile();
    }
}
