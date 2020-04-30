/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.db2as400.adaptors.FieldDescriptionToTable;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

/**
 * Logical representation of DB2 schema.
 *
 * @author Jiri Pechanec
 */
public class As400DatabaseSchema extends RelationalDatabaseSchema {

    private static final Logger LOGGER = LoggerFactory.getLogger(As400DatabaseSchema.class);

	public As400DatabaseSchema(As400ConnectorConfig config, 
			TopicSelector<TableId> topicSelector,
			SchemaNameAdjuster schemaNameAdjuster) {
		super(config, topicSelector, config.getTableFilters().dataCollectionFilter(), 
				config.getColumnFilter(), new TableSchemaBuilder(
		                new As400ValueConverters(),
		                schemaNameAdjuster,
		                config.customConverterRegistry(),
		                config.getSourceInfoStructMaker().schema(),
		                config.getSanitizeFieldNames()), false, config.getKeyMapper());
		// TODO Auto-generated constructor stub
		
		TableId tableId = new TableId(null, "MSDEVT", "TEST");
		
		Column column = Column.editor().name("id")
				.type("INTEGER")
				.length(6)
//				.defaultValue(-1)
				.create();
		
		Table table = Table.editor()
                .tableId(tableId)
                .addColumns(column)
                .create();
		this.buildAndRegisterSchema(table);
	}

    public void addSchema(TableId tableId, DynamicRecordFormat format) {
    	Table table = FieldDescriptionToTable.toTable(tableId, format);
    	this.buildAndRegisterSchema(table);
    }
    
//    public As400DatabaseSchema(As400ConnectorConfig connectorConfig, SchemaNameAdjuster schemaNameAdjuster, TopicSelector<TableId> topicSelector, As400Connection connection) {
//        super(connectorConfig, topicSelector, connectorConfig.getTableFilters().dataCollectionFilter(), connectorConfig.getColumnFilter(),
//                new TableSchemaBuilder(
//                        new As400ValueConverters(), //TODO connectorConfig.getDecimalMode(), connectorConfig.getTemporalPrecisionMode()
//                        schemaNameAdjuster,
//                        connectorConfig.customConverterRegistry(),
//                        connectorConfig.getSourceInfoStructMaker().schema(),
//                        connectorConfig.getSanitizeFieldNames()),
//                false, connectorConfig.getKeyMapper());
//    }

//	public As400DatabaseSchema(Configuration config, String logicalName, TableFilter systemTablesFilter,
//			TableIdToStringMapper tableIdMapper, int defaultSnapshotFetchSize) {
////		super(config, logicalName, systemTablesFilter, tableIdMapper, defaultSnapshotFetchSize);
//		// TODO Auto-generated constructor stub
//	}



}
