/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.HashMap;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.SchemaCacheIF;
import com.fnz.db2.journal.retrieve.SchemaCacheIF.TableInfo;

import io.debezium.relational.Column;
import io.debezium.relational.ColumnEditor;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableEditor;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

public class As400DatabaseSchema extends RelationalDatabaseSchema implements SchemaCacheIF {

    private static final Logger log = LoggerFactory.getLogger(As400DatabaseSchema.class);
    private final As400ConnectorConfig config;
	private final HashMap<String, TableInfo> map = new HashMap<>();


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
    
//    public void addSchema(TableId tableId, DynamicRecordFormat format) {
//        Table table = FieldDescriptionToTable.toTable(tableId, format);
//        this.buildAndRegisterSchema(table);
//    }
    
    public void addSchema(Table table) {
        this.buildAndRegisterSchema(table);
    }
    
    public String getSchemaName() {
    	return config.getSchema();
    }

	@Override
	public void store(String database, String schema, String tableName, TableInfo entry) {
		map.put(database + schema + tableName, entry);
		
		TableEditor editor = Table.editor();
		TableId id = new TableId(database, schema, tableName);
		editor.tableId(id);
		List<Structure> structure = entry.getStructure();
		for (Structure col: structure) {
			ColumnEditor ceditor = Column.editor();
			ceditor.jdbcType(col.getJdcbType());
			ceditor.type(col.getType());
			ceditor.length(col.getLength());
			ceditor.scale(col.getPrecision());
			ceditor.name(col.getName());
			ceditor.autoIncremented(col.isAutoinc());
			ceditor.optional(col.isOptional());
			ceditor.position(col.getPosition());
			
			editor.addColumn(ceditor.create());
		}
		
		editor.setPrimaryKeyNames(entry.getPrimaryKeys());
		Table table = editor.create();
		addSchema(table);
	}

	@Override
	public TableInfo retrieve(String database, String schema, String tableName) {
		return map.get(database + schema + tableName);
	}
    
    
}
