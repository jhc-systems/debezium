/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.SchemaCacheIF;

import io.debezium.connector.db2as400.conversion.SchemaInfoConversion;
import io.debezium.relational.RelationalDatabaseSchema;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchemaBuilder;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

public class As400DatabaseSchema extends RelationalDatabaseSchema implements SchemaCacheIF {

    private static final Logger log = LoggerFactory.getLogger(As400DatabaseSchema.class);
    private final As400ConnectorConfig config;
    private final HashMap<String, TableInfo> map = new HashMap<>();
    private final As400RpcConnection rpcConnection;

    public As400DatabaseSchema(As400ConnectorConfig config, As400RpcConnection rpcConnection,
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
        this.rpcConnection = rpcConnection;
    }

    public void addSchema(Table originalTable) {
        Table table = SchemaInfoConversion.fixColumnNames(originalTable);
        TableInfo tableInfo = SchemaInfoConversion.table2TableInfo(table);
        TableId id = table.id();
        map.put(id.catalog() + id.schema() + id.table(), tableInfo);
        this.buildAndRegisterSchema(table);
    }

    public String getSchemaName() {
        return config.getSchema();
    }

    @Override
    public void store(String database, String schema, String tableName, TableInfo tableInfo) {
        map.put(database + schema + tableName, tableInfo);

        Table table = SchemaInfoConversion.tableInfo2Table(database, schema, tableName, tableInfo);
        addSchema(table);
    }

    @Override
    public TableInfo retrieve(String database, String schema, String tableName) {
        return map.get(database + schema + tableName);
    }

}
