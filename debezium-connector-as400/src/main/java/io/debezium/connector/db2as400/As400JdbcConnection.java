/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fnz.db2.journal.retrieve.Connect;
import com.ibm.as400.access.AS400JDBCDriver;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;

public class As400JdbcConnection extends JdbcConnection implements Connect<Connection, SQLException> {
    private static final Logger log = LoggerFactory.getLogger(As400JdbcConnection.class);
    private static final String URL_PATTERN = "jdbc:as400://${" + JdbcConfiguration.HOSTNAME + "}/${"
            + JdbcConfiguration.DATABASE + "}";
    private final Configuration config;

    private static final String GET_DATABASE_NAME = "SELECT CURRENT_SERVER FROM SYSIBM.SYSDUMMY1";
    private static final String GET_SYSTEM_TABLE_NAME = "select system_table_name from qsys2.systables where table_schema=? AND table_name=?";
    private static final String GET_TABLE_NAME = "select table_name from qsys2.systables where table_schema=? AND system_table_name=?";
    private static final String GET_INDEXES = "SELECT c.column_name FROM qsys.QADBKATR k "
            + "INNER JOIN qsys2.SYSCOLUMNS c on c.table_schema=k.dbklib and c.system_table_name=k.dbkfil AND c.system_column_name=k.DBKFLD "
            + "WHERE k.dbklib=? AND k.dbkfil=? ORDER BY k.DBKPOS ASC ";
    private final Map<String, String> systemToLongName = new HashMap<>();
    private final Map<String, String> longToSystemName = new HashMap<>();

    private final String realDatabaseName;

    // private static final String GET_LIST_OF_KEY_COLUMNS = "SELECT c.TABLE_NAME,
    // c.COLUMN_NAME, c.DATA_TYPE, c.LENGTH, c.numeric_scale,
    // c.numeric_precision,column_default"
    // + "FROM qsys2.SYSCOLUMNS c "
    // + "WHERE ((c.system_TABLE_NAME='STOCK' AND
    // c.system_TABLE_SCHEMA='F63HLDDBRD') OR (c.TABLE_NAME='STOCK' AND
    // c.TABLE_SCHEMA='F63HLDDBRD')) "
    // + "order by c.COLUMN_NAME";

    private static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
            AS400JDBCDriver.class.getName(), As400JdbcConnection.class.getClassLoader());

    public As400JdbcConnection(Configuration config) {
        super(config, FACTORY);
        this.config = config;
        realDatabaseName = retrieveRealDatabaseName();
        log.debug("connection:" + this.connectionString(URL_PATTERN));
    }

    public static As400JdbcConnection forTestDatabase(String databaseName) {
        return new As400JdbcConnection(JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDatabase(databaseName).build());
    }

    public String getRealDatabaseName() {
        return realDatabaseName;
    }

    private String retrieveRealDatabaseName() {
        try {
            return queryAndMap(GET_DATABASE_NAME,
                    singleResultMapper(rs -> rs.getString(1).trim(), "Could not retrieve database name"));
        }
        catch (SQLException e) {
            throw new RuntimeException("Couldn't obtain database name", e);
        }
    }

    @Override
    public void readSchema(Tables tables, String databaseCatalog, String schemaNamePattern, TableFilter tableFilter,
                           ColumnNameFilter columnFilter, boolean removeTablesNotFoundInJdbc)
            throws SQLException {
        // Before we make any changes, get the copy of the set of table IDs ...
        Set<TableId> tableIdsBefore = new HashSet<>(tables.tableIds());

        // Read the metadata for the table columns ...
        DatabaseMetaData metadata = connection().getMetaData();

        // Find regular and materialized views as they cannot be snapshotted
        final Set<TableId> viewIds = new HashSet<>();
        final Set<TableId> tableIds = new HashSet<>();

        int totalTables = 0;
        try (final ResultSet rs = metadata.getTables(databaseCatalog, schemaNamePattern, null,
                new String[]{ "VIEW", "MATERIALIZED VIEW", "TABLE" })) {
            while (rs.next()) {
                final String catalogName = rs.getString(1);
                final String schemaName = rs.getString(2);
                final String tableName = rs.getString(3);
                final String tableType = rs.getString(4);
                if ("TABLE".equals(tableType)) {
                    totalTables++;
                    TableId tableId = new TableId(catalogName, schemaName, tableName);
                    if (tableFilter == null || tableFilter.isIncluded(tableId)) {
                        tableIds.add(tableId);
                    }
                }
                else {
                    TableId tableId = new TableId(catalogName, schemaName, tableName);
                    viewIds.add(tableId);
                }
            }
        }

        Map<TableId, List<Column>> columnsByTable = new HashMap<>();

        if (totalTables == tableIds.size()) {
            columnsByTable = getColumnsDetails(databaseCatalog, schemaNamePattern, null, tableFilter, columnFilter,
                    metadata, viewIds);
        }
        else {
            for (TableId includeTable : tableIds) {
                Map<TableId, List<Column>> cols = getColumnsDetails(databaseCatalog, schemaNamePattern,
                        includeTable.table(), tableFilter, columnFilter, metadata, viewIds);
                columnsByTable.putAll(cols);
            }
        }

        // Read the metadata for the primary keys ...
        for (Entry<TableId, List<Column>> tableEntry : columnsByTable.entrySet()) {
            // First get the primary key information, which must be done for *each* table
            // ...
            List<String> pkColumnNames = readPrimaryKeyOrUniqueIndexNames(metadata, tableEntry.getKey());

            // Then define the table ...
            List<Column> columns = tableEntry.getValue();
            Collections.sort(columns);
            String defaultCharsetName = null; // JDBC does not expose character sets
            tables.overwriteTable(tableEntry.getKey(), columns, pkColumnNames, defaultCharsetName);
        }

        if (removeTablesNotFoundInJdbc) {
            // Remove any definitions for tables that were not found in the database
            // metadata ...
            tableIdsBefore.removeAll(columnsByTable.keySet());
            tableIdsBefore.forEach(tables::removeTable);
        }
    }

    protected List<String> readPrimaryKeyOrUniqueIndexNames(DatabaseMetaData metadata, TableId id) throws SQLException {
        List<String> pkColumnNames = readPrimaryKeyNames(metadata, id);
        if (pkColumnNames.isEmpty())
            pkColumnNames = readAs400PrimaryKeys(id);
        if (pkColumnNames.isEmpty())
            pkColumnNames = readTableUniqueIndices(metadata, id);
        return pkColumnNames;
    }

    protected List<String> readAs400PrimaryKeys(TableId id) throws SQLException {
        List<String> columns = prepareQueryAndMap(GET_INDEXES,
                call -> {
                    call.setString(1, id.schema());
                    call.setString(2, id.table());
                },
                rs -> {
                    final List<String> indexColumns = new ArrayList<>();
                    while (rs.next()) {
                        indexColumns.add(rs.getString(1).trim());
                    }
                    return indexColumns;
                });
        return columns;
    }

    private Map<TableId, List<Column>> getColumnsDetails(String databaseCatalog, String schemaNamePattern,
                                                         String tableName, TableFilter tableFilter, ColumnNameFilter columnFilter, DatabaseMetaData metadata,
                                                         final Set<TableId> viewIds)
            throws SQLException {
        Map<TableId, List<Column>> columnsByTable = new HashMap<>();
        try (ResultSet columnMetadata = metadata.getColumns(databaseCatalog, schemaNamePattern, tableName, null)) {
            while (columnMetadata.next()) {
                String catalogName = columnMetadata.getString(1);
                String schemaName = columnMetadata.getString(2);
                String metaTableName = columnMetadata.getString(3);
                TableId tableId = new TableId(catalogName, schemaName, metaTableName);

                // exclude views and non-captured tables
                if (viewIds.contains(tableId) || (tableFilter != null && !tableFilter.isIncluded(tableId))) {
                    continue;
                }

                // add all included columns
                readTableColumn(columnMetadata, tableId, columnFilter).ifPresent(column -> {
                    columnsByTable.computeIfAbsent(tableId, t -> new ArrayList<>()).add(column.create());
                });
            }
        }
        return columnsByTable;
    }

    public String getSystemName(String schemaName, String longTableName) throws SQLException {
        String longKey = String.format("%s.%s", schemaName, longTableName);
        if (longToSystemName.containsKey(longKey)) {
            return longToSystemName.get(longKey);
        }
        else {
            String systemName = prepareQueryAndMap(GET_SYSTEM_TABLE_NAME,
                    call -> {
                        call.setString(1, schemaName);
                        call.setString(2, longTableName);
                    },
                    singleResultMapper(rs -> rs.getString(1).trim(), "Could not retrieve database name"));
            if (systemName == null)
                systemName = longTableName;
            String systemKey = String.format("%s.%s", schemaName, systemName);
            longToSystemName.put(longKey, systemName);
            systemToLongName.put(systemKey, longTableName);
            return systemName;
        }
    }

    public String getLongName(String schemaName, String systemName) {
        String systemKey = String.format("%s.%s", schemaName, systemName);
        if (schemaName.isEmpty() || systemName.isEmpty())
            return "";
        if (systemToLongName.containsKey(systemKey)) {
            return systemToLongName.get(systemKey);
        }
        else {
            try {
                String longTableName = prepareQueryAndMap(GET_TABLE_NAME,
                        call -> {
                            call.setString(1, schemaName);
                            call.setString(2, systemName);
                        },
                        singleResultMapper(rs -> rs.getString(1).trim(), "Could not retrieve database name"));
                if (longTableName == null)
                    longTableName = systemName;
                String longKey = String.format("%s.%s", schemaName, longTableName);
                longToSystemName.put(longKey, systemName);
                systemToLongName.put(systemKey, longTableName);
                return longTableName;
            }
            catch (SQLException e) {
                systemToLongName.put(systemKey, systemName);
                log.debug("unable to lookup system name {} {}", schemaName, systemName, e);
                return systemName;
            }
            catch (IllegalStateException e) {
                systemToLongName.put(systemKey, systemName);
                log.debug("unable to lookup system name {} {}", schemaName, systemName, e);
                return systemName;
            }
        }
    }
}
