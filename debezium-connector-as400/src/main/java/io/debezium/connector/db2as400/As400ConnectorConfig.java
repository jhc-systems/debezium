/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.db2as400;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Width;

import com.fnz.db2.journal.retrieve.JournalPosition;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;

//TODO  can we deliver HistorizedRelationalDatabaseConnectorConfig or should it be RelationalDatabaseConnectorConfig 
public class As400ConnectorConfig extends RelationalDatabaseConnectorConfig {
    private static TableIdToStringMapper tableToString = x -> x.schema() + "." + x.table();
    private final ColumnNameFilter columnFilter;
    private final SnapshotMode snapshotMode;
    private final Configuration config;

    /**
     * A field for the user to connect to the AS400. This field has no default
     * value.
     */
    public static final Field USER = Field.create("user", "Name of the user to be used when connecting to the as400");
    /**
     * A field for the password to connect to the AS400. This field has no default
     * value.
     */
    public static final Field PASSWORD = Field.create("password", "Password to be used when connecting to the as400");

    /**
     * A field for the password to connect to the AS400. This field has no default
     * value.
     */
    public static final Field SCHEMA = Field.create("schema", "schema holding tables to capture");

    //
    // public As400ConnectorConfig(Configuration config) {
    // super(As400RpcConnector.class, config, config.getString(SERVER_NAME), new
    // SystemTablesPredicate(), x -> x.schema() + "." + x.table(), false);
    //
    //// super(config, config.getString(SERVER_NAME),
    //// new SystemTablesPredicate(), tableToString, 10240);

    public As400ConnectorConfig(Configuration config) {
        super(config, config.getString(JdbcConfiguration.HOSTNAME), new SystemTablesPredicate(),
                x -> x.schema() + "." + x.table(), 1);
        this.config = config;
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE), SNAPSHOT_MODE.defaultValueAsString());
        this.columnFilter = Tables.ColumnNameFilterFactory
                .createExcludeListFilter(config.getString(RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST));
    }

    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    public String getHostName() {
        return config.getString(JdbcConfiguration.HOSTNAME);
    }

    public String getUser() {
        return config.getString(USER);
    }

    public String getPassword() {
        return config.getString(PASSWORD);
    }

    public String getSchema() {
        return config.getString(SCHEMA);
    }

    public JournalPosition getOffset() {
        String receiver = config.getString(As400OffsetContext.JOURNAL_RECEIVER);
        String lib = config.getString(As400OffsetContext.SCHEMA);
        Long offset = config.getLong(As400OffsetContext.EVENT_SEQUENCE);
        return new JournalPosition(offset, receiver, lib);
    }

    private static class SystemTablesPredicate implements TableFilter {

        @Override
        public boolean isIncluded(TableId t) {
            return !(t.schema().toLowerCase().equals("QSYS2")); // TODO
        }
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
        return new As400SourceInfoStructMaker(Module.name(), Module.version(), this);
    }

    public ColumnNameFilter getColumnFilter() {
        return columnFilter;
    }

    public static Field.Set ALL_FIELDS = Field.setOf(JdbcConfiguration.HOSTNAME, USER, PASSWORD, SCHEMA,
            RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE);

    public static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();

        Field.group(config, "As400 Server", JdbcConfiguration.HOSTNAME, USER, PASSWORD, SCHEMA);

        Field.group(config, "As400 Position", As400OffsetContext.EVENT_SEQUENCE_FIELD,
                As400OffsetContext.JOURNAL_RECEIVER_FIELD, As400OffsetContext.SCHEMA_FIELD);

        // TODO below borrowed form DB2
        Field.group(config, "Events",
                RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST,
                RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST,
                RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST,
                Heartbeat.HEARTBEAT_INTERVAL,
                Heartbeat.HEARTBEAT_TOPICS_PREFIX, CommonConnectorConfig.SOURCE_STRUCT_MAKER_VERSION,
                CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE,
                RelationalDatabaseConnectorConfig.SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE);
        Field.group(config, "Connector", CommonConnectorConfig.POLL_INTERVAL_MS, CommonConnectorConfig.MAX_BATCH_SIZE,
                CommonConnectorConfig.MAX_QUEUE_SIZE, CommonConnectorConfig.SNAPSHOT_DELAY_MS,
                CommonConnectorConfig.SNAPSHOT_FETCH_SIZE, RelationalDatabaseConnectorConfig.DECIMAL_HANDLING_MODE,
                RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE);

        return config;
    }

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode").withDisplayName("Snapshot mode")
            .withEnum(SnapshotMode.class, SnapshotMode.INITIAL).withWidth(Width.SHORT).withImportance(Importance.LOW)
            .withDescription("The criteria for running a snapshot upon startup of the connector. " + "Options include: "
                    + "'initial' (the default) to specify the connector should run a snapshot only when no offsets are available for the logical server name; "
                    + "'schema_only' to specify the connector should run a snapshot of the schema when no offsets are available for the logical server name. ");

    /**
     * The set of predefined SnapshotMode options or aliases.
     */
    public static enum SnapshotMode implements EnumeratedValue {

        /**
         * Perform a snapshot when it is needed.
         */
        WHEN_NEEDED("when_needed", true),

        /**
         * Perform a snapshot only upon initial startup of a connector.
         */
        INITIAL("initial", true),

        /**
         * Perform a snapshot of only the database schemas (without data) and then begin
         * reading the binlog. This should be used with care, but it is very useful when
         * the change event consumers need only the changes from the point in time the
         * snapshot is made (and doesn't care about any state or changes prior to this
         * point).
         */
        SCHEMA_ONLY("schema_only", false),

        /**
         * Never perform a snapshot and only read the binlog. This assumes the binlog
         * contains all the history of those databases and tables that will be captured.
         */
        NEVER("never", false);

        private final String value;
        private final boolean includeData;

        private SnapshotMode(String value, boolean includeData) {
            this.value = value;
            this.includeData = includeData;
        }

        @Override
        public String getValue() {
            return value;
        }

        /**
         * Whether this snapshotting mode should include the actual data or just the
         * schema of captured tables.
         */
        public boolean includeData() {
            return includeData;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value the configuration property value; may not be null
         * @return the matching option, or null if no match is found
         */
        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }

        /**
         * Determine if the supplied value is one of the predefined options.
         *
         * @param value        the configuration property value; may not be null
         * @param defaultValue the default value; may be null
         * @return the matching option, or null if no match is found and the non-null
         *         default is invalid
         */
        public static SnapshotMode parse(String value, String defaultValue) {
            SnapshotMode mode = parse(value);
            if (mode == null && defaultValue != null) {
                mode = parse(defaultValue);
            }
            return mode;
        }
    }

    /**
     * Returns any SELECT overrides, if present.
     */
    @Override
    public Map<TableId, String> getSnapshotSelectOverridesByTable() {
        String tableList = getConfig().getString(SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE);

        if (tableList == null) {
            return Collections.emptyMap();
        }

        Map<TableId, String> snapshotSelectOverridesByTable = new HashMap<>();

        for (String table : tableList.split(",")) {
            snapshotSelectOverridesByTable.put(TableId.parse(table, false),
                    getConfig().getString(SNAPSHOT_SELECT_STATEMENT_OVERRIDES_BY_TABLE + "." + table));
        }

        return Collections.unmodifiableMap(snapshotSelectOverridesByTable);
    }
}
