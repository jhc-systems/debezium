package io.debezium.connector.db2as400;

import org.apache.kafka.common.config.ConfigDef;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
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

    /**
     * A field for the user to connect to the AS400. This field has no default value.
     */
    public static final Field USER = Field.create("user",
            "Name of the user to be used when connecting to the as400");
    /**
     * A field for the password to connect to the AS400. This field has no default value.
     */
    public static final Field PASSWORD = Field.create("password",
            "Password to be used when connecting to the as400");

    /**
     * A field for the password to connect to the AS400. This field has no default value.
     */
    public static final Field JOURNAL_LIBRARY = Field.create("journal_library",
            "Journal path to be used retrieving entries");

    /**
     * A field for the password to connect to the AS400. This field has no default value.
     */
    public static final Field JOURNAL_FILE = Field.create("journal_file",
            "Journal file to be used retrieving entires");

    //
    // public As400ConnectorConfig(Configuration config) {
    // super(As400RpcConnector.class, config, config.getString(SERVER_NAME), new SystemTablesPredicate(), x -> x.schema() + "." + x.table(), false);
    //
    //// super(config, config.getString(SERVER_NAME),
    //// new SystemTablesPredicate(), tableToString, 10240);

    public As400ConnectorConfig(Configuration config) {
        super(config, config.getString(JdbcConfiguration.HOSTNAME), new SystemTablesPredicate(), x -> x.schema() + "." + x.table(), 1);
        this.columnFilter = Tables.ColumnNameFilterFactory.createExcludeListFilter(config.getString(RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST));
    }

    public String getHostName() {
        return getConfig().getString(JdbcConfiguration.HOSTNAME);
    }

    public String getUser() {
        return getConfig().getString(USER);
    }

    public String getPassword() {
        return getConfig().getString(PASSWORD);
    }

    public String getJournalLibrary() {
        return getConfig().getString(JOURNAL_LIBRARY);
    }

    public String getJournalFile() {
        return getConfig().getString(JOURNAL_FILE);
    }

    public JournalPosition getOffset() {
        String receiver = getConfig().getString(As400OffsetContext.JOURNAL_RECEIVER);
        String lib = getConfig().getString(As400OffsetContext.JOURNAL_LIB);
        Long offset = getConfig().getLong(As400OffsetContext.EVENT_SEQUENCE);
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

    public static Field.Set ALL_FIELDS = Field.setOf(
            JdbcConfiguration.HOSTNAME,
            USER, PASSWORD, JOURNAL_LIBRARY, JOURNAL_FILE);

    public static ConfigDef configDef() {
        ConfigDef config = new ConfigDef();

        Field.group(config, "As400 Server", JdbcConfiguration.HOSTNAME,
                USER, PASSWORD, JOURNAL_LIBRARY, JOURNAL_FILE);

        Field.group(config, "As400 Position", As400OffsetContext.EVENT_SEQUENCE_FIELD, As400OffsetContext.JOURNAL_RECEIVER_FIELD, As400OffsetContext.JOURNAL_LIB_FIELD);

        // TODO below borrowed form DB2
        Field.group(config, "Events", RelationalDatabaseConnectorConfig.TABLE_INCLUDE_LIST,
                RelationalDatabaseConnectorConfig.TABLE_EXCLUDE_LIST,
                RelationalDatabaseConnectorConfig.COLUMN_EXCLUDE_LIST,
                Heartbeat.HEARTBEAT_INTERVAL, Heartbeat.HEARTBEAT_TOPICS_PREFIX,
                CommonConnectorConfig.SOURCE_STRUCT_MAKER_VERSION,
                CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE);
        Field.group(config, "Connector", CommonConnectorConfig.POLL_INTERVAL_MS, CommonConnectorConfig.MAX_BATCH_SIZE,
                CommonConnectorConfig.MAX_QUEUE_SIZE, CommonConnectorConfig.SNAPSHOT_DELAY_MS, CommonConnectorConfig.SNAPSHOT_FETCH_SIZE,
                RelationalDatabaseConnectorConfig.DECIMAL_HANDLING_MODE, RelationalDatabaseConnectorConfig.TIME_PRECISION_MODE);

        return config;
    }
}
