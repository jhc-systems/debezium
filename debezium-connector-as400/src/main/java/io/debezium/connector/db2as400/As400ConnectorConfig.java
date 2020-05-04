package io.debezium.connector.db2as400;

import java.util.function.Predicate;

import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.function.Predicates;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.ColumnId;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.Selectors.TableIdToStringMapper;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables.ColumnNameFilter;
import io.debezium.relational.Tables.TableFilter;
import io.debezium.relational.history.HistoryRecordComparator;

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
//    public As400ConnectorConfig(Configuration config) {
//        super(As400RpcConnector.class, config, config.getString(SERVER_NAME), new SystemTablesPredicate(), x -> x.schema() + "." + x.table(), false);
//
////    	super(config, config.getString(SERVER_NAME), 
////    			new SystemTablesPredicate(), tableToString, 10240);
//
////        this.databaseName = config.getString(DATABASE_NAME);
//        this.columnFilter = getColumnNameFilter(config.getString(RelationalDatabaseConnectorConfig.COLUMN_BLACKLIST));
//    }
    
    public As400ConnectorConfig(Configuration config) {
		super(config, config.getString(JdbcConfiguration.HOSTNAME), new SystemTablesPredicate(), x -> x.schema() + "." + x.table(), 1);
		this.columnFilter = getColumnNameFilter(config.getString(RelationalDatabaseConnectorConfig.COLUMN_BLACKLIST));
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
	protected SourceInfoStructMaker<?> getSourceInfoStructMaker(Version version) {
		return new As400SourceInfoStructMaker(Module.name(), Module.version(), this);
	}
    
    private static ColumnNameFilter getColumnNameFilter(String excludedColumnPatterns) {
        return new ColumnNameFilter() {

            Predicate<ColumnId> delegate = Predicates.excludes(excludedColumnPatterns, ColumnId::toString);

            @Override
            public boolean matches(String catalogName, String schemaName, String tableName, String columnName) {
                // ignore database name as it's not relevant here
                return delegate.test(new ColumnId(new TableId(null, schemaName, tableName), columnName));
            }
        };
    }
    
    public ColumnNameFilter getColumnFilter() {
        return columnFilter;
    }

    public static Field.Set ALL_FIELDS = Field.setOf(
    		JdbcConfiguration.HOSTNAME,
    		USER, PASSWORD, JOURNAL_LIBRARY, JOURNAL_FILE);
}
