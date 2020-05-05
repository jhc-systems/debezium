package io.debezium.connector.db2as400;

import com.ibm.as400.access.AS400JDBCDriver;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.jdbc.JdbcConnection;

public class As400JdbcConnection extends JdbcConnection {
    private static final String URL_PATTERN = "jdbc:as400://${" + JdbcConfiguration.HOSTNAME + "}/${" + JdbcConfiguration.DATABASE + "}";

    private static final ConnectionFactory FACTORY = JdbcConnection.patternBasedFactory(URL_PATTERN,
            AS400JDBCDriver.class.getName(),
            As400JdbcConnection.class.getClassLoader());

    public As400JdbcConnection(Configuration config) {
        super(config, FACTORY);
        System.out.println("connection:" + this.connectionString(URL_PATTERN));
    }

    public static As400JdbcConnection forTestDatabase(String databaseName) {
        return new As400JdbcConnection(JdbcConfiguration.copy(Configuration.fromSystemProperties("database."))
                .withDatabase(databaseName)
                // .with("characterEncoding", "utf8")
                .build());
    }

}
