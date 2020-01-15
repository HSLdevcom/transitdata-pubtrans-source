package fi.hsl.transitdata.pulsarpubtransconnect.config;

import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.pulsarpubtransconnect.PubtransConnector;
import fi.hsl.transitdata.pulsarpubtransconnect.PubtransTableType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;
import java.util.TimeZone;

@Component
@Slf4j
@Profile("default")
public class ApplicationBeans {
    static {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    private static Connection createPubtransConnection() throws Exception {
        String connectionString = "";
        try {
            //Default path is what works with Docker out-of-the-box. Override with a local file if needed
            final String secretFilePath = ConfigUtils.getEnv("FILEPATH_CONNECTION_STRING").orElse("/run/secrets/,_community_conn_string");
            connectionString = new Scanner(new File(secretFilePath))
                    .useDelimiter("\\Z").next();
        } catch (Exception e) {
            log.error("Failed to read Pubtrans connection string from secrets", e);
            throw e;
        }

        if (connectionString.isEmpty()) {
            throw new Exception("Failed to find Pubtrans connection string, exiting application");
        }

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(connectionString);
            log.info("Database connection created");
        } catch (SQLException e) {
            log.error("Failed to connect to Pubtrans database", e);
            throw e;
        }

        return connection;
    }

    @Bean
    private Config config() {
        String table = ConfigUtils.getEnvOrThrow("PT_TABLE");
        PubtransTableType type = PubtransTableType.fromString(table);
        Config config = null;
        if (type == PubtransTableType.ROI_ARRIVAL) {
            config = ConfigParser.createConfig("arrival.conf");
        } else if (type == PubtransTableType.ROI_DEPARTURE) {
            config = ConfigParser.createConfig("departure.conf");
        } else {
            log.error("Failed to get table name from PT_TABLE-env variable, exiting application");
            System.exit(1);
        }
        return config;
    }

    @Bean
    private PulsarApplication pulsarApplication(Config config) throws Exception {
        return PulsarApplication.newInstance(config);
    }

    @Bean
    private PulsarApplicationContext pulsarApplicationContext(PulsarApplication pulsarApplication) {
        return pulsarApplication.getContext();
    }

    @Bean
    private PubtransConnector pubtransConnector(PulsarApplicationContext context) throws Exception {
        String table = ConfigUtils.getEnvOrThrow("PT_TABLE");
        PubtransTableType type = PubtransTableType.fromString(table);
        Connection pubtransConnection = createPubtransConnection();
        return PubtransConnector.newInstance(pubtransConnection, context, type);
    }
}
