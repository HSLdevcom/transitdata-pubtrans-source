package fi.hsl.transitdata.pulsarpubtransconnect;

import java.util.Scanner;
import java.io.File;
import java.sql.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.microsoft.sqlserver.jdbc.*;
import com.typesafe.config.Config;
import fi.hsl.common.ConfigParser;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.common.transitdata.TransitdataUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        try {
            String table = System.getenv("PT_TABLE");
            PubtransTableType type = PubtransTableType.fromString(table);

            Config config = null;
            if (type == PubtransTableType.ROI_ARRIVAL) {
                config = ConfigParser.createConfig("arrival.conf");
            }
            else if (type == PubtransTableType.ROI_DEPARTURE) {
                config = ConfigParser.createConfig("departure.conf");
            }
            else {
                log.error("Failed to get table name from PT_TABLE-env variable, exiting application");
                System.exit(1);
            }

            Connection connection = createPubtransConnection();

            PulsarApplication app = PulsarApplication.newInstance(config);
            PulsarApplicationContext context = app.getContext();

            final PubtransConnector connector = new PubtransConnector(connection, context.getProducer(), config, type);

            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            log.info("Starting scheduler");

            scheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    try {
                        connector.queryAndProcessResults();
                    } catch (Exception e) {
                        log.error("Error at Pubtrans scheduler", e);
                    }
                }
            }, 0, 1, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            log.error("Exception at Main", e);
        }
    }

    private static Connection createPubtransConnection() throws Exception {
        String connectionString = "";
        try {
            //Default path is what works with Docker out-of-the-box. Override with a local file if needed
            final String secretFilePath = TransitdataUtils.getEnv("FILEPATH_CONNECTION_STRING").orElse("/run/secrets/pubtrans_community_conn_string");
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
}
