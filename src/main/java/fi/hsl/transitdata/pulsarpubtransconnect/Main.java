package fi.hsl.transitdata.pulsarpubtransconnect;

import java.time.Duration;
import java.util.Scanner;
import java.io.File;
import java.sql.*;
import java.util.TimeZone;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.microsoft.sqlserver.jdbc.*;
import com.typesafe.config.Config;
import fi.hsl.common.config.ConfigParser;
import fi.hsl.common.config.ConfigUtils;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.common.pulsar.PulsarApplicationContext;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.exceptions.JedisException;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final int SQL_SERVER_ERROR_DEADLOCK_VICTIM = 1205;
    /**
     * Pubtrans stores modified timestamps in UTC format.
     *
     * We're creating java.sql.Timestamps(long epoch) which are also by definition in UTC format.
     * We inject the query parameter via prepared statements into the SQL Server queries.
     *
     * However for some reason JDBC Driver seems to convert this epoch to local datetime before running the query.
     * Using Calendar instances and java.time.* classes don't seem to work, since in our case
     * the ResultSet is always empty because of the conversion.
     *
     * So far the only working solution has been to set the whole JVM to use UTC timezone, which is a hack.
     * However it seems that we have to live with it until we find a better solution..
     *
     * See More:
     * https://github.com/Microsoft/mssql-jdbc/issues/339
     * https://social.msdn.microsoft.com/Forums/sqlserver/en-US/5b4fb9bf-fb9b-44eb-a7a2-222b92c31f5a/sql-server-jdbc-driver-datetimezone-related-query?forum=sqldataaccess
     * */
    static {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    }

    //JDBC connection needs executor for network timeout
    private static final Executor SQL_TIMEOUT_EXECUTOR = Executors.newSingleThreadExecutor(runnable -> {
        final Thread t = new Thread(runnable);
        t.setDaemon(true);
        return t;
    });

    public static void main(String[] args) {
        log.info("Starting Pubtrans Source Application");
        try {
            String table = ConfigUtils.getEnvOrThrow("PT_TABLE");
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

            final Duration networkTimeout = config.getDuration("pubtrans.networkTimeout");

            Connection connection = createPubtransConnection();
            connection.setNetworkTimeout(SQL_TIMEOUT_EXECUTOR, (int) networkTimeout.toMillis());

            final PulsarApplication app = PulsarApplication.newInstance(config);
            PulsarApplicationContext context = app.getContext();

            final PubtransConnector connector = PubtransConnector.newInstance(connection, context, type);

            final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            log.info("Starting scheduler");

            scheduler.scheduleAtFixedRate(() -> {
                try {
                    if(connector.checkPrecondition()) {
                        connector.queryAndProcessResults();
                    }
                    else {
                        log.error("Pubtrans poller precondition failed, skipping the current poll cycle.");
                    }
                }
                catch (SQLServerException sqlServerException) {
                    // Occasionally (once every 2 hours?) the driver throws us out as a deadlock victim.
                    // There's no easy way to fix the root problem so lets just convert it to warning.
                    // More info: https://stackoverflow.com/questions/8390322/cause-of-a-process-being-a-deadlock-victim
                    if (sqlServerException.getErrorCode() == SQL_SERVER_ERROR_DEADLOCK_VICTIM) {
                        log.warn("SQL Server evicted us as deadlock victim. ignoring this for now...", sqlServerException);
                    }
                    else {
                        log.error("SQL Server Unexpected error code, shutting down", sqlServerException);
                        log.warn("Driver Error code: {}", sqlServerException.getErrorCode());
                        closeApplication(app, scheduler);
                    }
                }
                catch (JedisException | SQLException | PulsarClientException connectionException) {
                    log.error("Connection problem, cannot recover so shutting down", connectionException);
                    closeApplication(app, scheduler);
                }
                catch (Exception e) {
                    log.error("Unknown error at Pubtrans scheduler, shutting down", e);
                    closeApplication(app, scheduler);
                }
            }, 0, 1, TimeUnit.SECONDS);
        }
        catch (Exception e) {
            log.error("Exception at Main", e);
        }
    }

    private static void closeApplication(PulsarApplication app, ScheduledExecutorService scheduler) {
        log.warn("Closing application");
        scheduler.shutdown();
        app.close();
    }

    private static Connection createPubtransConnection() throws Exception {
        String connectionString = "";
        try {
            //Default path is what works with Docker out-of-the-box. Override with a local file if needed
            final String secretFilePath = ConfigUtils.getEnv("FILEPATH_CONNECTION_STRING").orElse("/run/secrets/pubtrans_community_conn_string");
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
            if (connectionString.contains("postgresql")) {
                //Load Postgres driver
                Class.forName("org.postgresql.Driver");
            }

            connection = DriverManager.getConnection(connectionString);
            log.info("Database connection created");
        } catch (SQLException e) {
            log.error("Failed to connect to Pubtrans database", e);
            throw e;
        }

        return connection;
    }
}
