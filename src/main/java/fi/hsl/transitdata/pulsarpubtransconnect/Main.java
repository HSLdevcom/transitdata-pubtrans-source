package fi.hsl.transitdata.pulsarpubtransconnect;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import fi.hsl.common.pulsar.PulsarApplication;
import fi.hsl.transitdata.pulsarpubtransconnect.enabletestapi.AbstractPubtransConnector;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import redis.clients.jedis.exceptions.JedisException;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final int SQL_SERVER_ERROR_DEADLOCK_VICTIM = 1205;

    @Autowired
    private AbstractPubtransConnector connector;

    @Autowired
    private PulsarApplication app;

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    private static void closeApplication(PulsarApplication app, ScheduledExecutorService scheduler) {
        log.warn("Closing application");
        scheduler.shutdown();
        app.close();
    }

    @PostConstruct
    public void init() {
        final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        log.info("Starting scheduler");

        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (connector.checkPrecondition()) {
                    connector.queryAndProcessResults();
                } else {
                    log.error("Pubtrans poller precondition failed, skipping the current poll cycle.");
                }
            } catch (SQLServerException sqlServerException) {
                // Occasionally (once every 2 hours?) the driver throws us out as a deadlock victim.
                // There's no easy way to fix the root problem so lets just convert it to warning.
                // More info: https://stackoverflow.com/questions/8390322/cause-of-a-process-being-a-deadlock-victim
                if (sqlServerException.getErrorCode() == SQL_SERVER_ERROR_DEADLOCK_VICTIM) {
                    log.warn("SQL Server evicted us as deadlock victim. ignoring this for now...", sqlServerException);
                } else {
                    log.error("SQL Server Unexpected error code, shutting down", sqlServerException);
                    log.warn("Driver Error code: {}", sqlServerException.getErrorCode());
                    closeApplication(app, scheduler);
                }
            } catch (JedisException | SQLException | PulsarClientException connectionException) {
                log.error("Connection problem, cannot recover so shutting down", connectionException);
                closeApplication(app, scheduler);
            } catch (Exception e) {
                log.error("Unknown error at Pubtrans scheduler, shutting down", e);
                closeApplication(app, scheduler);
            }
        }, 0, 1, TimeUnit.SECONDS);
    }
}
