package fi.hsl.transitdata.pulsarpubtransconnect;

import com.typesafe.config.Config;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.*;
import java.util.Queue;

public class PubtransConnector {

    private static final Logger log = LoggerFactory.getLogger(PubtransConnector.class);

    private Connection connection;
    private long queryStartTime;

    private String queryString;

    private PubtransTableHandler handler;


    public PubtransConnector(Connection connection, Jedis jedis, Producer<byte[]> producer, Config config, PubtransTableType tableType) {

        this.connection = connection;
        this.queryString = queryString(config);

        log.info("TableType: " + tableType);
        switch (tableType) {
            case ROI_ARRIVAL:
                this.handler = new ArrivalHandler(jedis, producer);
                break;
            case ROI_DEPARTURE:
                this.handler = new DepartureHandler(jedis, producer);
                break;
            default:
                throw new IllegalArgumentException("Table type not supported");
        }

    }

    private String queryString(Config config) {
        String longName = config.getString("pubtrans.longName");
        String shortName = config.getString("pubtrans.shortName");

        return new StringBuilder()
                .append("SELECT * FROM ")
                .append(longName)
                .append(" AS ")
                .append(shortName)
                .append(" WHERE ")
                .append(shortName)
                .append(".LastModifiedUTCDateTime > ? ")
                .toString();
    }

    public void queryAndProcessResults() {

        queryStartTime = System.currentTimeMillis();
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            statement = connection.prepareStatement(queryString);
            statement.setTimestamp(1, new java.sql.Timestamp(handler.getLastModifiedTimeStamp()));
            resultSet = statement.executeQuery();
            produceMessages(handler.handleResultSet(resultSet));
        }
        catch (Exception e) {
            log.error("Exception while processing results", e);
        }
        finally {
            if (resultSet != null)  try { resultSet.close(); } catch (Exception e) {log.error(e.getMessage());}
            if (statement != null)  try { statement.close(); } catch (Exception e) {log.error(e.getMessage());}
            //if (connection != null)  try { connection.close(); } catch (Exception e) {}
        }
    }

    private void produceMessages(Queue<TypedMessageBuilder> messageBuilderQueue) {

        for (TypedMessageBuilder msg : messageBuilderQueue) {
            msg.sendAsync().thenRun(() -> {});
        }

        log.info(messageBuilderQueue.size() + " messages written. Newest timestamp: " + new java.sql.Timestamp(handler.getLastModifiedTimeStamp()) +
                " Total query and processing time: " + (System.currentTimeMillis() - this.queryStartTime) + " ms");
    }
}

