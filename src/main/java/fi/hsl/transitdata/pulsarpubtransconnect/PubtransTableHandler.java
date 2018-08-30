package fi.hsl.transitdata.pulsarpubtransconnect;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Queue;

public abstract class PubtransTableHandler {
    static final Logger log = LoggerFactory.getLogger(PubtransTableHandler.class);

    private long lastModifiedTimeStamp;
    Producer<byte[]> producer;

    public PubtransTableHandler(Producer<byte[]> producer) {
        this.lastModifiedTimeStamp = (System.currentTimeMillis() - 5000);
        this.producer = producer;
    }

    public long getLastModifiedTimeStamp() {
        return this.lastModifiedTimeStamp;
    }

    public void setLastModifiedTimeStamp(long ts) {
        this.lastModifiedTimeStamp = ts;
    }

    abstract public Queue<TypedMessageBuilder> handleResultSet(ResultSet resultSet) throws SQLException;

}
