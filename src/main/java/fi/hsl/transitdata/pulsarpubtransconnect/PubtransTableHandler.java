package fi.hsl.transitdata.pulsarpubtransconnect;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

public abstract class PubtransTableHandler {

    private long lastModifiedTimeStamp;
    Producer<byte[]> producer;

    public PubtransTableHandler(Producer producer) {
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
