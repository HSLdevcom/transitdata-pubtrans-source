package fi.hsl.transitdata.pulsarpubtransconnect;

import fi.hsl.common.transitdata.TransitdataProperties;
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
    final TransitdataProperties.ProtobufSchema schema;

    public PubtransTableHandler(Producer<byte[]> producer, TransitdataProperties.ProtobufSchema schema) {
        this.lastModifiedTimeStamp = (System.currentTimeMillis() - 5000);
        this.producer = producer;
        this.schema = schema;
    }

    public long getLastModifiedTimeStamp() {
        return this.lastModifiedTimeStamp;
    }

    public void setLastModifiedTimeStamp(long ts) {
        this.lastModifiedTimeStamp = ts;
    }

    //TODO finetune SQL so that we can use common method to parse most of the fields. now derived classes contain a lot of duplicate code.
    abstract public Queue<TypedMessageBuilder> handleResultSet(ResultSet resultSet) throws SQLException;

    protected TypedMessageBuilder createMessage(String key, long eventTime, long dvjId, byte[] data) {
        return producer.newMessage()
                .key(key)
                .eventTime(eventTime)
                //.property("table-name", "roi-arrival") //TODO remove, deprecated
                .property(TransitdataProperties.KEY_DVJ_ID, String.valueOf(dvjId))
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, schema.toString())
                .value(data);
    }
}
