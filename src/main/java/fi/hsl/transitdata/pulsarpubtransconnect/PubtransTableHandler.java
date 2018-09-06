package fi.hsl.transitdata.pulsarpubtransconnect;

import fi.hsl.common.transitdata.TransitdataProperties;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Queue;

public abstract class PubtransTableHandler {
    static final Logger log = LoggerFactory.getLogger(PubtransTableHandler.class);

    private long lastModifiedTimeStamp;
    Producer<byte[]> producer;
    final TransitdataProperties.ProtobufSchema schema;
    final String redisPrefix = TransitdataProperties.REDIS_PREFIX_DVJ; //TODO when to use JPP?!
    private Jedis jedis;

    public PubtransTableHandler(Jedis jedis, Producer<byte[]> producer, TransitdataProperties.ProtobufSchema schema) {
        this.lastModifiedTimeStamp = (System.currentTimeMillis() - 5000);
        this.jedis = jedis;
        this.producer = producer;
        this.schema = schema;
    }

    public long getLastModifiedTimeStamp() {
        return this.lastModifiedTimeStamp;
    }

    public void setLastModifiedTimeStamp(long ts) {
        this.lastModifiedTimeStamp = ts;
    }

    private Map<String, String> getJourneyInfo(long dvjId) {
        String key = redisPrefix + Long.toString(dvjId);
        return jedis.hgetAll(key);
    }

    //TODO finetune SQL so that we can use common method to parse most of the fields. now derived classes contain a lot of duplicate code.
    abstract public Queue<TypedMessageBuilder> handleResultSet(ResultSet resultSet) throws SQLException;

    TypedMessageBuilder createMessage(String key, long eventTime, long dvjId, byte[] data) {
        Map<String, String> journeyInfo = getJourneyInfo(dvjId);
        if (journeyInfo != null) {
            boolean containsAll = journeyInfo.containsKey(TransitdataProperties.KEY_DIRECTION) &&
                    journeyInfo.containsKey(TransitdataProperties.KEY_ROUTE_NAME) &&
                    journeyInfo.containsKey(TransitdataProperties.KEY_START_TIME) &&
                    journeyInfo.containsKey(TransitdataProperties.KEY_OPERATING_DAY);
            if (!containsAll) {
                throw new IllegalArgumentException("Missing fields in journey data for DatedVehicleJourneyId " + dvjId);
            }
        } else {
            throw new IllegalArgumentException("No journey data found for DatedVehicleJourneyId " + dvjId);
        }

        return producer.newMessage()
                .key(key)
                .eventTime(eventTime)
                .property(TransitdataProperties.KEY_DVJ_ID, Long.toString(dvjId))
                .property(TransitdataProperties.KEY_PROTOBUF_SCHEMA, schema.toString())
                .property(TransitdataProperties.KEY_DIRECTION, journeyInfo.get(TransitdataProperties.KEY_DIRECTION))
                .property(TransitdataProperties.KEY_ROUTE_NAME, journeyInfo.get(TransitdataProperties.KEY_ROUTE_NAME))
                .property(TransitdataProperties.KEY_START_TIME, journeyInfo.get(TransitdataProperties.KEY_START_TIME))
                .property(TransitdataProperties.KEY_OPERATING_DAY, journeyInfo.get(TransitdataProperties.KEY_OPERATING_DAY))
                .value(data);
    }
}
