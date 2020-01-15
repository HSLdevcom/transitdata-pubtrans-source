package fi.hsl.transitdata.pulsarpubtransconnect.enabletestapi;

import fi.hsl.common.pulsar.PulsarApplicationContext;
import fi.hsl.transitdata.pulsarpubtransconnect.ArrivalHandler;
import fi.hsl.transitdata.pulsarpubtransconnect.DepartureHandler;
import fi.hsl.transitdata.pulsarpubtransconnect.PubtransTableHandler;
import fi.hsl.transitdata.pulsarpubtransconnect.PubtransTableType;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.util.Queue;

@Slf4j
@RequiredArgsConstructor
public class MockPubTransConnector extends AbstractPubtransConnector {

    @NonNull
    public PubtransTableType pubtransTableType;

    @Autowired
    private PulsarApplicationContext pulsarApplicationContext;
    private Producer<byte[]> producer;
    private PubtransTableHandler handler;

    @PostConstruct
    public void init() {
        this.producer = pulsarApplicationContext.getProducer();
        log.info("TableType: " + pubtransTableType);
        switch (pubtransTableType) {
            case ROI_ARRIVAL:
                this.handler = new ArrivalHandler(pulsarApplicationContext);
                break;
            case ROI_DEPARTURE:
                this.handler = new DepartureHandler(pulsarApplicationContext);
                break;
            default:
                throw new IllegalArgumentException("Table type not supported");
        }
    }

    @Override
    public boolean checkPrecondition() throws SQLException {
        return true;
    }

    @Override
    public void queryAndProcessResults() throws PulsarClientException {

    }

    void queryAndProcessResults(TestPojo testPojo) throws PulsarClientException {
        produceMessages(handler.handleResultSet(testPojo));
    }


    private void produceMessages(Queue<TypedMessageBuilder<byte[]>> messageBuilderQueue) throws PulsarClientException {
        if (!producer.isConnected()) {
            throw new PulsarClientException("Producer is not connected");
        }
        for (TypedMessageBuilder<byte[]> msg : messageBuilderQueue) {
            msg.sendAsync()
                    .exceptionally(throwable -> {
                        log.error("Failed to send Pulsar message", throwable);
                        return null;
                    });
        }
        //If we want to get Pulsar Exceptions to bubble up into this thread we need to do a sync flush for all pending messages.
        producer.flush();
    }
}
