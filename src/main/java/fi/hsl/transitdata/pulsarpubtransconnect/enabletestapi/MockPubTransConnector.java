package fi.hsl.transitdata.pulsarpubtransconnect.enabletestapi;

import org.apache.pulsar.client.api.PulsarClientException;

import java.sql.SQLException;

public class MockPubTransConnector extends AbstractPubtransConnector {
    @Override
    public boolean checkPrecondition() throws SQLException {
        return true;
    }

    @Override
    public void queryAndProcessResults() throws PulsarClientException {

    }

    String queryAndProcessResults(TestPojo testPojo) {
        //TODO Implement indirection
        return null;
    }
}
