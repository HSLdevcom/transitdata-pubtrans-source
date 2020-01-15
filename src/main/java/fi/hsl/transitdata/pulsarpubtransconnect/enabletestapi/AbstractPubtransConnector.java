package fi.hsl.transitdata.pulsarpubtransconnect.enabletestapi;

import org.apache.pulsar.client.api.PulsarClientException;

import java.sql.SQLException;

public abstract class AbstractPubtransConnector {
    public abstract boolean checkPrecondition() throws SQLException;

    public abstract void queryAndProcessResults() throws PulsarClientException, SQLException;
}
