package org.neo4j.driver.internal.net.pooling;

import org.neo4j.driver.internal.exceptions.ConnectionException;
import org.neo4j.driver.internal.exceptions.PackStreamException;

interface Connector
{
    PooledConnection newConnection() throws PackStreamException, ConnectionException;
}
