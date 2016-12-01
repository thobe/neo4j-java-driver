/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.driver.internal.net;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.exceptions.ConnectionException;
import org.neo4j.driver.internal.exceptions.PackStreamException;
import org.neo4j.driver.internal.messaging.InitMessage;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.RunMessage;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Logging;
import org.neo4j.driver.v1.Value;

import static java.lang.String.format;
import static org.neo4j.driver.internal.messaging.AckFailureMessage.ACK_FAILURE;
import static org.neo4j.driver.internal.messaging.DiscardAllMessage.DISCARD_ALL;
import static org.neo4j.driver.internal.messaging.PullAllMessage.PULL_ALL;
import static org.neo4j.driver.internal.messaging.ResetMessage.RESET;

public class SocketConnection implements Connection
{
    private final Queue<Message> pendingMessages = new LinkedList<>();
    private final SocketResponseHandler responseHandler;
    private AtomicBoolean isInterrupted = new AtomicBoolean( false );
    private AtomicBoolean isAckFailureMuted = new AtomicBoolean( false );
    private final Collector.InitCollector initCollector = new Collector.InitCollector();

    private final SocketClient socket;

    private final Logger logger;

    public SocketConnection( BoltServerAddress address, SecurityPlan securityPlan, Logging logging )
            throws ConnectionException
    {
        this.logger = logging.getLog( format( "conn-%s", UUID.randomUUID().toString() ) );

        if( logger.isDebugEnabled() )
        {
            this.responseHandler = new LoggingResponseHandler( logger );
        }
        else
        {
            this.responseHandler = new SocketResponseHandler();
        }

        this.socket = new SocketClient( address, securityPlan, logger );
        socket.start();
    }

    @Override
    public void init( String clientName, Map<String,Value> authToken ) throws PackStreamException, ConnectionException
    {
        queueMessage( new InitMessage( clientName, authToken ), initCollector );
        sync();
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, Collector collector ) throws
            PackStreamException,
            ConnectionException
    {
        queueMessage( new RunMessage( statement, parameters ), collector );
    }

    @Override
    public void discardAll( Collector collector ) throws PackStreamException, ConnectionException
    {
        queueMessage( DISCARD_ALL, collector );
    }

    @Override
    public void pullAll( Collector collector ) throws PackStreamException, ConnectionException
    {
        queueMessage( PULL_ALL, collector );
    }

    @Override
    public void reset() throws PackStreamException, ConnectionException
    {
        queueMessage( RESET, Collector.RESET );
    }

    @Override
    public void ackFailure() throws PackStreamException, ConnectionException
    {
        queueMessage( ACK_FAILURE, Collector.ACK_FAILURE );
    }

    @Override
    public void sync() throws PackStreamException, ConnectionException
    {
        flush();
        receiveAll();
    }

    @Override
    public synchronized void flush() throws PackStreamException, ConnectionException
    {
        ensureNotInterrupted();
        socket.send( pendingMessages );
    }

    private void ensureNotInterrupted() throws PackStreamException, ConnectionException
    {
        if ( isInterrupted.get() )
        {
            // receive each of it and throw error immediately
            while ( responseHandler.collectorsWaiting() > 0 )
            {
                receiveOne();
            }
        }
    }

    private void receiveAll() throws PackStreamException, ConnectionException
    {
        socket.receiveAll( responseHandler );
        assertNoServerFailure();
    }

    @Override
    public void receiveOne() throws PackStreamException, ConnectionException
    {
        socket.receiveOne( responseHandler );
        assertNoServerFailure();
    }

    private void assertNoServerFailure() throws PackStreamException.ServerFailure
    {
        if ( responseHandler.serverFailureOccurred() )
        {
            PackStreamException.ServerFailure exception = responseHandler.serverFailure();
            responseHandler.clearError();
            isInterrupted.set( false );
            throw exception;
        }
    }

    private synchronized void queueMessage( Message msg, Collector collector ) throws
            PackStreamException,
            ConnectionException
    {
        ensureNotInterrupted();

        pendingMessages.add( msg );
        responseHandler.appendResultCollector( collector );
    }

    @Override
    public void close() throws ConnectionException.ImproperlyClosed
    {
        socket.stop();
    }

    @Override
    public boolean isOpen()
    {
        return socket.isOpen();
    }

    @Override
    public void onError( Runnable runnable )
    {
        throw new UnsupportedOperationException( "Error subscribers are not supported on SocketConnection." );
    }

    @Override
    public boolean hasUnrecoverableErrors()
    {
        throw new UnsupportedOperationException( "Unrecoverable error detection is not supported on SocketConnection." );
    }

    @Override
    public synchronized void resetAsync() throws PackStreamException, ConnectionException
    {
        queueMessage( RESET, new Collector.ResetCollector()
        {
            @Override
            public void doneSuccess()
            {
                isInterrupted.set( false );
                isAckFailureMuted.set( false );
            }
        } );
        flush();
        isInterrupted.set( true );
        isAckFailureMuted.set( true );
    }

    @Override
    public boolean isAckFailureMuted()
    {
        return isAckFailureMuted.get();
    }

    @Override
    public String server()
    {
        return initCollector.server(  );
    }

    @Override
    public BoltServerAddress address()
    {
        return this.socket.address();
    }

    @Override
    public Logger logger()
    {
        return this.logger;
    }
}
