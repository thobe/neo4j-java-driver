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

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.driver.internal.exceptions.ConnectionException;
import org.neo4j.driver.internal.exceptions.PackStreamException;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;

/**
 * This class ensures there can only ever be one thread using a connection at
 * the same time. Rather than doing this through synchronization, we do it by
 * throwing errors, because connections are not meant to be thread safe -
 * we simply want to inform the application it is using the session incorrectly.
 */
public class ConcurrencyGuardingConnection implements Connection
{
    private final Connection delegate;
    private final AtomicBoolean inUse = new AtomicBoolean( false );

    public ConcurrencyGuardingConnection( Connection delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public void init( String clientName, Map<String,Value> authToken ) throws PackStreamException
    {
        try
        {
            markAsInUse();
            delegate.init(clientName, authToken);
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void run( String statement, Map<String,Value> parameters,
            Collector collector ) throws PackStreamException
    {
        try
        {
            markAsInUse();
            delegate.run(statement, parameters, collector);
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void discardAll( Collector collector ) throws PackStreamException
    {
        try
        {
            markAsInUse();
            delegate.discardAll( collector );
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void pullAll( Collector collector ) throws PackStreamException
    {
        try
        {
            markAsInUse();
            delegate.pullAll(collector);
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void reset() throws PackStreamException
    {
        try
        {
            markAsInUse();
            delegate.reset();
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void ackFailure() throws PackStreamException
    {
        try
        {
            markAsInUse();
            delegate.ackFailure();
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void sync() throws PackStreamException
    {
        try
        {
            markAsInUse();
            delegate.sync();
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void flush() throws PackStreamException
    {
        try
        {
            markAsInUse();
            delegate.flush();
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void receiveOne() throws PackStreamException
    {
        try
        {
            markAsInUse();
            delegate.receiveOne();
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public void close() throws ConnectionException.ImproperlyClosed
    {
        try
        {
            markAsInUse();
            delegate.close();
        }
        finally
        {
            markAsAvailable();
        }
    }

    @Override
    public boolean isOpen()
    {
        return delegate.isOpen();
    }

    @Override
    public void onError( Runnable runnable )
    {
        delegate.onError( runnable );
    }

    @Override
    public boolean hasUnrecoverableErrors()
    {
        return delegate.hasUnrecoverableErrors();
    }

    @Override
    public void resetAsync() throws PackStreamException
    {
        delegate.resetAsync();
    }

    @Override
    public boolean isAckFailureMuted()
    {
        return delegate.isAckFailureMuted();
    }

    private void markAsAvailable()
    {
        inUse.set( false );
    }

    private void markAsInUse()
    {
        if(!inUse.compareAndSet( false, true ))
        {
            throw new ClientException( "You are using a session from multiple locations at the same time, " +
                                       "which is not supported. If you want to use multiple threads, you should ensure " +
                                       "that each session is used by only one thread at a time. One way to " +
                                       "do that is to give each thread its own dedicated session." );
        }
    }

    @Override
    public String server()
    {
        return delegate.server();
    }

    @Override
    public BoltServerAddress address()
    {
        return delegate.address();
    }

    @Override
    public Logger logger()
    {
        return delegate.logger();
    }
}
