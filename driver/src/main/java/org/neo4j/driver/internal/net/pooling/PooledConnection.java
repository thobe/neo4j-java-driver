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
package org.neo4j.driver.internal.net.pooling;

import java.util.Map;

import org.neo4j.driver.internal.exceptions.ConnectionException;
import org.neo4j.driver.internal.exceptions.InternalException;
import org.neo4j.driver.internal.exceptions.PackStreamException;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Collector;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.internal.util.Clock;
import org.neo4j.driver.internal.util.Consumer;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.Value;
/**
 * The state of a pooledConnection from a pool point of view could be one of the following:
 * Created,
 * Available,
 * Claimed,
 * Closed,
 * Disposed.
 *
 * The state machine looks like:
 *
 *                      session.finalize
 *                       session.close     failed return to pool
 * Created -------> Claimed  ----------> Closed ---------> Disposed
 *                    ^                    |                    ^
 *      pool.acquire  |                    |returned to pool    |
 *                    |                    |                    |
 *                    ---- Available <-----                     |
 *                              |           pool.close          |
 *                              ---------------------------------
 */
public class PooledConnection implements Connection
{
    /** The real connection who will do all the real jobs */
    private final Connection delegate;
    private final Consumer<PooledConnection> release;

    private boolean unrecoverableErrorsOccurred = false;
    private Runnable onError = null;
    private final Clock clock;
    private long lastUsed;

    public PooledConnection( Connection delegate, Consumer<PooledConnection> release, Clock clock )
    {
        this.delegate = delegate;
        this.release = release;
        this.clock = clock;
        this.lastUsed = clock.millis();
    }

    public void updateTimestamp()
    {
        lastUsed = clock.millis();
    }

    @Override
    public void init( String clientName, Map<String,Value> authToken ) throws PackStreamException, ConnectionException
    {
        try
        {
            delegate.init( clientName, authToken );
        }
        catch ( PackStreamException e )
        {
            throw onDelegateException( e );
        }
        catch ( ConnectionException e )
        {
            throw onDelegateException( e );
        }
    }

    @Override
    public void run( String statement, Map<String,Value> parameters, Collector collector ) throws
            PackStreamException,
            ConnectionException
    {
        try
        {
            delegate.run( statement, parameters, collector );
        }
        catch ( PackStreamException e )
        {
            throw onDelegateException( e );
        }
        catch ( ConnectionException e )
        {
            throw onDelegateException( e );
        }
    }

    @Override
    public void discardAll( Collector collector ) throws PackStreamException, ConnectionException
    {
        try
        {
            delegate.discardAll( collector );
        }
        catch ( PackStreamException e )
        {
            throw onDelegateException( e );
        }
        catch ( ConnectionException e )
        {
            throw onDelegateException( e );
        }
    }

    @Override
    public void pullAll( Collector collector ) throws PackStreamException, ConnectionException
    {
        try
        {
            delegate.pullAll( collector );
        }
        catch ( PackStreamException e )
        {
            throw onDelegateException( e );
        }
        catch ( ConnectionException e )
        {
            throw onDelegateException( e );
        }
    }

    @Override
    public void reset() throws PackStreamException, ConnectionException
    {
        try
        {
            delegate.reset();
        }
        catch ( PackStreamException e )
        {
            throw onDelegateException( e );
        }
        catch ( ConnectionException e )
        {
            throw onDelegateException( e );
        }
    }

    @Override
    public void ackFailure() throws PackStreamException, ConnectionException
    {
        try
        {
            delegate.ackFailure();
        }
        catch ( PackStreamException e )
        {
            throw onDelegateException( e );
        }
        catch ( ConnectionException e )
        {
            throw onDelegateException( e );
        }
    }

    @Override
    public void sync() throws PackStreamException, ConnectionException
    {
        try
        {
            delegate.sync();
        }
        catch ( PackStreamException e )
        {
            throw onDelegateException( e );
        }
        catch ( ConnectionException e )
        {
            throw onDelegateException( e );
        }
    }

    @Override
    public void flush() throws PackStreamException, ConnectionException
    {
        try
        {
            delegate.flush();
        }
        catch ( PackStreamException e )
        {
            throw onDelegateException( e );
        }
        catch ( ConnectionException e )
        {
            throw onDelegateException( e );
        }
    }

    @Override
    public void receiveOne() throws PackStreamException, ConnectionException
    {
        try
        {
            delegate.receiveOne();
        }
        catch ( PackStreamException e )
        {
            throw onDelegateException( e );
        }
        catch ( ConnectionException e )
        {
            throw onDelegateException( e );
        }
    }

    /**
     * Make sure only close the connection once on each session to avoid releasing the connection twice, a.k.a.
     * adding back the connection twice into the pool.
     */
    @Override
    public void close()
    {
        release.accept( this );
        // put the full logic of deciding whether to dispose the connection or to put it back to
        // the pool into the release object
    }

    @Override
    public boolean isOpen()
    {
        return delegate.isOpen();
    }

    public boolean hasUnrecoverableErrors()
    {
        return unrecoverableErrorsOccurred;
    }

    @Override
    public void resetAsync() throws PackStreamException, ConnectionException
    {
        try
        {
            delegate.resetAsync();
        }
        catch ( PackStreamException e )
        {
            throw onDelegateException( e );
        }
        catch ( ConnectionException e )
        {
            throw onDelegateException( e );
        }
    }

    @Override
    public boolean isAckFailureMuted()
    {
        return delegate.isAckFailureMuted();
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

    public void dispose() throws ConnectionException.ImproperlyClosed
    {
        delegate.close();
    }

    private ConnectionException onDelegateException( ConnectionException e )
    {
        return handleDelegateException( e );
    }

    private PackStreamException onDelegateException( PackStreamException e )
    {
        return handleDelegateException( e );
    }

    /**
     * If something goes wrong with the delegate, we want to figure out if this "wrong" is something that means
     * the connection is screwed (and thus should be evicted from the pool), or if it's something that we can
     * safely recover from.
     * @param e the exception the delegate threw
     */
    private <EX extends InternalException> EX handleDelegateException( EX e )
    {
        if ( e instanceof PackStreamException.ServerFailure && ((PackStreamException.ServerFailure) e).isUnrecoverable())
        {
            unrecoverableErrorsOccurred = true;
        }
        else if( !isAckFailureMuted() )
        {
            try
            {
                ackFailure();
            }
            catch ( PackStreamException | ConnectionException x )
            {
                e.addSuppressed( x );
            }
        }
        if( onError != null )
        {
            onError.run();
        }
        return e;
    }

    @Override
    public void onError( Runnable runnable )
    {
        this.onError = runnable;
    }

    public long idleTime()
    {
        return clock.millis() - lastUsed;
    }
}
