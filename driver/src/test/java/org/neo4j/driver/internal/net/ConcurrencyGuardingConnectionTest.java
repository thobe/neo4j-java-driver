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

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.driver.internal.exceptions.PackStreamException;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.util.Function;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

@RunWith( Parameterized.class )
public class ConcurrencyGuardingConnectionTest
{
    public interface Operation
    {
        void perform( Connection connection ) throws PackStreamException;
    }
    
    @Parameterized.Parameter
    public Operation operation;

    @Parameterized.Parameters
    public static List<Object[]> params()
    {
        return asList(
                new Object[]{INIT},
                new Object[]{RUN},
                new Object[]{PULL_ALL},
                new Object[]{DISCARD_ALL},
                new Object[]{CLOSE},
                new Object[]{RECIEVE_ONE},
                new Object[]{FLUSH},
                new Object[]{SYNC});
    }

    @Test
    public void shouldNotAllowConcurrentAccess() throws Throwable
    {
        // Given
        final AtomicReference<Connection> conn = new AtomicReference<>();
        final AtomicReference<ClientException> exception = new AtomicReference<>();

        Connection delegate = mock( Connection.class, new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                try
                {
                    operation.perform( conn.get() );
                    fail("Expected this call to fail, because it is calling a method on the connector while 'inside' " +
                         "a connector call already.");
                } catch(ClientException e)
                {
                    exception.set( e );
                }
                return null;
            }
        });

        conn.set(new ConcurrencyGuardingConnection( delegate ));

        // When
        operation.perform( conn.get() );

        // Then
        assertThat( exception.get().getMessage(), equalTo(
                "You are using a session from multiple locations at the same time, " +
                "which is not supported. If you want to use multiple threads, you should ensure " +
                "that each session is used by only one thread at a time. One way to " +
                "do that is to give each thread its own dedicated session.") );
    }

    public static final Operation INIT = new Operation()
    {
        @Override
        public void perform( Connection connection ) throws PackStreamException
        {
            connection.init(null, null);
        }
    };

    public static final Operation RUN = new Operation()
    {
        @Override
        public void perform( Connection connection ) throws PackStreamException
        {
            connection.run(null, null, null);
        }
    };

    public static final Operation DISCARD_ALL = new Operation()
    {
        @Override
        public void perform( Connection connection ) throws PackStreamException
        {
            connection.discardAll(null);
        }
    };

    public static final Operation PULL_ALL = new Operation()
    {
        @Override
        public void perform( Connection connection ) throws PackStreamException
        {
            connection.pullAll(null);
        }
    };

    public static final Operation RECIEVE_ONE = new Operation()
    {
        @Override
        public void perform( Connection connection ) throws PackStreamException
        {
            connection.receiveOne();
        }
    };

    public static final Operation CLOSE = new Operation()
    {
        @Override
        public void perform( Connection connection ) throws PackStreamException
        {
            connection.close();
        }
    };

    public static final Operation SYNC = new Operation()
    {
        @Override
        public void perform( Connection connection ) throws PackStreamException
        {
            connection.sync();
        }
    };

    public static final Operation FLUSH = new Operation()
    {
        @Override
        public void perform( Connection connection ) throws PackStreamException
        {
            connection.flush();
        }
    };
}
