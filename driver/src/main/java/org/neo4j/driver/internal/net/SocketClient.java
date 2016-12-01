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

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SocketChannel;
import java.util.Queue;

import org.neo4j.driver.internal.exceptions.ConnectionException;
import org.neo4j.driver.internal.exceptions.PackStreamException;
import org.neo4j.driver.internal.exceptions.TunnellingSSLException;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.security.SecurityPlan;
import org.neo4j.driver.internal.security.TLSSocketChannel;
import org.neo4j.driver.internal.util.BytePrinter;
import org.neo4j.driver.v1.Logger;
import org.neo4j.driver.v1.exceptions.ClientException;

import static java.nio.ByteOrder.BIG_ENDIAN;

public class SocketClient
{
    private static final int MAGIC_PREAMBLE = 0x6060B017;
    private static final int VERSION1 = 1;
    private static final int HTTP = 1213486160;//== 0x48545450 == "HTTP"
    private static final int NO_VERSION = 0;
    private static final int[] SUPPORTED_VERSIONS = new int[]{VERSION1, NO_VERSION, NO_VERSION, NO_VERSION};

    private final BoltServerAddress address;
    private final SecurityPlan securityPlan;
    private final Logger logger;

    private SocketProtocol protocol;
    private MessageFormat.Reader reader;
    private MessageFormat.Writer writer;

    private ByteChannel channel;

    public SocketClient( BoltServerAddress address, SecurityPlan securityPlan, Logger logger )
    {
        this.address = address;
        this.securityPlan = securityPlan;
        this.logger = logger;
        this.channel = null;
    }

    void setChannel( ByteChannel channel )
    {
        this.channel = channel;
    }

    void blockingRead( ByteBuffer buf ) throws ConnectionException.ReadFailure, ConnectionException.SSLFailure
    {
        while(buf.hasRemaining())
        {
            try
            {
                if ( channel.read( buf ) < 0 )
                {
                    ConnectionException.EndOfStream eos = new ConnectionException.EndOfStream(
                            buf.limit(), BytePrinter.hex( buf ).trim() );
                    try
                    {
                        channel.close();
                    }
                    catch ( IOException e )
                    {
                        // best effort
                        eos.addSuppressed( e );
                    }
                    throw eos;
                }
            }
            catch ( TunnellingSSLException e )
            {
                throw e.getCause();
            }
            catch ( IOException e )
            {
                throw new ConnectionException.ReadFailure( e );
            }
        }
    }

    void blockingWrite( ByteBuffer buf ) throws ConnectionException.WriteFailure, ConnectionException.SSLFailure
    {
        while(buf.hasRemaining())
        {
            try
            {
                if ( channel.write( buf ) < 0 )
                {
                    ConnectionException.ConnectionClosed closed =
                            new ConnectionException.ConnectionClosed( buf.limit(), BytePrinter.hex( buf ).trim() );
                    try
                    {
                        channel.close();
                    }
                    catch ( IOException e )
                    {
                        // best effort
                        closed.addSuppressed( e );
                    }
                    throw closed;
                }
            }
            catch ( TunnellingSSLException e )
            {
                throw e.getCause();
            }
            catch ( IOException e )
            {
                throw new ConnectionException.WriteFailure( e );
            }
        }
    }

    public void start() throws ConnectionException
    {
        logger.debug( "~~ [CONNECT] %s", address );
        setChannel( ChannelFactory.create( address, securityPlan, logger ) );
        protocol = negotiateProtocol();
        reader = protocol.reader();
        writer = protocol.writer();
    }

    public void send( Queue<Message> messages ) throws PackStreamException.SerializationFailure
    {
        int messageCount = 0;
        while ( true )
        {
            Message message = messages.poll();
            if ( message == null )
            {
                break;
            }
            else
            {
                logger.debug( "C: %s", message );
                writer.write( message );
                messageCount += 1;
            }
        }
        if ( messageCount > 0 )
        {
            writer.flush();
        }
    }

    public void receiveAll( SocketResponseHandler handler ) throws
            PackStreamException.DeserializationFailure,
            PackStreamException.ServerFailure,
            ConnectionException.ImproperlyClosed
    {
        // Wait until all pending requests have been replied to
        while ( handler.collectorsWaiting() > 0 )
        {
            receiveOne( handler );
        }
    }

    public void receiveOne( SocketResponseHandler handler ) throws
            PackStreamException.DeserializationFailure,
            PackStreamException.ServerFailure,
            ConnectionException.ImproperlyClosed
    {
        reader.read( handler );

        // Stop immediately if bolt protocol error happened on the server
        if ( handler.protocolViolationErrorOccurred() )
        {
            stop();
            throw handler.serverFailure();
        }
    }

    public void stop() throws ConnectionException.ImproperlyClosed
    {
        if ( channel != null )
        {
            try
            {
                channel.close();
            }
            catch ( IOException e )
            {
                if ( !e.getMessage().equals( "An existing connection was forcibly closed by the remote host" ) )
                {
                    // Swallow exceptions due to connection already closed by server, otherwise:
                    throw new ConnectionException.ImproperlyClosed( e );
                }
            }
            finally
            {
                setChannel( null );
                logger.debug( "~~ [DISCONNECT]" );
            }
        }
    }

    public boolean isOpen()
    {
        return channel != null && channel.isOpen();
    }

    private SocketProtocol negotiateProtocol() throws ConnectionException
    {
        //Propose protocol versions
        ByteBuffer buf = ByteBuffer.allocate( 5 * 4 ).order( BIG_ENDIAN );
        logger.debug( "C: [HANDSHAKE] 0x6060B017" );
        buf.putInt( MAGIC_PREAMBLE );
        logger.debug( "C: [HANDSHAKE] [1, 0, 0, 0]" );
        for ( int version : SUPPORTED_VERSIONS )
        {
            buf.putInt( version );
        }
        buf.flip();

        blockingWrite( buf );

        // Read (blocking) back the servers choice
        buf.clear();
        buf.limit( 4 );
        try
        {
            blockingRead( buf );
        }
        catch ( ConnectionException.ReadFailure readFailure )
        {
            if ( buf.position() == 0 )
            {
                throw new ConnectionException.CannotConnect( address, readFailure );
            }
            throw readFailure;
        }
        // Choose protocol, or fail
        buf.flip();
        final int proposal = buf.getInt();
        switch ( proposal )
        {
        case VERSION1:
            logger.debug( "S: [HANDSHAKE] -> 1" );
            return new SocketProtocolV1( channel );
        case NO_VERSION:
            throw new ClientException( "The server does not support any of the protocol versions supported by " +
                                       "this driver. Ensure that you are using driver and server versions that " +
                                       "are compatible with one another." );
        case HTTP:
            throw new ClientException(
                    "Server responded HTTP. Make sure you are not trying to connect to the http endpoint " +
                    "(HTTP defaults to port 7474 whereas BOLT defaults to port 7687)" );
        default:
            throw new ClientException( "Protocol error, server suggested unexpected protocol version: " +
                                       proposal );
        }
    }

    @Override
    public String toString()
    {
        int version = protocol == null ? -1 : protocol.version();
        return "SocketClient[protocolVersion=" + version + "]";
    }

    private static class ChannelFactory
    {
        public static ByteChannel create( BoltServerAddress address, SecurityPlan securityPlan, Logger logger )
                throws ConnectionException
        {
            ByteChannel channel;
            try
            {
                SocketChannel soChannel = SocketChannel.open();
                soChannel.setOption( StandardSocketOptions.SO_REUSEADDR, true );
                soChannel.setOption( StandardSocketOptions.SO_KEEPALIVE, true );
                soChannel.connect( address.toSocketAddress() );
                channel = soChannel;
            }
            catch ( Exception e )
            {
                throw new ConnectionException.CannotConnect( address, e );
            }

            if (securityPlan.requiresEncryption())
            {
                channel = new TLSSocketChannel( address, securityPlan, channel, logger );
            }

            if ( logger.isTraceEnabled() )
            {
                channel = new LoggingByteChannel( channel, logger );
            }

            return channel;
        }
    }

    public BoltServerAddress address()
    {
        return address;
    }
}
