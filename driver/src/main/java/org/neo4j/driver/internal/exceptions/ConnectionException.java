package org.neo4j.driver.internal.exceptions;

import java.io.IOException;
import java.net.ConnectException;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;

import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;

public abstract class ConnectionException extends InternalException
{
    private ConnectionException()
    {
    }

    private ConnectionException( Throwable cause )
    {
        super( cause );
    }

    public static class CannotConnect extends ConnectionException
    {
        private final BoltServerAddress address;

        public CannotConnect( BoltServerAddress address, Exception e )
        {
            super( e );
            this.address = address;
        }

        @Override
        public RuntimeException publicException()
        {
            Throwable cause = getCause();
            if ( cause instanceof ConnectException )
            {
                return new ServiceUnavailableException( String.format(
                        "Unable to connect to %s, ensure the database is running and that there is a " +
                                "working network connection to it.", address ), cause );
            }
            if ( cause instanceof IOException )
            {
                return new ClientException( "Unable to process request: " + cause.getMessage(), cause );
            }
            if ( cause instanceof ReadFailure )
            {
                throw new ClientException( String.format(
                        "Failed to establish connection with server. Make sure that you have a server with bolt " +
                                "enabled on %s", address ) );
            }
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public String getMessage()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }

    public static class ReadFailure extends ConnectionException
    {
        public ReadFailure( IOException cause )
        {
            super( cause );
        }

        private ReadFailure()
        {
        }

        @Override
        public RuntimeException publicException()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public String getMessage()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }

    public static class EndOfStream extends ReadFailure
    {
        private final int expected;
        private final String data;

        public EndOfStream( int expected, String data )
        {
            this.expected = expected;
            this.data = data == null || data.isEmpty() ? null : data;
        }

        @Override
        public String getMessage()
        {
            return String.format(
                    "Connection terminated while receiving data. This can happen due to network " +
                            "instabilities, or due to restarts of the database. Expected %s bytes, received %s.",
                    expected,
                    data == null ? "none" : data );
        }
    }

    public static class WriteFailure extends ConnectionException
    {
        public WriteFailure( IOException cause )
        {
            super( cause );
        }

        private WriteFailure()
        {
        }

        @Override
        public RuntimeException publicException()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public String getMessage()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }

    public static class ConnectionClosed extends WriteFailure {
        private final int expected;
        private final String data;

        public ConnectionClosed( int expected, String data )
        {
            this.expected = expected;
            this.data = data == null || data.isEmpty() ? null : data;
        }

        @Override
        public String getMessage()
        {
            return String.format(
                    "Connection terminated while sending data. This can happen due to network " +
                            "instabilities, or due to restarts of the database. Expected %s bytes, wrote %s.",
                    expected, data == null ? "none" :data );
        }
    }

    public static class SSLFailure extends ConnectionException
    {
        public SSLFailure( SSLException e )
        {
            super( e );
        }

        private SSLFailure( IOException e )
        {
            super( e );
        }

        private SSLFailure()
        {
        }

        @Override
        public RuntimeException publicException()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public String getMessage()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        public IOException asIOException()
        {
            Throwable cause = getCause();
            if ( cause instanceof IOException )
            {
                return (IOException) cause;
            }
            return new TunnellingSSLException( this );
        }

        public static class UnableToRead extends SSLFailure
        {
            public UnableToRead( IOException e )
            {
                super( e );
            }
        }

        public static class UnableToWrite extends SSLFailure
        {
            public UnableToWrite( IOException e )
            {
                super( e );
            }
        }

        public static class InvalidStatus extends SSLFailure
        {
            private final SSLEngineResult.Status status;
            private final SSLEngineResult.HandshakeStatus handshakeStatus;
            private final int bytesConsumed, bytesProduced;

            public InvalidStatus( SSLEngineResult.Status status )
            {
                this.status = status;
                this.handshakeStatus = null;
                this.bytesProduced = this.bytesConsumed = 0;
            }

            public InvalidStatus( SSLEngineResult.Status status, SSLEngineResult unwrapResult )
            {
                this.status = status;
                this.handshakeStatus = unwrapResult.getHandshakeStatus();
                this.bytesConsumed = unwrapResult.bytesConsumed();
                this.bytesProduced = unwrapResult.bytesProduced();
            }

            @Override
            public String getMessage()
            {
                if ( handshakeStatus == null )
                {
                    return "Got unexpected status " + status;
                }
                else
                {
                    return "Got unexpected status " + status + "; HandshakeStatus:" + handshakeStatus +
                            ", bytesConsumed=" + bytesConsumed + ", bytesProduced=" + bytesProduced;
                }
            }
        }

        public static class OutputBufferOverflow extends SSLFailure
        {
            private final int currentSize;
            private final int requiredSize;
            private final int capacity;

            public OutputBufferOverflow( int currentSize, int requiredSize, int capacity )
            {
                this.currentSize = currentSize;
                this.requiredSize = requiredSize;
                this.capacity = capacity;
            }

            @Override
            public String getMessage()
            {
                return String.format( "Failed to enlarge network buffer from %s to %s. This is either because the " +
                        "new size is however less than the old size, or because the application " +
                        "buffer size %s is so big that the application data still cannot fit into the " +
                        "new network buffer.", currentSize, requiredSize, capacity );
            }
        }

        public static class InputBufferOverflow extends SSLFailure
        {
            private final int currentSize;
            private final int requiredSize;
            private final int maxSize;

            public InputBufferOverflow( int currentSize, int requiredSize, int maxSize )
            {
                this.currentSize = currentSize;
                this.requiredSize = requiredSize;
                this.maxSize = maxSize;
            }

            @Override
            public String getMessage()
            {
                return String.format( "Failed ro enlarge application input buffer from %s to %s, as the maximum " +
                                "buffer size allowed is %s.",
                        currentSize, requiredSize, maxSize );
            }
        }

        public static class Terminated extends SSLFailure
        {
            @Override
            public String getMessage()
            {
                return "SSL Connection terminated while receiving data. " +
                        "This can happen due to network instabilities, or due to restarts of the database.";
            }
        }
    }

    public static class ImproperlyClosed extends ConnectionException
    {
        public ImproperlyClosed( IOException cause )
        {
            super(cause);
        }

        @Override
        public RuntimeException publicException()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public String getMessage()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }
}
