package org.neo4j.driver.internal.exceptions;

import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonWritableChannelException;

import org.neo4j.driver.internal.packstream.PackType;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.DatabaseException;
import org.neo4j.driver.v1.exceptions.TransientException;

public abstract class PackStreamException extends InternalException
{
    private PackStreamException()
    {
    }

    private PackStreamException( Exception cause )
    {
        super( cause instanceof TunnellingSSLException ? ((TunnellingSSLException)cause).getCause() : cause );
    }

    public static abstract class SerializationFailure extends PackStreamException
    {
        private SerializationFailure()
        {
        }

        private SerializationFailure( Exception cause )
        {
            super( cause );
        }
    }

    public static abstract class DeserializationFailure extends PackStreamException
    {
        private DeserializationFailure()
        {
        }

        private DeserializationFailure( Exception cause )
        {
            super( cause );
        }
    }

    public RuntimeException publicException()
    {
        return new UnsupportedOperationException( "not implemented", this );
    }

    public static class InputFailure extends DeserializationFailure
    {
        public InputFailure( Exception cause )
        {
            super( cause );
        }

        private InputFailure()
        {
        }

        @Override
        public RuntimeException publicException()
        {
            return super.publicException();
        }

        @Override
        public  String getMessage()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }

    public static class OutputFailure extends SerializationFailure
    {
        public OutputFailure( Exception cause )
        {
            super( cause );
        }

        @Override
        public RuntimeException publicException()
        {
            Exception cause = (Exception) getCause();
            if ( cause instanceof NonWritableChannelException )
            {
                NonWritableChannelException e = (NonWritableChannelException) cause;
            }
            if ( cause instanceof ClosedChannelException )
            {
                ClosedChannelException e = (ClosedChannelException) cause;
            }
            if ( cause instanceof AsynchronousCloseException )
            {
                AsynchronousCloseException e = (AsynchronousCloseException) cause;
            }
            if ( cause instanceof ClosedByInterruptException )
            {
                ClosedByInterruptException e = (ClosedByInterruptException) cause;
            }
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public String getMessage()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }

    public static class EndOfStream extends InputFailure
    {
        private final int expectedBytes;

        public EndOfStream( int expectedBytes )
        {
            this.expectedBytes = expectedBytes;
        }

        @Override
        public RuntimeException publicException()
        {
            return super.publicException();
        }

        @Override
        public String getMessage()
        {
            return String.format(
                    "Expected %d bytes available, but no more bytes accessible from underlying stream.",
                    expectedBytes );
        }
    }

    public static class InvalidChunkSize extends InputFailure
    {
        private final int chunkSize;

        public InvalidChunkSize( int chunkSize )
        {
            this.chunkSize = chunkSize;
        }

        @Override
        public String getMessage()
        {
            return "Invalid chunk size: " + chunkSize;
        }
    }

    public static class UnexpectedData extends InputFailure
    {
        private final String contentHex;
        private final int size;

        public UnexpectedData( String contentHex, int size )
        {
            this.contentHex = contentHex;
            this.size = size;
        }

        @Override
        public String getMessage()
        {
            return "left in the message content unread: buffer [" +
                    contentHex + "], unread chunk size " + size;
        }
    }

    public static class StructureFieldOverflow extends SerializationFailure
    {
        private static final int MAX = (1 << 16) - 1;
        private final int size;

        public StructureFieldOverflow( int size )
        {
            this.size = size;
        }

        @Override
        public String getMessage()
        {
            return String.format( "Structures cannot have more than %d fields, requested %d", MAX, size );
        }
    }

    public static class InvalidStructureSignature extends DeserializationFailure
    {
        private final String structName;
        private final byte expected;
        private final byte actual;

        public InvalidStructureSignature( String structName, byte expected, byte actual )
        {
            this.structName = structName;
            this.expected = expected;
            this.actual = actual;
        }

        @Override
        public String getMessage()
        {
            return String.format(
                    "Invalid message received, expected a `%s`, signature 0x%s. Recieved signature was 0x%s.",
                    structName, Integer.toHexString( expected ), Integer.toHexString( actual ) );
        }
    }

    public static class InvalidStructSize extends DeserializationFailure
    {
        private final String structName;
        private final int expected;
        private final long actual;

        public InvalidStructSize( String structName, int expected, long actual )
        {
            this.structName = structName;
            this.expected = expected;
            this.actual = actual;
        }

        @Override
        public String getMessage()
        {
            return String.format(
                    "Invalid message received, serialized %s structures should have %d fields, "
                            + "received %s structure has %d fields.",
                    structName, expected, structName, actual );
        }
    }

    public static class CannotRepresent extends DeserializationFailure
    {
        private final String type;
        private final long size;

        public CannotRepresent( String type, long size )
        {
            this.type = type;
            this.size = size;
        }

        @Override
        public String getMessage()
        {
            return String.format( "%s of size %d is too long for Java", type, size );
        }
    }

    public static class UnsupportedType extends DeserializationFailure
    {
        private final PackType type;

        public UnsupportedType( PackType type )
        {
            this.type = type;
        }

        @Override
        public String getMessage()
        {
            return "Unknown value type: " + type;
        }
    }

    public static class UnexpectedType extends DeserializationFailure
    {
        private final String type;
        private final byte[] expected;
        private final byte actual;

        public UnexpectedType( byte actual, String type, byte... expected )
        {
            this.actual = actual;
            this.type = type;
            this.expected = expected;
        }

        @Override
        public String getMessage()
        {
            StringBuilder message = new StringBuilder().append( "Expected a " ).append( type );
            if ( expected != null )
            {
                String sep = " (denoted by ";
                for ( int i = 0; i < expected.length; i++ )
                {
                    if ( i == (expected.length - 1) && i > 0 )
                    {
                        message.append( " or " );
                    }
                    else
                    {
                        message.append( sep );
                    }
                    message.append( Integer.toHexString( 0xFF & expected[i] ) );
                    sep = ", ";
                }
                if ( expected.length > 0 )
                {
                    message.append( ")" );
                }
            }
            return message.append( ", but got: " ).append( Integer.toHexString( 0xFF & actual ) ).toString();
        }
    }

    public static class UnexpectedMessage extends DeserializationFailure
    {
        private final int type;

        public UnexpectedMessage( int type )
        {
            this.type = type;
        }

        @Override
        public String getMessage()
        {
            return "Unknown message type: " + type;
        }
    }

    public static class Unpackable extends SerializationFailure
    {
        private final String type, value;

        public Unpackable( Object value )
        {
            if ( value instanceof Value )
            {
                Value val = (Value) value;
                this.type = val.type().name();
                this.value = val.toString();
            }
            else
            {
                this.type = value.getClass().getName();
                this.value = value.toString();
            }
        }

        @Override
        public String getMessage()
        {
            return String.format( "Cannot pack <%s> of type %s", value, type );
        }
    }

    public static class ServerFailure extends PackStreamException
    {
        private final String code;
        private final String message;

        public ServerFailure( String code, String message )
        {
            this.code = code;
            this.message = message;
        }

        @Override
        public RuntimeException publicException()
        {
            String[] parts = code.split( "\\." );
            String classification = parts[1];
            switch ( classification )
            {
            case "ClientError":
                return new ClientException( code, message );
            case "TransientError":
                return new TransientException( code, message );
            default:
                return new DatabaseException( code, message );
            }
        }

        @Override
        public String getMessage()
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        public boolean isProtocolViolation()
        {
            return code.startsWith( "Neo.ClientError.Request" );
        }

        public boolean isUnrecoverable()
        {
            return isProtocolViolation() || !(code.contains( "ClientError" ) || code.contains( "TransientError" ));
        }
    }
}
