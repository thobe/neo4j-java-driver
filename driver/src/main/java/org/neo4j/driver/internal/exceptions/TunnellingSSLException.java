package org.neo4j.driver.internal.exceptions;

import java.io.IOException;

public class TunnellingSSLException extends IOException
{
    TunnellingSSLException( ConnectionException.SSLFailure cause )
    {
        super( cause );
    }

    @Override
    public ConnectionException.SSLFailure getCause()
    {
        return (ConnectionException.SSLFailure) super.getCause();
    }

    @Override
    public Throwable fillInStackTrace()
    {
        return this;
    }
}
