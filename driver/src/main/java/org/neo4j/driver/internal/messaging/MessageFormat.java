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
package org.neo4j.driver.internal.messaging;

import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.neo4j.driver.internal.exceptions.PackStreamException;

public interface MessageFormat
{
    interface Writer
    {
        interface CompletionHandler
        {
            void run() throws PackStreamException.OutputFailure;
        }

        Writer write( Message msg ) throws PackStreamException.SerializationFailure;

        Writer flush() throws PackStreamException.OutputFailure;

        Writer reset( WritableByteChannel channel );
    }

    interface Reader
    {
        interface CompletionHandler
        {
            void run() throws PackStreamException.InputFailure;
        }

        /**
         * Return true is there is another message in the underlying buffer
         */
        boolean hasNext() throws PackStreamException.InputFailure;

        <Failure extends Exception> void read( MessageHandler<Failure> handler ) throws
                PackStreamException.DeserializationFailure,
                Failure;
    }

    Writer newWriter( WritableByteChannel ch );

    Reader newReader( ReadableByteChannel ch );

    int version();
}
