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
package org.neo4j.driver.internal.exceptions;

/**
 * This is the base of all exceptions thrown in the internals of the implementation.
 * <p>
 * The user-facing exceptions in {@link org.neo4j.driver.v1.exceptions} should only be thrown from the very surface
 * level of the implementation, and at that point the {@link #publicException()} method should be used
 */
public abstract class InternalException extends Exception
{
    InternalException()
    {
    }

    InternalException( Throwable cause )
    {
        super( cause );
    }

    public abstract RuntimeException publicException();

    @Override
    public abstract String getMessage();
}
