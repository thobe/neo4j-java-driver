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

import java.util.Map;

import org.neo4j.driver.v1.Value;

public interface MessageHandler<Failure extends Exception>
{
    // Requests
    void handleInitMessage( String clientNameAndVersion, Map<String,Value> authToken ) throws Failure;

    void handleRunMessage( String statement, Map<String,Value> parameters ) throws Failure;

    void handlePullAllMessage() throws Failure;

    void handleDiscardAllMessage() throws Failure;

    void handleResetMessage() throws Failure;

    void handleAckFailureMessage() throws Failure;

    // Responses
    void handleSuccessMessage( Map<String,Value> meta ) throws Failure;

    void handleRecordMessage( Value[] fields ) throws Failure;

    void handleFailureMessage( String code, String message ) throws Failure;

    void handleIgnoredMessage() throws Failure;

}
