package org.neo4j.driver.v1.stress;

import java.util.LinkedList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.ResultCursor;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.util.Neo4jRunner;
import org.neo4j.driver.v1.util.Neo4jSettings;

import static org.neo4j.driver.v1.util.Neo4jRunner.getOrCreateGlobalRunner;

public class ConnectionPoolStressTest
{
    public static final int NUM_RUNS = 100;
    public static final int NUM_THREADS = 10;
    public static final int POOL_SIZE = 20;

    private Neo4jRunner server;
    private LinkedList<Thread> threads = new LinkedList<>();

    @Before
    public void setUp() throws Exception
    {
        server = getOrCreateGlobalRunner();
        server.ensureRunning( Neo4jSettings.DEFAULT );
        Thread.sleep( 10000 );
    }

    @After
    public void tearDown() throws Exception
    {
        server.stop();
    }

    public static class Client implements Runnable
    {
        public Driver driver = GraphDatabase.driver( "bolt://localhost",
                Config.build().withMaxSessions( POOL_SIZE ).toConfig() );

        private LinkedList<Session> sessions = new LinkedList<>();
        private LinkedList<ResultCursor> results = new LinkedList<>();

        @Override
        public void run()
        {
            for ( int cycle = 0; cycle < NUM_RUNS; cycle += 1 )
            {
                acquireSessions( POOL_SIZE );
                runCypher();
                discardResults();
                releaseSessions();
            }
        }

        private void acquireSessions( int count )
        {
            for ( int i = 0; i < count; i += 1 )
            {
                Session session = driver.session();
                sessions.add( session );
            }
        }

        private void runCypher()
        {
            for ( Session session : sessions )
            {
                ResultCursor result = session.run( "UNWIND range(1, 1000) AS n RETURN n" );
                results.add( result );
            }
        }

        private void discardResults()
        {
            while ( !results.isEmpty() )
            {
                ResultCursor result = results.pop();
                result.close();
            }
        }

        private void releaseSessions()
        {
            while ( !sessions.isEmpty() )
            {
                Session session = sessions.pop();
                session.close();
            }
        }

    }

    @Test
    public void shouldBeAbleToAcquireAndReleaseLotsOfTimes() throws InterruptedException
    {
        createThreads(NUM_THREADS);
        startThreads();
        joinThreads();
    }

    private void createThreads(int numThreads)
    {
        for ( int i = 0; i < numThreads; i += 1 )
        {
            Client client = new Client();
            Thread thread = new Thread( client );
            threads.add( thread );
        }
    }

    private void startThreads()
    {
        for ( Thread thread : threads )
        {
            thread.start();
        }
    }

    private void joinThreads() throws InterruptedException
    {
        while ( !threads.isEmpty() )
        {
            Thread thread = threads.pop();
            thread.join();
        }
    }

}
