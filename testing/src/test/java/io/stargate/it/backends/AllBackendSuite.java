package io.stargate.it.backends;

import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

import io.stargate.it.CQLTest;
import io.stargate.it.PersistenceTest;
import io.stargate.it.http.GraphqlTest;
import io.stargate.it.http.RestApiTest;
import io.stargate.it.http.RestApiv2Test;

/**
 * Allows a suite of tests to run against
 * a number of persistence backends.
 * Each backend should extend this class.
 */
@Suite.SuiteClasses({ PersistenceTest.class, RestApiTest.class, RestApiv2Test.class, GraphqlTest.class, CQLTest.class })
public abstract class AllBackendSuite
{
    @Parameterized.Parameter(0)
    public String dockerImage;

    @Parameterized.Parameter(1)
    public Boolean isDse;

    @Parameterized.Parameter(2)
    public String version;
}
