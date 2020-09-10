package io.stargate.it.backends;

import java.util.Collections;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.github.peterwippermann.junit4.parameterizedsuite.ParameterizedSuite;

@RunWith(ParameterizedSuite.class)
public class Cassandra311Backend extends AllBackendSuite
{
    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> functions()
    {
        return Collections.singletonList(new Object[]{ "cassandra:3.11.6", false, "3.11" });
    }
}
