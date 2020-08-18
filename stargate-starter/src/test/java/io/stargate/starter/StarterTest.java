package io.stargate.starter;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StarterTest {


    @BeforeEach
    public void reset() {
        System.setProperties(null);
    }

    @Test
    void testSetStargatePropertiesWithIPSeedNode() {
        Starter starter = new Starter();
        starter.simpleSnitch = true;
        starter.seedList = Arrays.asList("127.0.0.1", "127.0.0.2");
        starter.clusterName = "foo";
        starter.version = "3.11";

        starter.setStargateProperties();

        assertThat(System.getProperty("stargate.seed_list")).isEqualTo("127.0.0.1,127.0.0.2");
    }

    @Test
    void testSetStargatePropertiesWithHostSeedNode() {
        Starter starter = new Starter();
        starter.simpleSnitch = true;
        starter.seedList = Arrays.asList("cassandra.apache.org", "datastax.com");
        starter.clusterName = "foo";
        starter.version = "3.11";
        starter.setStargateProperties();

        assertThat(System.getProperty("stargate.seed_list")).isEqualTo("cassandra.apache.org,datastax.com");
    }

    @Test
    void testSetStargatePropertiesWithBadHostSeedNode() {
        Starter starter = new Starter();
        starter.simpleSnitch = true;
        starter.seedList = Arrays.asList("google.com", "datasta.cmo", "cassandra.apache.org");
        starter.clusterName = "foo";
        starter.version = "3.11";
        RuntimeException thrown = assertThrows(
                RuntimeException.class,
                starter::setStargateProperties,
                "Expected setStargateProperties() to throw RuntimeException"
        );


        assertThat(System.getProperty("stargate.seed_list")).isNull();
        assertThat(thrown.getMessage()).isEqualTo("Unable to resolve seed node address datasta.cmo");
    }

    @Test
    void testSetStargatePropertiesMissingSeedNode() {
        Starter starter = new Starter();
        starter.simpleSnitch = true;
        starter.seedList = new ArrayList<>();
        starter.clusterName = "foo";
        starter.version = "3.11";

        RuntimeException thrown = assertThrows(
                IllegalArgumentException.class,
                starter::setStargateProperties,
                "Expected setStargateProperties() to throw RuntimeException"
        );

        assertThat(System.getProperty("stargate.seed_list")).isNull();
        assertThat(thrown.getMessage()).isEqualTo("At least one seed node address is required.");
    }

    @Test
    void testSetStargatePropertiesMissingDC() {
        Starter starter = new Starter();
        starter.simpleSnitch = false;
        starter.rack = "rack0";
        starter.clusterName = "foo";
        starter.version = "3.11";
        RuntimeException thrown = assertThrows(
                IllegalArgumentException.class,
                starter::setStargateProperties,
                "Expected setStargateProperties() to throw RuntimeException"
        );

        assertThat(thrown.getMessage()).isEqualTo("--dc and --rack are both required unless --simple-snitch is specified.");
    }

    @Test
    void testSetStargatePropertiesMissingRack() {
        Starter starter = new Starter();
        starter.simpleSnitch = false;
        starter.dc = "dc1";
        starter.clusterName = "foo";
        starter.version = "3.11";
        RuntimeException thrown = assertThrows(
                IllegalArgumentException.class,
                starter::setStargateProperties,
                "Expected setStargateProperties() to throw RuntimeException"
        );

        assertThat(thrown.getMessage()).isEqualTo("--dc and --rack are both required unless --simple-snitch is specified.");
    }

    @Test
    void testSetStargatePropertiesMissingVersion() {
        Starter starter = new Starter();
        starter.simpleSnitch = true;
        starter.clusterName = "foo";

        RuntimeException thrown = assertThrows(
                IllegalArgumentException.class,
                starter::setStargateProperties,
                "Expected setStargateProperties() to throw RuntimeException"
        );

        assertThat(thrown.getMessage()).isEqualTo("--cluster-version must be a number");
    }
}