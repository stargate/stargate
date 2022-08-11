package io.stargate.sgv2.it;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.stargate.sgv2.common.testprofiles.IntegrationTestProfile;
import org.junit.jupiter.api.ClassOrderer;
import org.junit.jupiter.api.TestClassOrder;
import org.junit.jupiter.api.TestInstance;

import javax.enterprise.context.control.ActivateRequestContext;

@QuarkusTest
@TestProfile(IntegrationTestProfile.class)
@ActivateRequestContext
@TestClassOrder(ClassOrderer.DisplayName.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiV2QRowDeleteIT extends RestApiV2QIntegrationTestBase {
    public RestApiV2QRowDeleteIT() {
        super("rowdel_ks_", "rowdel_t_");
    }
}
