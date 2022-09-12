/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.sgv2.docsapi.bridge;

import static org.mockito.Mockito.when;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.mockito.InjectMock;
import io.stargate.bridge.proto.QueryOuterClass;
import io.stargate.bridge.proto.StargateBridge;
import io.stargate.sgv2.api.common.StargateRequestInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Provides an infrastructure to mock {@link StargateBridge}.
 *
 * <p>Subclasses must be annotated with {@link QuarkusTest}.
 */
public abstract class AbstractValidatingStargateBridgeTest {

  protected ValidatingStargateBridge bridge;
  @InjectMock protected StargateRequestInfo requestInfo;

  @BeforeEach
  public void createBridge() {
    bridge = new ValidatingStargateBridge();
    when(requestInfo.getStargateBridge()).thenReturn(bridge);
  }

  @AfterEach
  public void checkExpectedExecutions() {
    bridge.validate();
  }

  protected ValidatingStargateBridge.QueryExpectation withQuery(
      String cql, QueryOuterClass.Value... values) {
    return bridge.withQuery(cql, values);
  }

  protected ValidatingStargateBridge.QueryExpectation withAnySelectFrom(
      String keyspace, String table) {
    return bridge.withAnySelectFrom(keyspace, table);
  }

  protected void resetExpectations() {
    bridge.reset();
  }
}
