package io.stargate.health;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

import com.codahale.metrics.health.HealthCheck.Result;
import io.stargate.db.Persistence;
import java.util.Collection;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;

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
class SchemaAgreementCheckerTest {

  private BundleContext context;
  private Persistence persistence;
  private SchemaAgreementChecker checker;

  @BeforeEach
  public void setup() throws InvalidSyntaxException {
    context = Mockito.mock(BundleContext.class);
    persistence = Mockito.mock(Persistence.class);
    checker = new SchemaAgreementChecker(context);

    @SuppressWarnings("unchecked")
    ServiceReference<Persistence> ref = Mockito.mock(ServiceReference.class);
    Collection<ServiceReference<Persistence>> refs = Collections.singletonList(ref);
    Mockito.when(context.getServiceReferences(eq(Persistence.class), any())).thenReturn(refs);

    Mockito.when(context.getService(eq(ref))).thenReturn(persistence);
  }

  @Test
  public void shouldSucceedWithNoPersistence() throws InvalidSyntaxException {
    Mockito.when(context.getServiceReferences(eq(Persistence.class), any()))
        .thenReturn(Collections.emptyList());
    assertThat(checker.execute()).extracting(Result::isHealthy).isEqualTo(true);
  }

  @Test
  public void shouldSucceedWhenSchemasAgree() {
    Mockito.doReturn(true).when(persistence).isInSchemaAgreement();
    assertThat(checker.execute()).extracting(Result::isHealthy).isEqualTo(true);
  }

  @Test
  public void shouldSucceedWhenSchemaAgreesWithStorage() {
    Mockito.doReturn(false).when(persistence).isInSchemaAgreement();
    Mockito.doReturn(true).when(persistence).isInSchemaAgreementWithStorage();
    assertThat(checker.execute()).extracting(Result::isHealthy).isEqualTo(true);
  }

  @Test
  public void shouldSucceedWhenAgreementIsAchievable() {
    Mockito.doReturn(false).when(persistence).isInSchemaAgreement();
    Mockito.doReturn(true).when(persistence).isSchemaAgreementAchievable();
    assertThat(checker.execute()).extracting(Result::isHealthy).isEqualTo(true);
  }

  @Test
  public void shouldFailWhenAgreementIsNotAchievable() {
    Mockito.doReturn(false).when(persistence).isInSchemaAgreement();
    Mockito.doReturn(false).when(persistence).isSchemaAgreementAchievable();
    assertThat(checker.execute()).extracting(Result::isHealthy).isEqualTo(false);
  }

  @Test
  public void shouldFailOnException() throws InvalidSyntaxException {
    Mockito.doThrow(new RuntimeException("test-exception")).when(persistence).isInSchemaAgreement();
    assertThat(checker.execute()).extracting(Result::isHealthy).isEqualTo(false);
  }
}
