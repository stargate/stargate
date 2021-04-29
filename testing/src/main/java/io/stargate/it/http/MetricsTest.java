/*
 * Copyright The Stargate Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.stargate.it.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.stargate.it.BaseOsgiIntegrationTest;
import io.stargate.it.storage.StargateConnectionInfo;
import io.stargate.it.storage.StargateParameters;
import io.stargate.it.storage.StargateSpec;
import io.stargate.testing.TestingServicesActivator;
import io.stargate.testing.metrics.TagMeHttpMetricsTagProvider;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import net.jcip.annotations.NotThreadSafe;
import okhttp3.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@NotThreadSafe
@StargateSpec(parametersCustomizer = "buildParameters")
public class MetricsTest extends BaseOsgiIntegrationTest {

  private static String host;

  @SuppressWarnings("unused") // referenced in @StargateSpec
  public static void buildParameters(StargateParameters.Builder builder) {
    builder.putSystemProperties(
        TestingServicesActivator.HTTP_TAG_PROVIDER_PROPERTY,
        TestingServicesActivator.TAG_ME_HTTP_TAG_PROVIDER);
  }

  @BeforeAll
  public static void setup(StargateConnectionInfo cluster) {
    host = "http://" + cluster.seedAddress();
  }

  @Test
  public void restApiHttpRequestMetrics() throws IOException {
    // call the rest api path with target header
    String path = String.format("%s:8082/v1/keyspaces", host);
    OkHttpClient client = new OkHttpClient().newBuilder().build();
    Request request =
        new Request.Builder()
            .url(path)
            .get()
            .addHeader(TagMeHttpMetricsTagProvider.TAG_ME_HEADER, "test-value")
            .build();
    Response execute = client.newCall(request).execute();

    String result = RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

    List<String> lines =
        Arrays.stream(result.split(System.getProperty("line.separator")))
            .filter(line -> line.startsWith("http_server_requests"))
            .collect(Collectors.toList());

    assertThat(lines)
        .anySatisfy(
            metric ->
                assertThat(metric)
                    .contains("method=\"GET\"")
                    .contains("module=\"restapi\"")
                    .contains("uri=\"/v1/keyspaces\"")
                    .contains(String.format("status=\"%d\"", execute.code()))
                    .contains(TagMeHttpMetricsTagProvider.TAG_ME_KEY + "=\"test-value\""));
  }

  @Test
  public void graphqlApiHttpRequestMetrics() throws IOException {
    // call the rest api path with target header
    String path = String.format("%s:8080/graphql", host);
    OkHttpClient client = new OkHttpClient().newBuilder().build();
    Request request =
        new Request.Builder()
            .url(path)
            .get()
            .addHeader(TagMeHttpMetricsTagProvider.TAG_ME_HEADER, "test-value")
            .build();
    Response execute = client.newCall(request).execute();

    String result = RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

    List<String> lines =
        Arrays.stream(result.split(System.getProperty("line.separator")))
            .filter(line -> line.startsWith("http_server_requests"))
            .collect(Collectors.toList());

    assertThat(lines)
        .anySatisfy(
            metric ->
                assertThat(metric)
                    .contains("method=\"GET\"")
                    .contains("module=\"graphqlapi\"")
                    .contains("uri=\"/graphql\"")
                    .contains(String.format("status=\"%d\"", execute.code()))
                    .contains(TagMeHttpMetricsTagProvider.TAG_ME_KEY + "=\"test-value\""));
  }

  @Test
  public void authApiHttpRequestMetrics() throws IOException {
    // call the rest api path with target header
    String path = String.format("%s:8081/v1/auth", host);
    OkHttpClient client = new OkHttpClient().newBuilder().build();
    Request request =
        new Request.Builder()
            .url(path)
            .post(RequestBody.create("{}", MediaType.parse("application/json")))
            .addHeader(TagMeHttpMetricsTagProvider.TAG_ME_HEADER, "test-value")
            .build();
    Response execute = client.newCall(request).execute();

    String result = RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

    List<String> lines =
        Arrays.stream(result.split(System.getProperty("line.separator")))
            .filter(line -> line.startsWith("http_server_requests"))
            .collect(Collectors.toList());

    assertThat(lines)
        .anySatisfy(
            metric ->
                assertThat(metric)
                    .contains("method=\"POST\"")
                    .contains("module=\"authapi\"")
                    .contains("uri=\"/v1/auth\"")
                    .contains(String.format("status=\"%d\"", execute.code()))
                    .contains(TagMeHttpMetricsTagProvider.TAG_ME_KEY + "=\"test-value\""));
  }

  @ParameterizedTest
  @ValueSource(strings = {"authapi", "graphqlapi", "health_checker", "restapi"})
  public void dropwizardMetricsModule(String module) throws IOException {
    String[] expectedMetricGroups =
        new String[] {"TimeBoundHealthCheck", "io_dropwizard_jersey", "org_eclipse_jetty"};

    String result = RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

    List<String> moduleLines =
        Arrays.stream(result.split(System.getProperty("line.separator")))
            .filter(line -> line.startsWith(module))
            .collect(Collectors.toList());

    for (String metricGroup : expectedMetricGroups) {
      assertThat(moduleLines).anyMatch(line -> line.contains(metricGroup));
    }
  }

  @Test
  public void dropwizardMetricsPersistence() throws IOException {
    String expectedPrefix;
    if (backend.isDse()) {
      expectedPrefix =
          "persistence_dse_" + StringUtils.remove(backend.clusterVersion(), '.').substring(0, 2);
    } else {
      expectedPrefix = "persistence_cassandra_" + backend.clusterVersion().replace('.', '_');
    }

    String result = RestUtils.get("", String.format("%s:8084/metrics", host), HttpStatus.SC_OK);

    List<String> lines =
        Arrays.stream(result.split(System.getProperty("line.separator")))
            .filter(line -> line.startsWith(expectedPrefix))
            .collect(Collectors.toList());

    assertThat(lines).isNotEmpty();
  }
}
