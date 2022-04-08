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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.sgv2.docsapi.api.v2.example;

import io.grpc.Metadata;
import io.grpc.stub.StreamObserver;
import io.quarkus.test.junit.QuarkusTest;
import io.stargate.proto.Schema;
import io.stargate.sgv2.docsapi.BridgeTest;
import io.stargate.sgv2.docsapi.config.GrpcMetadataConfig;
import io.stargate.sgv2.docsapi.config.constants.Constants;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import javax.inject.Inject;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@QuarkusTest
public class ExampleResourceTest extends BridgeTest {

    @Inject
    GrpcMetadataConfig metadataConfig;

    ArgumentCaptor<Metadata> headersCaptor;

    @BeforeEach
    public void init() {
        headersCaptor = ArgumentCaptor.forClass(Metadata.class);
    }

    @Nested
    class KeyspaceExists {

        @Test
        public void happyPath() {
            String token = RandomStringUtils.randomAlphanumeric(16);
            String keyspaceName = RandomStringUtils.randomAlphanumeric(16);
            Schema.DescribeKeyspaceQuery query = Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName(keyspaceName).build();
            Schema.CqlKeyspaceDescribe response = Schema.CqlKeyspaceDescribe.newBuilder().setCqlKeyspace(Schema.CqlKeyspace.newBuilder().setName(keyspaceName).build()).buildPartial();
            doAnswer(invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer = invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
            }).when(bridgeService).describeKeyspace(eq(query), any());

            given()
                    .header(Constants.AUTHENTICATION_TOKEN_HEADER_NAME, token)
                    .when().get("/api/v2/example/keyspace-exists/{name}", keyspaceName)
                    .then()
                    .statusCode(200)
                    .body("name", is(equalTo(keyspaceName)))
                    .body("exists", is(equalTo(true)));

            verify(bridgeInterceptor).interceptCall(any(), headersCaptor.capture(), any());
            assertThat(headersCaptor.getAllValues()).singleElement()
                    .satisfies(metadata -> {
                        Metadata.Key<String> tokenKey = Metadata.Key.of(metadataConfig.cassandraTokenKey(), Metadata.ASCII_STRING_MARSHALLER);
                        assertThat(metadata.get(tokenKey)).isEqualTo(token);
                    });
        }

        @Test
        public void noToken() {
            String keyspaceName = RandomStringUtils.randomAlphanumeric(16);

            given()
                    .when().get("/api/v2/example/keyspace-exists/{name}", keyspaceName)
                    .then()
                    .statusCode(401);

            verifyNoInteractions(bridgeInterceptor, bridgeService);
        }

    }

}
