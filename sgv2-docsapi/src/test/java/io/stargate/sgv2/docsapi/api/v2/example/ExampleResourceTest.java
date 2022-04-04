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

import io.grpc.stub.StreamObserver;
import io.quarkus.test.junit.QuarkusTest;
import io.stargate.proto.Schema;
import io.stargate.sgv2.docsapi.api.BridgeTest;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;

@QuarkusTest
public class ExampleResourceTest extends BridgeTest {

    @Nested
    class KeyspaceExists {

        @Test
        public void happyPath() {
            String keyspaceName = RandomStringUtils.randomAlphanumeric(16);
            Schema.DescribeKeyspaceQuery query = Schema.DescribeKeyspaceQuery.newBuilder().setKeyspaceName(keyspaceName).build();
            Schema.CqlKeyspaceDescribe response = Schema.CqlKeyspaceDescribe.newBuilder().setCqlKeyspace(Schema.CqlKeyspace.newBuilder().setName(keyspaceName).build()).buildPartial();
            doAnswer(invocationOnMock -> {
                StreamObserver<Schema.CqlKeyspaceDescribe> observer = invocationOnMock.getArgument(1);
                observer.onNext(response);
                observer.onCompleted();
                return null;
            }).when(bridgeImplBase).describeKeyspace(eq(query), any());

            given()
                    .when().get("/api/v2/example/keyspace-exists/{name}", keyspaceName)
                    .then()
                    .statusCode(200)
                    .body("name", is(equalTo(keyspaceName)))
                    .body("exists", is(equalTo(true)));
        }

    }

}