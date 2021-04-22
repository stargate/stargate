package io.stargate.it.grpc;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Any;
import com.google.protobuf.Int32Value;
import com.google.protobuf.InvalidProtocolBufferException;
import io.stargate.proto.QueryOuterClass.Payload;
import io.stargate.proto.QueryOuterClass.Payload.Type;
import io.stargate.proto.QueryOuterClass.Query;
import io.stargate.proto.QueryOuterClass.QueryParameters;
import io.stargate.proto.QueryOuterClass.Result;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.QueryOuterClass.Value.InnerCase;
import io.stargate.proto.QueryOuterClass.Values;
import org.junit.jupiter.api.Test;

public class QueryTest extends GrpcIntegrationTest {
  @Test
  public void simpleQuery() throws InvalidProtocolBufferException {
    Result result =
        stub.withCallCredentials(new StargateBearerToken(authToken))
            .executeQuery(
                Query.newBuilder()
                    .setCql("SELECT release_version FROM system.local WHERE key = ?")
                    .setParameters(
                        QueryParameters.newBuilder()
                            .setPayload(
                                Payload.newBuilder()
                                    .setType(Type.TYPE_CQL)
                                    .setValue(
                                        Any.pack(
                                            Values.newBuilder()
                                                .addValues(
                                                    Value.newBuilder().setString("local").build())
                                                .build()))
                                    .build())
                            .build())
                    .build());
    assertThat(result.hasPayload()).isTrue();
    ResultSet rs = result.getPayload().getValue().unpack(ResultSet.class);
    assertThat(rs.getRowsCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValuesCount()).isEqualTo(1);
    assertThat(rs.getRows(0).getValues(0).getInnerCase()).isEqualTo(InnerCase.STRING);
  }

  @Test
  public void simpleQueryWithPaging() throws InvalidProtocolBufferException {
    Result result =
        stub.withCallCredentials(new StargateBearerToken(authToken))
            .executeQuery(
                Query.newBuilder()
                    .setCql("select keyspace_name,table_name from system_schema.tables")
                    .setParameters(
                        QueryParameters.newBuilder()
                            .setPayload(Payload.newBuilder().setType(Type.TYPE_CQL).build())
                            .setPageSize(Int32Value.newBuilder().setValue(2).build())
                            .build())
                    .build());

    assertThat(result.hasPayload()).isTrue();
    ResultSet rs = result.getPayload().getValue().unpack(ResultSet.class);
    assertThat(rs.getRowsCount()).isEqualTo(2);
    assertThat(rs.getPagingState()).isNotNull();
  }
}
