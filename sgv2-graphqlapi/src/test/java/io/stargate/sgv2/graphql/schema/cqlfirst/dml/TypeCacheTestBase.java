package io.stargate.sgv2.graphql.schema.cqlfirst.dml;

import io.stargate.proto.QueryOuterClass.TypeSpec;
import io.stargate.proto.QueryOuterClass.TypeSpec.Tuple;
import java.util.Arrays;

abstract class TypeCacheTestBase {

  protected static final TypeSpec.Builder INT_TYPE =
      TypeSpec.newBuilder().setBasic(TypeSpec.Basic.INT);
  protected static final TypeSpec.Builder TEXT_TYPE =
      TypeSpec.newBuilder().setBasic(TypeSpec.Basic.VARCHAR);
  protected static final TypeSpec.Builder BOOLEAN_TYPE =
      TypeSpec.newBuilder().setBasic(TypeSpec.Basic.BOOLEAN);
  protected static final TypeSpec.Builder DOUBLE_TYPE =
      TypeSpec.newBuilder().setBasic(TypeSpec.Basic.DOUBLE);
  protected static final TypeSpec.Builder UUID_TYPE =
      TypeSpec.newBuilder().setBasic(TypeSpec.Basic.UUID);
  protected static final TypeSpec.Builder TIMEUUID_TYPE =
      TypeSpec.newBuilder().setBasic(TypeSpec.Basic.TIMEUUID);
  protected static final TypeSpec.Builder BIGINT_TYPE =
      TypeSpec.newBuilder().setBasic(TypeSpec.Basic.BIGINT);
  protected static final TypeSpec.Builder TIMESTAMP_TYPE =
      TypeSpec.newBuilder().setBasic(TypeSpec.Basic.TIMESTAMP);
  protected static final TypeSpec.Builder INET_TYPE =
      TypeSpec.newBuilder().setBasic(TypeSpec.Basic.INET);
  protected static final TypeSpec.Builder FLOAT_TYPE =
      TypeSpec.newBuilder().setBasic(TypeSpec.Basic.FLOAT);
  protected static final TypeSpec.Builder SMALLINT_TYPE =
      TypeSpec.newBuilder().setBasic(TypeSpec.Basic.SMALLINT);
  protected static final TypeSpec.Builder TINYINT_TYPE =
      TypeSpec.newBuilder().setBasic(TypeSpec.Basic.TINYINT);
  protected static final TypeSpec.Builder DECIMAL_TYPE =
      TypeSpec.newBuilder().setBasic(TypeSpec.Basic.DECIMAL);

  protected static TypeSpec.Builder listT(TypeSpec.Builder elementType) {
    return TypeSpec.newBuilder().setList(TypeSpec.List.newBuilder().setElement(elementType));
  }

  protected static TypeSpec.Builder setT(TypeSpec.Builder elementType) {
    return TypeSpec.newBuilder().setSet(TypeSpec.Set.newBuilder().setElement(elementType));
  }

  protected static TypeSpec.Builder mapT(TypeSpec.Builder keyType, TypeSpec.Builder valueType) {
    return TypeSpec.newBuilder()
        .setMap(TypeSpec.Map.newBuilder().setKey(keyType).setValue(valueType));
  }

  protected static TypeSpec.Builder tupleT(TypeSpec.Builder... elementTypes) {
    Tuple.Builder tuple = Tuple.newBuilder();
    Arrays.stream(elementTypes).forEach(tuple::addElements);
    return TypeSpec.newBuilder().setTuple(tuple);
  }
}
