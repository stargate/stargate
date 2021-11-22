package io.stargate.grpc;

import static io.stargate.grpc.CqlHelper.newStatement;
import static io.stargate.grpc.CqlHelper.selectColumn;
import static io.stargate.grpc.CqlHelper.selectWriteTime;
import static io.stargate.grpc.CqlHelper.termOf;
import static io.stargate.grpc.CqlHelper.whereColumn;
import static io.stargate.grpc.CqlHelper.whereToken;
import static stargate.Cql.Relation.Operator.EQ;

import stargate.Cql.Relation.Operator;
import stargate.Cql.Select;
import stargate.Cql.Statement;

public class TmpCqlExamples {

  public static void main(String[] args) {
    Statement statement;

    // SELECT * FROM ks.tbl
    statement = newStatement(Select.newBuilder().setKeyspaceName("ks").setTableName("tbl"));

    // SELECT c1, c2 FROM ks.tbl
    statement =
        newStatement(
            Select.newBuilder()
                .setKeyspaceName("ks")
                .setTableName("tbl")
                .addSelectors(selectColumn("c1"))
                .addSelectors(selectColumn("c2")));

    // SELECT c1, c2 FROM ks.tbl WHERE k = 1
    statement =
        newStatement(
            Select.newBuilder()
                .setKeyspaceName("ks")
                .setTableName("tbl")
                .addSelectors(selectColumn("c1"))
                .addSelectors(selectColumn("c2"))
                .addWhere(whereColumn("k").setOperator(EQ).setTerm(termOf(1))));

    // SELECT writeTime(c1) FROM ks.tbl WHERE k = 1
    newStatement(
        Select.newBuilder()
            .setKeyspaceName("ks")
            .setTableName("tbl")
            .addSelectors(selectWriteTime("c1"))
            .addWhere(whereColumn("k").setOperator(EQ).setTerm(termOf(1))));

    // SELECT * FROM tbl WHERE token(k1, k2) < ...
    newStatement(
        Select.newBuilder()
            .setTableName("tbl")
            .addWhere(
                whereToken("c1", "c2")
                    .setOperator(Operator.LT)
                    .setTerm(termOf(5765203080415074583L))));

    // SELECT token(k1, k2) FROM tbl WHERE k1 = 1 and k2 = 1
    newStatement(
        Select.newBuilder()
            .setTableName("tbl")
            .addSelectors(CqlHelper.selectToken("k1", "k2"))
            .addWhere(whereColumn("k1").setOperator(EQ).setTerm(termOf(1)))
            .addWhere(whereColumn("k2").setOperator(EQ).setTerm(termOf(1))));
  }
}
