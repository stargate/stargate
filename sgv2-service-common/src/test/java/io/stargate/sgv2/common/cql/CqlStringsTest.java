package io.stargate.sgv2.common.cql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class CqlStringsTest {
  @Test
  public void shouldDoubleQuoteUdts() {
    assertThat(CqlStrings.doubleQuoteUdts("int")).isEqualTo("int");
    assertThat(CqlStrings.doubleQuoteUdts("text")).isEqualTo("text");
    assertThat(CqlStrings.doubleQuoteUdts("'CustomType'")).isEqualTo("'CustomType'");
    assertThat(CqlStrings.doubleQuoteUdts("list<map<int, text>>")).isEqualTo("list<map<int,text>>");
    assertThat(CqlStrings.doubleQuoteUdts("frozen<set<bigint>>")).isEqualTo("frozen<set<bigint>>");

    assertThat(CqlStrings.doubleQuoteUdts("address")).isEqualTo("\"address\"");
    assertThat(CqlStrings.doubleQuoteUdts("Address")).isEqualTo("\"Address\"");
    assertThat(CqlStrings.doubleQuoteUdts("frozen<list<map<text,address>>>"))
        .isEqualTo("frozen<list<map<text,\"address\">>>");

    // Corner-case: these should always be parameterized in regular CQL, so if they occur on their
    // own we'd have to assume it's a custom UDT.
    assertThat(CqlStrings.doubleQuoteUdts("list")).isEqualTo("\"list\"");
    assertThat(CqlStrings.doubleQuoteUdts("frozen")).isEqualTo("\"frozen\"");
  }
}
