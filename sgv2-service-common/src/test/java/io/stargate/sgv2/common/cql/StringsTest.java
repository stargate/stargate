package io.stargate.sgv2.common.cql;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class StringsTest {
  @Test
  public void shouldDoubleQuoteUdts() {
    assertThat(Strings.doubleQuoteUdts("int")).isEqualTo("int");
    assertThat(Strings.doubleQuoteUdts("text")).isEqualTo("text");
    assertThat(Strings.doubleQuoteUdts("'CustomType'")).isEqualTo("'CustomType'");
    assertThat(Strings.doubleQuoteUdts("list<map<int, text>>")).isEqualTo("list<map<int,text>>");
    assertThat(Strings.doubleQuoteUdts("frozen<set<bigint>>")).isEqualTo("frozen<set<bigint>>");

    assertThat(Strings.doubleQuoteUdts("address")).isEqualTo("\"address\"");
    assertThat(Strings.doubleQuoteUdts("Address")).isEqualTo("\"Address\"");
    assertThat(Strings.doubleQuoteUdts("frozen<list<map<text,address>>>"))
        .isEqualTo("frozen<list<map<text,\"address\">>>");

    // Corner-case: these should always be parameterized in regular CQL, so if they occur on their
    // own we'd have to assume it's a custom UDT.
    assertThat(Strings.doubleQuoteUdts("list")).isEqualTo("\"list\"");
    assertThat(Strings.doubleQuoteUdts("frozen")).isEqualTo("\"frozen\"");
  }
}
