package io.stargate.web.models;

public class Changeset {
    String column;
    String value;

    public String getColumn() {
        return column;
    }

    public Changeset setColumn(String column) {
        this.column = column;
        return this;
    }

    public String getValue() {
        return value;
    }

    public Changeset setValue(String value) {
        this.value = value;
        return this;
    }
}
