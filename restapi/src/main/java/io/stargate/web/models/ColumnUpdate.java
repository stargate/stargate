package io.stargate.web.models;

public class ColumnUpdate {
    String newName;

    public ColumnUpdate() {}

    public ColumnUpdate(String newName) {
        this.newName = newName;
    }

    public String getNewName() {
        return newName;
    }

    public void setNewName(String newName) {
        this.newName = newName;
    }
}
