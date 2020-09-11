package io.stargate.web.models;

import java.util.List;

public class RowUpdate {
    List<Changeset> changeset;

    public List<Changeset> getChangeset() {
        return changeset;
    }

    public RowUpdate setChangeset(List<Changeset> changeset) {
        this.changeset = changeset;
        return this;
    }
}
