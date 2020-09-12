package io.stargate.web.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class RowsResponse {
    boolean success = true;
    int rowsModified = 0;

    @JsonCreator
    public RowsResponse(@JsonProperty("success") boolean success, @JsonProperty("rowsModified") int rowsModified) {
        this.success = success;
        this.rowsModified = rowsModified;
    }

    public boolean getSuccess() {
        return success;
    }

    public int getRowsModified() {
        return rowsModified;
    }
}
