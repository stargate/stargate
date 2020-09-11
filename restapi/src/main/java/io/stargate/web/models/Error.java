package io.stargate.web.models;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A description of an error state
 **/
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Error {
    private String description = null;
    private int code;
    private String internalTxId = null;

    public Error() {
        super();
    }

    public Error(String description) {
        this.description = description;
    }

    public Error(String description, int code) {
        this.description = description;
        this.code = code;
    }

    /**
     * A human readable description of the error state
     **/
    public Error description(String description) {
        this.description = description;
        return this;
    }

    @JsonProperty("description")
    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * The internal number referencing the error state
     **/
    public Error code(int code) {
        this.code = code;
        return this;
    }


    @JsonProperty("code")
    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    /**
     * The internal tracking number of the request
     **/
    public Error internalTxId(String internalTxId) {
        this.internalTxId = internalTxId;
        return this;
    }

    @JsonProperty("internalTxId")
    public String getInternalTxId() {
        return internalTxId;
    }

    public void setInternalTxId(String internalTxId) {
        this.internalTxId = internalTxId;
    }


    @Override
    public boolean equals(java.lang.Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Error error = (Error) o;
        return Objects.equals(description, error.description) &&
                Objects.equals(code, error.code) &&
                Objects.equals(internalTxId, error.internalTxId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(description, code, internalTxId);
    }

    @Override
    public String toString() {
        return "Error{" +
                "description='" + description + '\'' +
                ", code='" + code + '\'' +
                ", internalTxId='" + internalTxId + '\'' +
                '}';
    }
}

