/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.auth.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A description of an error state
 **/
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Error {
    private String description = null;
    private String internalCode = null;
    private String internalTxId = null;

    public Error() {
        super();
    }

    public Error(String description) {
        this.description = description;
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
    public Error internalCode(String internalCode) {
        this.internalCode = internalCode;
        return this;
    }


    @JsonProperty("internalCode")
    public String getInternalCode() {
        return internalCode;
    }

    public void setInternalCode(String internalCode) {
        this.internalCode = internalCode;
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
                Objects.equals(internalCode, error.internalCode) &&
                Objects.equals(internalTxId, error.internalTxId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(description, internalCode, internalTxId);
    }

    @Override
    public String toString() {
        return "Error{" +
                "description='" + description + '\'' +
                ", internalCode='" + internalCode + '\'' +
                ", internalTxId='" + internalTxId + '\'' +
                '}';
    }
}
