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
package io.stargate.sgv2.docssvc.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;

/**
 * Response wrapper used for most Document API endpoints, where {@code documentId} and {@code
 * pageState} are to be returned along with wrapped response.
 *
 * @param <T> Type of response wrapped
 * @see SimpleResponseWrapper
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DocumentResponseWrapper<T> {
    @JsonProperty("documentId")
    String documentId;

    @JsonProperty("pageState")
    String pageState;

    @JsonProperty("data")
    T data;

//    @JsonProperty("profile")
//    ExecutionProfile profile;

    @ApiModelProperty(value = "The id of the document")
    public String getDocumentId() {
        return documentId;
    }

    public DocumentResponseWrapper setDocumentId(String documentId) {
        this.documentId = documentId;
        return this;
    }

    @ApiModelProperty(
            value = "A string representing the paging state to be used on future paging requests.")
    public String getPageState() {
        return pageState;
    }

    public DocumentResponseWrapper setPageState(String pageState) {
        this.pageState = pageState;
        return this;
    }

    @ApiModelProperty(value = "The data returned by the request")
    public T getData() {
        return data;
    }

    public DocumentResponseWrapper setData(T data) {
        this.data = data;
        return this;
    }

//    @ApiModelProperty("Profiling information related to the execution of the request (optional)")
//    public ExecutionProfile getProfile() {
//        return profile;
//    }

    @JsonCreator
    public DocumentResponseWrapper(
            @JsonProperty("documentId") final String documentId,
            @JsonProperty("pageState") final String pageState,
            @JsonProperty("data") final T data) {
            //@JsonProperty("profile") ExecutionProfile profile) {
        this.documentId = documentId;
        this.pageState = pageState;
        this.data = data;
//        this.profile = profile;
    }
}