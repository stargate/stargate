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
 *  See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.stargate.web.docsapi.service;

import com.bpodgursky.jbool_expressions.Expression;
import com.bpodgursky.jbool_expressions.Literal;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.reactivex.rxjava3.core.Single;
import io.stargate.auth.AuthenticationSubject;
import io.stargate.auth.AuthorizationService;
import io.stargate.auth.SourceAPI;
import io.stargate.db.datastore.Row;
import io.stargate.web.docsapi.dao.DocumentDB;
import io.stargate.web.docsapi.dao.Paginator;
import io.stargate.web.docsapi.exception.ErrorCode;
import io.stargate.web.docsapi.exception.ErrorCodeRuntimeException;
import io.stargate.web.docsapi.models.DocumentResponseWrapper;
import io.stargate.web.docsapi.service.query.DocumentSearchService;
import io.stargate.web.docsapi.service.query.DocumentServiceUtils;
import io.stargate.web.docsapi.service.query.ExpressionParser;
import io.stargate.web.docsapi.service.query.FilterExpression;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;

public class ReactiveDocumentService {

  @Inject ExpressionParser expressionParser;
  @Inject DocumentSearchService searchService;
  @Inject DocumentService documentService;
  @Inject JsonConverter jsonConverter;
  @Inject ObjectMapper objectMapper;

  public ReactiveDocumentService() {}

  public ReactiveDocumentService(
      ExpressionParser expressionParser,
      DocumentSearchService searchService,
      DocumentService documentService,
      JsonConverter jsonConverter,
      ObjectMapper objectMapper) {
    this.expressionParser = expressionParser;
    this.searchService = searchService;
    this.documentService = documentService;
    this.jsonConverter = jsonConverter;
    this.objectMapper = objectMapper;
  }

  public Single<DocumentResponseWrapper<? extends JsonNode>> findDocuments(
      DocumentDB db,
      String namespace,
      String collection,
      String where,
      String fields,
      Paginator paginator,
      ExecutionContext context) {

    // everything in the reactive sequence
    return Single.defer(
        () -> {
          // resolve the inputs first
          Expression<FilterExpression> expression = Literal.getTrue();
          if (null != where) {
            try {
              JsonNode whereNode = objectMapper.readTree(where);
              expression =
                  expressionParser.constructFilterExpression(
                      Collections.emptyList(), whereNode, db.treatBooleansAsNumeric());
            } catch (JsonProcessingException ex) {
              throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_WHERE_JSON_INVALID);
            }
          }

          List<String> fieldsResolved = Collections.emptyList();
          if (null != fields) {
            try {
              JsonNode fieldsNode = objectMapper.readTree(fields);
              fieldsResolved = documentService.convertToSelectionList(fieldsNode);
            } catch (JsonProcessingException ex) {
              throw new ErrorCodeRuntimeException(ErrorCode.DOCS_API_SEARCH_FIELDS_JSON_INVALID);
            }
          }

          // authentication for the read before searching
          AuthorizationService authorizationService = db.getAuthorizationService();
          AuthenticationSubject authenticationSubject = db.getAuthenticationSubject();
          authorizationService.authorizeDataRead(
              authenticationSubject, namespace, collection, SourceAPI.REST);

          // needed for lambda
          List<String> fieldsFinal = fieldsResolved;

          // call the search service
          return searchService
              .searchDocuments(
                  db.getQueryExecutor(), namespace, collection, expression, paginator, context)

              // collect and make sure it's not empty
              .toList()
              .filter(rawDocuments -> !rawDocuments.isEmpty())

              // map to the json & ensure page state is updated in the wrapped
              .map(
                  rawDocuments -> {
                    String state = Paginator.makeExternalPagingState(paginator, rawDocuments);

                    ObjectNode docsResult = createJsonMap(db, rawDocuments, fieldsFinal);
                    return new DocumentResponseWrapper<JsonNode>(
                        null, state, docsResult, context.toProfile());
                  })
              .switchIfEmpty(
                  Single.fromSupplier(
                      () -> {
                        ObjectNode emptyNode = objectMapper.createObjectNode();
                        return new DocumentResponseWrapper<>(
                            null, null, emptyNode, context.toProfile());
                      }));
        });
  }

  public ObjectNode createJsonMap(DocumentDB db, List<RawDocument> docs, List<String> fields) {
    ObjectNode docsResult = objectMapper.createObjectNode();

    for (RawDocument doc : docs) {
      // filter needed rows only
      List<Row> rows = doc.rows();
      if (!fields.isEmpty()) {
        rows =
            doc.rows().stream()
                .filter(row -> fields.stream().anyMatch(field -> fieldMatchesRowPath(row, field)))
                .collect(Collectors.toList());
      }

      // create document node and set to result
      JsonNode node = jsonConverter.convertToJsonDoc(rows, false, db.treatBooleansAsNumeric());
      docsResult.set(doc.id(), node);
    }

    return docsResult;
  }

  private boolean fieldMatchesRowPath(Row row, String field) {
    String rowPath = getFieldPathFromRow(row, DocumentServiceUtils.maxFieldDepth(field));
    return rowPath.startsWith(field)
        && (rowPath.substring(field.length()).isEmpty()
            || rowPath.substring(field.length()).startsWith("."));
  }

  private String getFieldPathFromRow(Row row, long maxDepth) {
    List<String> path = new ArrayList<>();
    for (int i = 0; i < maxDepth; i++) {
      String value = row.getString("p" + i);
      if (value == null || value.isEmpty()) {
        break;
      }
      path.add(value);
    }
    return String.join(".", path);
  }
}
