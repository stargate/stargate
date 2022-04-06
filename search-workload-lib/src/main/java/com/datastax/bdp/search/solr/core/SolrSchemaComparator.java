/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */

package com.datastax.bdp.search.solr.core;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class SolrSchemaComparator {
  final Document oldSchema;
  final Document newSchema;

  public SolrSchemaComparator(String oldSchema, String newSchema) {
    this.oldSchema = DocumentUtils.stringToDocument(oldSchema);
    this.newSchema = DocumentUtils.stringToDocument(newSchema);
  }

  public Set<String> addedFields() {
    Map<String, Element> oldFields = getElements(oldSchema, "field");
    Map<String, Element> newFields = getElements(newSchema, "field");
    newFields.keySet().removeAll(oldFields.keySet());

    return newFields.keySet();
  }

  public Set<String> fieldsWithChangedType() {
    Map<String, Element> oldFields = getElements(oldSchema, "field");
    Map<String, Element> newFields = getElements(newSchema, "field");

    return changedElements(
        oldFields,
        newFields,
        (oldField, newField) -> {
          String oldType = oldField.getAttribute("type");
          String newType = newField.getAttribute("type");
          return newType.equals(oldType);
        });
  }

  public Set<String> changedTextFieldTypes() {
    Map<String, Element> oldFieldTypes = getElements(oldSchema, "fieldType");
    Map<String, Element> newFieldTypes = getElements(newSchema, "fieldType");

    return changedElements(oldFieldTypes, newFieldTypes, this::textFieldTypesEqual);
  }

  public Set<String> fieldsWithChangedDocValues() {
    Map<String, Element> oldFields = getElements(oldSchema, "field");
    Map<String, Element> newFields = getElements(newSchema, "field");

    return changedElements(
        oldFields,
        newFields,
        (oldField, newField) -> {
          Boolean oldSchemaValues = Boolean.valueOf(oldField.getAttribute("docValues"));
          Boolean newSchemaValues = Boolean.valueOf(newField.getAttribute("docValues"));
          return oldSchemaValues.equals(newSchemaValues);
        });
  }

  private Set<String> changedElements(
      Map<String, Element> oldElements,
      Map<String, Element> newElements,
      BiFunction<Element, Element, Boolean> elementsEqualPredicate) {
    Map<String, Element> changedElements = new HashMap<>();
    for (Element newElement : newElements.values()) {
      String name = newElement.getAttribute("name");
      Element oldElement = oldElements.get(name);
      if (oldElement != null) {
        if (!elementsEqualPredicate.apply(oldElement, newElement)) {
          changedElements.put(name, oldElement);
        }
      }
    }
    return changedElements.keySet();
  }

  private Map<String, Element> getElements(Document doc, String tag) {
    NodeList elementList = doc.getElementsByTagName(tag);
    HashMap<String, Element> elements = new HashMap<>();
    for (int elementIdx = 0; elementIdx < elementList.getLength(); elementIdx++) {
      Element field = (Element) elementList.item(elementIdx);
      elements.put(field.getAttribute("name"), field);
    }
    return elements;
  }

  private Boolean textFieldTypesEqual(Element oldFieldType, Element newFieldType) {
    if (!isTextFieldType(oldFieldType)) {
      return true;
    }
    if (!isTextFieldType(newFieldType)) {
      return true;
    }
    oldFieldType.normalize();
    newFieldType.normalize();

    String oldType = elementToString(oldFieldType);
    String newType = elementToString(newFieldType);
    return oldType.equals(newType);
  }

  private boolean isTextFieldType(Element fieldType) {
    return fieldType.getAttribute("class").equals("solr.TextField")
        || fieldType.getAttribute("class").equals("org.apache.solr.schema.TextField");
  }

  private String elementToString(Element element) {
    trimTextNodes(element);
    try {
      StringWriter writer = new StringWriter();
      Transformer transformer = TransformerFactory.newInstance().newTransformer();
      transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
      transformer.setOutputProperty(OutputKeys.INDENT, "no");
      transformer.transform(new DOMSource(element), new StreamResult(writer));
      return writer.toString();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void trimTextNodes(Node node) {
    if (node.getNodeType() == Node.TEXT_NODE) {
      node.setTextContent(node.getTextContent().trim());
    } else {
      NodeList children = node.getChildNodes();
      for (int childIdx = 0; childIdx < children.getLength(); childIdx++) {
        trimTextNodes(children.item(childIdx));
      }
    }
  }
}
