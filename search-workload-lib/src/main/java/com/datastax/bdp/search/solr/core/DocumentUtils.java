/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.core;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class DocumentUtils {
  public static DocumentBuilder newDocumentBuilder() throws ParserConfigurationException {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    // protect from XXE attacks
    dbf.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    dbf.setFeature("http://xml.org/sax/features/external-general-entities", false);
    dbf.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
    return dbf.newDocumentBuilder();
  }

  public static String documentToString(Document document) {
    StringWriter writer = new StringWriter();
    try {
      document.getDocumentElement().normalize();
      NodeList blankTextNodes =
          (NodeList)
              XPathFactory.newInstance()
                  .newXPath()
                  .evaluate("//text()[normalize-space(.) = '']", document, XPathConstants.NODESET);
      for (int index = 0; index < blankTextNodes.getLength(); index++) {
        blankTextNodes.item(index).getParentNode().removeChild(blankTextNodes.item(index));
      }
      Transformer transformer = TransformerFactory.newInstance().newTransformer();
      transformer.setOutputProperty(OutputKeys.INDENT, "yes");
      transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
      transformer.transform(new DOMSource(document), new StreamResult(writer));
      return writer.toString();
    } catch (TransformerException | XPathExpressionException e) {
      throw new RuntimeException(e);
    }
  }

  public static Document stringToDocument(String xml) {
    try {
      return newDocumentBuilder().parse(new InputSource(new StringReader(xml)));
    } catch (SAXException | IOException | ParserConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  public static void populateFromJsonObject(Element element, JSONObject object, boolean namedLists)
      throws ResourceGenerationException {
    for (String key : JSONObject.getNames(object)) {
      Object value = object.opt(key);
      if (value instanceof JSONObject) {
        Element nextElement = element.getOwnerDocument().createElement(key);
        populateFromJsonObject(nextElement, (JSONObject) value, namedLists);
        element.appendChild(nextElement);
      } else if (value instanceof JSONArray) {
        if (namedLists) {
          populateNamedList(element, key, (JSONArray) value);
        } else {
          JSONArray array = (JSONArray) value;
          for (int index = 0; index < array.length(); index++) {
            Object arrayObject = array.opt(index);
            if (arrayObject instanceof JSONObject) {
              Element nextElement = element.getOwnerDocument().createElement(key);
              populateFromJsonObject(nextElement, (JSONObject) arrayObject, namedLists);
              element.appendChild(nextElement);
            }
          }
        }
      } else {
        populateElement(element, value, key, namedLists);
      }
    }
  }

  private static void populateNamedList(Element element, String arrayName, JSONArray array)
      throws ResourceGenerationException {
    Element namedList;
    if (array.opt(0) instanceof JSONObject) {
      namedList = element.getOwnerDocument().createElement("lst");
    } else {
      namedList = element.getOwnerDocument().createElement("arr");
    }
    namedList.setAttribute("name", arrayName);
    element.appendChild(namedList);

    for (int index = 0; index < array.length(); index++) {
      Object arrayObject = array.opt(index);
      if (arrayObject instanceof JSONObject) {
        String name = JSONObject.getNames(((JSONObject) arrayObject))[0];
        try {
          Object value = ((JSONObject) arrayObject).get(name);

          populateNamedListElement(namedList, value, name);
        } catch (JSONException e) {
          throw new ResourceGenerationException("Failed to read from JSON object", e);
        }
      } else {
        populateNamedListElement(namedList, arrayObject, null);
      }
    }
  }

  private static void populateElement(
      Element element, Object value, String name, boolean namedLists)
      throws ResourceGenerationException {
    if (namedLists) {
      populateNamedListElement(element, value, name);
    } else {
      populateElementAttribute(element, value, name);
    }
  }

  private static void populateElementAttribute(Element element, Object value, String name)
      throws ResourceGenerationException {
    if (value instanceof String
        || value instanceof Boolean
        || value instanceof Integer
        || value instanceof Double) {
      element.setAttribute(name, value.toString());
    } else {
      throw new ResourceGenerationException(
          String.format(
              "Failed to add attribute for value of type %s", value.getClass().getName()));
    }
  }

  private static void populateNamedListElement(Element namedList, Object value, String name)
      throws ResourceGenerationException {
    Element childElement = null;
    if (value instanceof String) {
      childElement = namedList.getOwnerDocument().createElement("str");
    } else if (value instanceof Boolean) {
      childElement = namedList.getOwnerDocument().createElement("bool");
    } else if (value instanceof Integer) {
      childElement = namedList.getOwnerDocument().createElement("int");
    } else if (value instanceof Double) {
      childElement = namedList.getOwnerDocument().createElement("float");
    }
    if (childElement != null) {
      childElement.setTextContent(value.toString());
      if (name != null) {
        childElement.setAttribute("name", name);
      }
      namedList.appendChild(childElement);
    } else {
      throw new ResourceGenerationException(
          String.format(
              "Failed to create named list entry for value of type %s",
              value.getClass().getName()));
    }
  }
}
