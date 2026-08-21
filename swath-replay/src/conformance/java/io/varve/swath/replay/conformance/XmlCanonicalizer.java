/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.replay.conformance;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

final class XmlCanonicalizer {

    private static final Set<String> VOLATILE_ELEMENTS = Set.of(
            "NextContinuationToken",
            "RequestId",
            "HostId");

    private XmlCanonicalizer() {
    }

    static String canonicalize(byte[] xml) {
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            factory.setIgnoringComments(true);
            factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
            factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
            Document document = factory.newDocumentBuilder()
                    .parse(new InputSource(new ByteArrayInputStream(xml)));
            StringBuilder out = new StringBuilder(xml.length);
            appendElement(out, document.getDocumentElement());
            return out.toString();
        } catch (Exception e) {
            throw new IllegalArgumentException("failed to parse XML: " + preview(xml), e);
        }
    }

    private static void appendElement(StringBuilder out, Element element) {
        String name = elementName(element);
        open(out, element, name);
        if (VOLATILE_ELEMENTS.contains(name)) {
            out.append(normalizedText(element).isEmpty() ? "" : "<present>");
        } else if (hasElementChildren(element)) {
            NodeList children = element.getChildNodes();
            for (int i = 0; i < children.getLength(); i++) {
                Node child = children.item(i);
                if (child.getNodeType() == Node.ELEMENT_NODE) {
                    appendElement(out, (Element) child);
                }
            }
        } else {
            out.append(normalizedText(element));
        }
        close(out, element, name);
    }

    private static void open(StringBuilder out, Element element, String name) {
        out.append('<');
        if (element.getNamespaceURI() != null) {
            out.append('{').append(element.getNamespaceURI()).append('}');
        }
        out.append(name).append('>');
    }

    private static void close(StringBuilder out, Element element, String name) {
        out.append("</");
        if (element.getNamespaceURI() != null) {
            out.append('{').append(element.getNamespaceURI()).append('}');
        }
        out.append(name).append('>');
    }

    private static String elementName(Element element) {
        return element.getLocalName() == null ? element.getNodeName() : element.getLocalName();
    }

    private static boolean hasElementChildren(Element element) {
        NodeList children = element.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            if (children.item(i).getNodeType() == Node.ELEMENT_NODE) {
                return true;
            }
        }
        return false;
    }

    private static String normalizedText(Element element) {
        StringBuilder text = new StringBuilder();
        NodeList children = element.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node child = children.item(i);
            short type = child.getNodeType();
            if (type == Node.TEXT_NODE || type == Node.CDATA_SECTION_NODE) {
                text.append(child.getNodeValue());
            }
        }
        return text.toString().trim();
    }

    private static String preview(byte[] xml) {
        String text = new String(xml, StandardCharsets.UTF_8).replaceAll("\\s+", " ");
        return text.length() <= 160 ? text : text.substring(0, 160) + "...";
    }
}
