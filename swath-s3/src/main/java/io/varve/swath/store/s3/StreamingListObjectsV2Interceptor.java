/*
 * Copyright 2026 Varve Systems Ltd
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package io.varve.swath.store.s3;

import io.varve.swath.observability.RunMetrics;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import software.amazon.awssdk.core.SdkRequest;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.Owner;
import software.amazon.awssdk.services.s3.model.RestoreStatus;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Streams successful {@code ListObjectsV2} XML into the SDK response model instead of letting the
 * SDK first materialize its generic {@code XmlElement} tree.
 *
 * <p>The SDK still owns request marshalling, HTTP, authentication, timeouts, retry decisions,
 * headers, error-body unmarshalling and the public response model. Only a successful
 * {@code ListObjectsV2} body takes this path. The original body is replaced with a minimal valid
 * result for the generated unmarshaller; {@link #modifyResponse} then overlays the values parsed
 * here on that response, preserving its HTTP metadata and header-derived fields.
 */
final class StreamingListObjectsV2Interceptor implements ExecutionInterceptor {

    private static final byte[] EMPTY_RESULT = ("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            + "<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>")
            .getBytes(StandardCharsets.UTF_8);
    private static final ExecutionAttribute<ParsedResponse> PARSED_RESPONSE =
            new ExecutionAttribute<>(StreamingListObjectsV2Interceptor.class.getName() + ".parsedResponse");
    private static final ThreadLocal<XMLInputFactory> XML_FACTORIES =
            ThreadLocal.withInitial(StreamingListObjectsV2Interceptor::newXmlInputFactory);
    private static final ThreadLocal<XMLOutputFactory> XML_OUTPUT_FACTORIES =
            ThreadLocal.withInitial(XMLOutputFactory::newFactory);

    private final RunMetrics metrics;

    StreamingListObjectsV2Interceptor(RunMetrics metrics) {
        this.metrics = metrics;
    }

    @Override
    public Optional<InputStream> modifyHttpResponseContent(
            Context.ModifyHttpResponse context, ExecutionAttributes executionAttributes) {
        SdkRequest request = context.request();
        Optional<InputStream> responseBody = context.responseBody();
        if (!(request instanceof ListObjectsV2Request listRequest)
                || !isSuccessful(context.httpResponse().statusCode())
                || responseBody.isEmpty()) {
            return responseBody;
        }

        try {
            ParsedResponse parsed = parse(responseBody.orElseThrow(), listRequest.maxKeys());
            if (parsed.sdkErrorBody != null) {
                if (metrics != null) {
                    metrics.recordStealReason("S3_RESPONSE", "sdk_error_in_success");
                }
                return Optional.of(new ByteArrayInputStream(parsed.sdkErrorBody));
            }
            executionAttributes.putAttribute(PARSED_RESPONSE, parsed);
            if (metrics != null) {
                metrics.recordStealReason("S3_RESPONSE", "streaming_xml");
            }
            return Optional.of(new ByteArrayInputStream(EMPTY_RESULT));
        } catch (XMLStreamException | RuntimeException e) {
            throw SdkClientException.builder()
                    .message("Unable to stream ListObjectsV2 XML response")
                    .cause(e)
                    .build();
        }
    }

    @Override
    public SdkResponse modifyResponse(Context.ModifyResponse context, ExecutionAttributes executionAttributes) {
        ParsedResponse parsed = executionAttributes.getAttribute(PARSED_RESPONSE);
        if (parsed == null) {
            return context.response();
        }
        if (!(context.response() instanceof ListObjectsV2Response response)) {
            throw SdkClientException.create("Streaming ListObjectsV2 parser received an unexpected response type");
        }
        return parsed.applyTo(response);
    }

    static ParsedResponse parse(InputStream input, Integer requestedMaxKeys) throws XMLStreamException {
        int expectedEntries = requestedMaxKeys == null ? 0 : Math.clamp(requestedMaxKeys, 0, 1_000);
        XMLStreamReader reader = XML_FACTORIES.get().createXMLStreamReader(input);
        try {
            ParsedResponse parsed = new ParsedResponse(expectedEntries);
            boolean rootSeen = false;
            while (reader.hasNext()) {
                if (reader.next() != XMLStreamConstants.START_ELEMENT) {
                    continue;
                }
                if (!rootSeen) {
                    rootSeen = true;
                    if ("Error".equals(reader.getLocalName())) {
                        parsed.sdkErrorBody = copyCurrentElement(reader);
                        return parsed;
                    }
                    if (!"ListBucketResult".equals(reader.getLocalName())) {
                        throw new XMLStreamException(
                                "Unexpected ListObjectsV2 root element: " + reader.getLocalName());
                    }
                    continue;
                }
                switch (reader.getLocalName()) {
                    case "IsTruncated" -> parsed.isTruncated = parseBoolean(reader);
                    case "Contents" -> parsed.addContent(parseObject(reader));
                    case "Name" -> parsed.name = reader.getElementText();
                    case "Prefix" -> parsed.prefix = reader.getElementText();
                    case "Delimiter" -> parsed.delimiter = reader.getElementText();
                    case "MaxKeys" -> parsed.maxKeys = parseInteger(reader);
                    case "CommonPrefixes" -> parsed.addCommonPrefix(parseCommonPrefix(reader));
                    case "EncodingType" -> parsed.encodingType = reader.getElementText();
                    case "KeyCount" -> parsed.keyCount = parseInteger(reader);
                    case "ContinuationToken" -> parsed.continuationToken = reader.getElementText();
                    case "NextContinuationToken" -> parsed.nextContinuationToken = reader.getElementText();
                    case "StartAfter" -> parsed.startAfter = reader.getElementText();
                    default -> skipElement(reader);
                }
            }
            if (!rootSeen) {
                throw new XMLStreamException("ListObjectsV2 response contained no root element");
            }
            return parsed;
        } finally {
            reader.close();
        }
    }

    private static S3Object parseObject(XMLStreamReader reader) throws XMLStreamException {
        S3Object.Builder object = S3Object.builder();
        List<String> checksums = null;
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.END_ELEMENT && "Contents".equals(reader.getLocalName())) {
                if (checksums != null) {
                    object.checksumAlgorithmWithStrings(checksums);
                }
                return object.build();
            }
            if (event != XMLStreamConstants.START_ELEMENT) {
                continue;
            }
            switch (reader.getLocalName()) {
                case "Key" -> object.key(reader.getElementText());
                case "LastModified" -> object.lastModified(parseInstant(reader));
                case "ETag" -> object.eTag(reader.getElementText());
                case "ChecksumAlgorithm" -> {
                    if (checksums == null) {
                        checksums = new ArrayList<>(1);
                    }
                    checksums.add(reader.getElementText());
                }
                case "ChecksumType" -> object.checksumType(reader.getElementText());
                case "Size" -> object.size(parseLong(reader));
                case "StorageClass" -> object.storageClass(reader.getElementText());
                case "Owner" -> object.owner(parseOwner(reader));
                case "RestoreStatus" -> object.restoreStatus(parseRestoreStatus(reader));
                default -> skipElement(reader);
            }
        }
        throw new XMLStreamException("Unclosed Contents element");
    }

    private static Owner parseOwner(XMLStreamReader reader) throws XMLStreamException {
        Owner.Builder owner = Owner.builder();
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.END_ELEMENT && "Owner".equals(reader.getLocalName())) {
                return owner.build();
            }
            if (event != XMLStreamConstants.START_ELEMENT) {
                continue;
            }
            switch (reader.getLocalName()) {
                case "ID" -> owner.id(reader.getElementText());
                case "DisplayName" -> owner.displayName(reader.getElementText());
                default -> skipElement(reader);
            }
        }
        throw new XMLStreamException("Unclosed Owner element");
    }

    private static RestoreStatus parseRestoreStatus(XMLStreamReader reader) throws XMLStreamException {
        RestoreStatus.Builder restore = RestoreStatus.builder();
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.END_ELEMENT && "RestoreStatus".equals(reader.getLocalName())) {
                return restore.build();
            }
            if (event != XMLStreamConstants.START_ELEMENT) {
                continue;
            }
            switch (reader.getLocalName()) {
                case "IsRestoreInProgress" -> restore.isRestoreInProgress(parseBoolean(reader));
                case "RestoreExpiryDate" -> restore.restoreExpiryDate(parseInstant(reader));
                default -> skipElement(reader);
            }
        }
        throw new XMLStreamException("Unclosed RestoreStatus element");
    }

    private static CommonPrefix parseCommonPrefix(XMLStreamReader reader) throws XMLStreamException {
        CommonPrefix.Builder prefix = CommonPrefix.builder();
        while (reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.END_ELEMENT && "CommonPrefixes".equals(reader.getLocalName())) {
                return prefix.build();
            }
            if (event != XMLStreamConstants.START_ELEMENT) {
                continue;
            }
            if ("Prefix".equals(reader.getLocalName())) {
                prefix.prefix(reader.getElementText());
            } else {
                skipElement(reader);
            }
        }
        throw new XMLStreamException("Unclosed CommonPrefixes element");
    }

    private static Boolean parseBoolean(XMLStreamReader reader) throws XMLStreamException {
        return Boolean.valueOf(reader.getElementText());
    }

    private static Integer parseInteger(XMLStreamReader reader) throws XMLStreamException {
        return Integer.valueOf(reader.getElementText());
    }

    private static Long parseLong(XMLStreamReader reader) throws XMLStreamException {
        return Long.valueOf(reader.getElementText());
    }

    private static Instant parseInstant(XMLStreamReader reader) throws XMLStreamException {
        return Instant.parse(reader.getElementText());
    }

    private static void skipElement(XMLStreamReader reader) throws XMLStreamException {
        int depth = 1;
        while (depth > 0 && reader.hasNext()) {
            int event = reader.next();
            if (event == XMLStreamConstants.START_ELEMENT) {
                depth++;
            } else if (event == XMLStreamConstants.END_ELEMENT) {
                depth--;
            }
        }
    }

    /**
     * Preserve an S3 {@code <Error>} document found under HTTP 200 for the SDK's
     * error-could-be-in-body response handler. Re-serializing the rare error tree avoids buffering
     * every successful 1000-key page merely so the input stream could be rewound.
     */
    private static byte[] copyCurrentElement(XMLStreamReader reader) throws XMLStreamException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(512);
        XMLStreamWriter writer = XML_OUTPUT_FACTORIES.get().createXMLStreamWriter(bytes, StandardCharsets.UTF_8.name());
        int depth = 0;
        try {
            while (true) {
                switch (reader.getEventType()) {
                    case XMLStreamConstants.START_ELEMENT -> {
                        String namespace = reader.getNamespaceURI();
                        String prefix = reader.getPrefix();
                        writer.writeStartElement(prefix == null ? "" : prefix, reader.getLocalName(),
                                namespace == null ? "" : namespace);
                        for (int i = 0; i < reader.getNamespaceCount(); i++) {
                            String declaredPrefix = reader.getNamespacePrefix(i);
                            String declaredNamespace = reader.getNamespaceURI(i);
                            if (declaredPrefix == null) {
                                writer.writeDefaultNamespace(declaredNamespace);
                            } else {
                                writer.writeNamespace(declaredPrefix, declaredNamespace);
                            }
                        }
                        for (int i = 0; i < reader.getAttributeCount(); i++) {
                            String attributeNamespace = reader.getAttributeNamespace(i);
                            String attributePrefix = reader.getAttributePrefix(i);
                            if (attributeNamespace == null || attributeNamespace.isEmpty()) {
                                writer.writeAttribute(reader.getAttributeLocalName(i), reader.getAttributeValue(i));
                            } else {
                                writer.writeAttribute(attributePrefix == null ? "" : attributePrefix,
                                        attributeNamespace, reader.getAttributeLocalName(i),
                                        reader.getAttributeValue(i));
                            }
                        }
                        depth++;
                    }
                    case XMLStreamConstants.END_ELEMENT -> {
                        writer.writeEndElement();
                        depth--;
                        if (depth == 0) {
                            writer.flush();
                            return bytes.toByteArray();
                        }
                    }
                    case XMLStreamConstants.CHARACTERS, XMLStreamConstants.SPACE ->
                            writer.writeCharacters(reader.getText());
                    case XMLStreamConstants.CDATA -> writer.writeCData(reader.getText());
                    case XMLStreamConstants.COMMENT -> writer.writeComment(reader.getText());
                    case XMLStreamConstants.PROCESSING_INSTRUCTION ->
                            writer.writeProcessingInstruction(reader.getPITarget(), reader.getPIData());
                    default -> {
                        // DTD and external entities are disabled; document-level events are outside
                        // the root element and therefore cannot occur while depth is positive.
                    }
                }
                if (!reader.hasNext()) {
                    throw new XMLStreamException("Unclosed Error element");
                }
                reader.next();
            }
        } finally {
            writer.close();
        }
    }

    private static boolean isSuccessful(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    private static XMLInputFactory newXmlInputFactory() {
        XMLInputFactory factory = XMLInputFactory.newFactory();
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        factory.setProperty("javax.xml.stream.isSupportingExternalEntities", false);
        factory.setProperty(XMLInputFactory.IS_COALESCING, true);
        return factory;
    }

    static final class ParsedResponse {
        private final int expectedEntries;
        private Boolean isTruncated;
        private List<S3Object> contents;
        private String name;
        private String prefix;
        private String delimiter;
        private Integer maxKeys;
        private List<CommonPrefix> commonPrefixes;
        private String encodingType;
        private Integer keyCount;
        private String continuationToken;
        private String nextContinuationToken;
        private String startAfter;
        private byte[] sdkErrorBody;

        private ParsedResponse(int expectedEntries) {
            this.expectedEntries = expectedEntries;
        }

        private void addContent(S3Object object) {
            if (contents == null) {
                contents = new ArrayList<>(expectedEntries);
            }
            contents.add(object);
        }

        private void addCommonPrefix(CommonPrefix commonPrefix) {
            if (commonPrefixes == null) {
                commonPrefixes = new ArrayList<>(Math.min(expectedEntries, 64));
            }
            commonPrefixes.add(commonPrefix);
        }

        private ListObjectsV2Response applyTo(ListObjectsV2Response response) {
            // Keep every URL-encoded field exactly as it appeared on the wire. The S3 client's
            // built-in DecodeUrlEncodedResponseInterceptor runs after client-level interceptors
            // and decodes these fields once, preserving the SDK's established semantics for '%',
            // '+', and malformed encodings.
            ListObjectsV2Response.Builder builder = response.toBuilder()
                    .isTruncated(isTruncated)
                    .name(name)
                    .prefix(prefix)
                    .delimiter(delimiter)
                    .maxKeys(maxKeys)
                    .encodingType(encodingType)
                    .keyCount(keyCount)
                    .continuationToken(continuationToken)
                    .nextContinuationToken(nextContinuationToken)
                    .startAfter(startAfter);
            if (contents != null) {
                builder.contents(contents);
            }
            if (commonPrefixes != null) {
                builder.commonPrefixes(commonPrefixes);
            }
            return builder.build();
        }
    }
}
