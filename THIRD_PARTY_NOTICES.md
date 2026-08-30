# Third-Party Notices

This file is generated from the resolved `:swath-cli` `runtimeClasspath`, the exact
dependency closure shaded into `swath.jar`. Do not edit it by hand.

Regenerate and verify it with:

    ./gradlew generateThirdPartyNotices verifyThirdPartyNotices

The inventory is derived from the dependency-license-report JSON. Embedded upstream
notices are copied from matching `META-INF/NOTICE*` resources; the pinned Zstandard
wrapper and native-library terms are rendered explicitly because zstd-jni's binary jar
does not carry those source-tree license files.
The shaded jar separately retains merged `META-INF/LICENSE*` and `META-INF/NOTICE*`
resources.

## Runtime dependency inventory

- `ch.qos.logback:logback-classic:1.5.38` — Eclipse Public License - v 2.0; GNU LESSER GENERAL PUBLIC LICENSE, Version 2.1
- `ch.qos.logback:logback-core:1.5.38` — Eclipse Public License - v 2.0; GNU LESSER GENERAL PUBLIC LICENSE, Version 2.1
- `com.bucket4j:bucket4j-core:8.10.1` — Apache License, Version 2.0
- `com.fasterxml.jackson.core:jackson-annotations:2.22` — Apache License, Version 2.0
- `com.fasterxml.jackson.core:jackson-core:2.22.2` — Apache License, Version 2.0
- `com.fasterxml.jackson.core:jackson-databind:2.22.2` — Apache License, Version 2.0
- `com.fasterxml.woodstox:woodstox-core:7.2.2` — Apache License, Version 2.0
- `com.github.luben:zstd-jni:1.5.6-9` — The 2-Clause BSD License
- `com.google.code.findbugs:jsr305:3.0.2` — Apache License, Version 2.0
- `com.google.code.gson:gson:2.9.0` — Apache License, Version 2.0
- `com.google.guava:failureaccess:1.0.3` — Apache License, Version 2.0
- `com.google.guava:guava:33.7.1-jre` — Apache License, Version 2.0
- `com.google.guava:listenablefuture:9999.0-empty-to-avoid-conflict-with-guava` — Apache License, Version 2.0
- `com.google.j2objc:j2objc-annotations:3.1` — Apache License, Version 2.0
- `com.google.protobuf:protobuf-java:4.34.0` — The 3-Clause BSD License
- `com.google.re2j:re2j:1.1` — The Go license
- `commons-beanutils:commons-beanutils:1.11.0` — Apache License, Version 2.0
- `commons-cli:commons-cli:1.5.0` — Apache License, Version 2.0
- `commons-codec:commons-codec:1.19.0` — Apache License, Version 2.0
- `commons-collections:commons-collections:3.2.2` — Apache License, Version 2.0
- `commons-io:commons-io:2.22.0` — Apache License, Version 2.0
- `commons-logging:commons-logging:1.3.6` — Apache License, Version 2.0
- `commons-net:commons-net:3.9.0` — Apache License, Version 2.0
- `commons-pool:commons-pool:1.6` — Apache License, Version 2.0
- `dnsjava:dnsjava:3.6.1` — The 3-Clause BSD License
- `info.picocli:picocli:4.7.6` — Apache License, Version 2.0
- `io.airlift:aircompressor:2.0.3` — Apache License, Version 2.0
- `io.dropwizard.metrics:metrics-core:3.2.4` — Apache License, Version 2.0
- `io.micrometer:micrometer-commons:1.17.0` — Apache License, Version 2.0
- `io.micrometer:micrometer-core:1.17.0` — Apache License, Version 2.0
- `io.micrometer:micrometer-observation:1.17.0` — Apache License, Version 2.0
- `io.micrometer:micrometer-registry-otlp:1.17.0` — Apache License, Version 2.0
- `io.opentelemetry.proto:opentelemetry-proto:1.10.0-alpha` — Apache License, Version 2.0
- `jakarta.activation:jakarta.activation-api:1.2.1` — Eclipse Public License - v 2.0; GNU GENERAL PUBLIC LICENSE, Version 2; GNU GENERAL PUBLIC LICENSE, Version 2 + Classpath Exception; The 3-Clause BSD License
- `javax.annotation:javax.annotation-api:1.3.2` — CDDL + GPLv2 with classpath exception; Common Development and Distribution License 1.0
- `org.apache.avro:avro:1.11.5` — Apache License, Version 2.0
- `org.apache.commons:commons-compress:1.28.0` — Apache License, Version 2.0
- `org.apache.commons:commons-configuration2:2.15.1` — Apache License, Version 2.0
- `org.apache.commons:commons-lang3:3.20.0` — Apache License, Version 2.0
- `org.apache.commons:commons-text:1.15.0` — Apache License, Version 2.0
- `org.apache.hadoop.thirdparty:hadoop-shaded-guava:1.3.0` — Apache License, Version 2.0
- `org.apache.hadoop.thirdparty:hadoop-shaded-protobuf_3_25:1.3.0` — Apache License, Version 2.0
- `org.apache.hadoop:hadoop-annotations:3.4.1` — Apache License, Version 2.0
- `org.apache.hadoop:hadoop-common:3.4.1` — Apache License, Version 2.0
- `org.apache.hadoop:hadoop-mapreduce-client-core:3.4.1` — Apache License, Version 2.0
- `org.apache.httpcomponents:httpclient:4.5.13` — Apache License, Version 2.0
- `org.apache.httpcomponents:httpcore:4.4.16` — Apache License, Version 2.0
- `org.apache.parquet:parquet-column:1.15.1` — Apache License, Version 2.0
- `org.apache.parquet:parquet-common:1.15.1` — Apache License, Version 2.0
- `org.apache.parquet:parquet-encoding:1.15.1` — Apache License, Version 2.0
- `org.apache.parquet:parquet-format-structures:1.15.1` — Apache License, Version 2.0
- `org.apache.parquet:parquet-hadoop:1.15.1` — Apache License, Version 2.0
- `org.apache.parquet:parquet-jackson:1.15.1` — Apache License, Version 2.0
- `org.codehaus.jettison:jettison:1.5.4` — Apache License, Version 2.0
- `org.codehaus.woodstox:stax2-api:4.3.0` — Apache License, Version 2.0; The 2-Clause BSD License
- `org.hdrhistogram:HdrHistogram:2.2.2` — Creative Commons Legal Code; PUBLIC DOMAIN; The 2-Clause BSD License
- `org.jline:jline-terminal:3.30.16` — Apache License, Version 2.0; The 3-Clause BSD License
- `org.jline:jline-terminal-ffm:3.30.16` — The 3-Clause BSD License
- `org.jspecify:jspecify:1.0.1` — Apache License, Version 2.0
- `org.reactivestreams:reactive-streams:1.0.4` — MIT-0
- `org.slf4j:slf4j-api:2.0.17` — No license declared in resolved metadata; MIT License
- `org.xerial.snappy:snappy-java:1.1.10.7` — Apache License, Version 2.0
- `org.xerial:sqlite-jdbc:3.47.1.0` — Apache License, Version 2.0
- `software.amazon.awssdk:annotations:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:apache-client:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:arns:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:auth:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:aws-core:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:aws-query-protocol:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:aws-xml-protocol:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:checksums:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:checksums-spi:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:crt-core:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:endpoints-spi:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:http-auth:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:http-auth-aws:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:http-auth-aws-eventstream:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:http-auth-spi:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:http-client-spi:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:identity-spi:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:json-utils:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:metrics-spi:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:profiles:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:protocol-core:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:regions:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:retries:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:retries-spi:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:s3:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:sdk-core:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:sts:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:third-party-jackson-core:2.31.78` — Apache License, Version 2.0
- `software.amazon.awssdk:utils:2.31.78` — Apache License, Version 2.0
- `software.amazon.eventstream:eventstream:1.0.1` — Apache License, Version 2.0

## Bundled Zstandard legal notices

The `com.github.luben:zstd-jni:1.5.6-9` runtime contains both the zstd-jni wrapper and bundled native
Zstandard code. The wrapper's BSD 2-Clause terms and the native library's BSD
3-Clause terms are reproduced separately below.

### zstd-jni wrapper — BSD 2-Clause

    Zstd-jni: JNI bindings to Zstd Library

    Copyright (c) 2015-present, Luben Karavelov/ All rights reserved.

    BSD License

    Redistribution and use in source and binary forms, with or without modification,
    are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice, this
      list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above copyright notice, this
      list of conditions and the following disclaimer in the documentation and/or
      other materials provided with the distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
    ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
    WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
    ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
    (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
    ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

### Native Zstandard library — BSD 3-Clause

    BSD License

    For Zstandard software

    Copyright (c) 2016-present, Facebook, Inc. All rights reserved.

    Redistribution and use in source and binary forms, with or without modification,
    are permitted provided that the following conditions are met:

     * Redistributions of source code must retain the above copyright notice, this
       list of conditions and the following disclaimer.

     * Redistributions in binary form must reproduce the above copyright notice,
       this list of conditions and the following disclaimer in the documentation
       and/or other materials provided with the distribution.

     * Neither the name Facebook nor the names of its contributors may be used to
       endorse or promote products derived from this software without specific
       prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
    ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
    WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR
    ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
    (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
    ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
    (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

## Embedded upstream notice resources

### annotations-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### apache-client-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### arns-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### auth-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### avro-1.11.5.jar

#### META-INF/NOTICE


    Apache Avro
    Copyright 2009-2025 The Apache Software Foundation


    This product includes software developed at
    The Apache Software Foundation (http://www.apache.org/).

### aws-core-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### aws-query-protocol-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### aws-xml-protocol-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### checksums-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### checksums-spi-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### commons-beanutils-1.11.0.jar

#### META-INF/NOTICE.txt

    Apache Commons BeanUtils
    Copyright 2000-2025 The Apache Software Foundation

    This product includes software developed at
    The Apache Software Foundation (https://www.apache.org/).

### commons-cli-1.5.0.jar

#### META-INF/NOTICE.txt

    Apache Commons CLI
    Copyright 2002-2021 The Apache Software Foundation

    This product includes software developed at
    The Apache Software Foundation (https://www.apache.org/).

### commons-codec-1.19.0.jar

#### META-INF/NOTICE.txt

    Apache Commons Codec
    Copyright 2002-2025 The Apache Software Foundation

    This product includes software developed at
    The Apache Software Foundation (https://www.apache.org/).

### commons-collections-3.2.2.jar

#### META-INF/NOTICE.txt

    Apache Commons Collections
    Copyright 2001-2015 The Apache Software Foundation

    This product includes software developed by
    The Apache Software Foundation (http://www.apache.org/).

### commons-compress-1.28.0.jar

#### META-INF/NOTICE.txt

    Apache Commons Compress
    Copyright 2002-2025 The Apache Software Foundation

    This product includes software developed at
    The Apache Software Foundation (https://www.apache.org/).

### commons-configuration2-2.15.1.jar

#### META-INF/NOTICE.txt

    Apache Commons Configuration
    Copyright 2001-2026 The Apache Software Foundation

    This product includes software developed at
    The Apache Software Foundation (https://www.apache.org/).

### commons-io-2.22.0.jar

#### META-INF/NOTICE.txt

    Apache Commons IO
    Copyright 2002-2026 The Apache Software Foundation

    This product includes software developed at
    The Apache Software Foundation (https://www.apache.org/).

### commons-lang3-3.20.0.jar

#### META-INF/NOTICE.txt

    Apache Commons Lang
    Copyright 2001-2025 The Apache Software Foundation

    This product includes software developed at
    The Apache Software Foundation (https://www.apache.org/).

### commons-logging-1.3.6.jar

#### META-INF/NOTICE.txt

    Apache Commons Logging
    Copyright 2001-2026 The Apache Software Foundation

    This product includes software developed at
    The Apache Software Foundation (https://www.apache.org/).

### commons-net-3.9.0.jar

#### META-INF/NOTICE.txt

    Apache Commons Net
    Copyright 2001-2022 The Apache Software Foundation

    This product includes software developed at
    The Apache Software Foundation (https://www.apache.org/).

### commons-pool-1.6.jar

#### META-INF/NOTICE.txt

    Apache Commons Pool
    Copyright 2001-2012 The Apache Software Foundation

    This product includes software developed by
    The Apache Software Foundation (http://www.apache.org/).

### commons-text-1.15.0.jar

#### META-INF/NOTICE.txt

    Apache Commons Text
    Copyright 2014-2025 The Apache Software Foundation

    This product includes software developed at
    The Apache Software Foundation (https://www.apache.org/).

### crt-core-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### endpoints-spi-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### hadoop-annotations-3.4.1.jar

#### META-INF/NOTICE.txt

    Apache Hadoop
    Copyright 2006 and onwards The Apache Software Foundation.

    This product includes software developed at
    The Apache Software Foundation (http://www.apache.org/).

    Export Control Notice
    ---------------------

    This distribution includes cryptographic software.  The country in
    which you currently reside may have restrictions on the import,
    possession, use, and/or re-export to another country, of
    encryption software.  BEFORE using any encryption software, please
    check your country's laws, regulations and policies concerning the
    import, possession, or use, and re-export of encryption software, to
    see if this is permitted.  See <http://www.wassenaar.org/> for more
    information.

    The U.S. Government Department of Commerce, Bureau of Industry and
    Security (BIS), has classified this software as Export Commodity
    Control Number (ECCN) 5D002.C.1, which includes information security
    software using or performing cryptographic functions with asymmetric
    algorithms.  The form and manner of this Apache Software Foundation
    distribution makes it eligible for export under the License Exception
    ENC Technology Software Unrestricted (TSU) exception (see the BIS
    Export Administration Regulations, Section 740.13) for both object
    code and source code.

    The following provides more details on the included cryptographic software:

    This software uses the SSL libraries from the Jetty project written
    by mortbay.org.
    Hadoop Yarn Server Web Proxy uses the BouncyCastle Java
    cryptography APIs written by the Legion of the Bouncy Castle Inc.

### hadoop-common-3.4.1.jar

#### META-INF/NOTICE.txt

    Apache Hadoop
    Copyright 2006 and onwards The Apache Software Foundation.

    This product includes software developed at
    The Apache Software Foundation (http://www.apache.org/).

    Export Control Notice
    ---------------------

    This distribution includes cryptographic software.  The country in
    which you currently reside may have restrictions on the import,
    possession, use, and/or re-export to another country, of
    encryption software.  BEFORE using any encryption software, please
    check your country's laws, regulations and policies concerning the
    import, possession, or use, and re-export of encryption software, to
    see if this is permitted.  See <http://www.wassenaar.org/> for more
    information.

    The U.S. Government Department of Commerce, Bureau of Industry and
    Security (BIS), has classified this software as Export Commodity
    Control Number (ECCN) 5D002.C.1, which includes information security
    software using or performing cryptographic functions with asymmetric
    algorithms.  The form and manner of this Apache Software Foundation
    distribution makes it eligible for export under the License Exception
    ENC Technology Software Unrestricted (TSU) exception (see the BIS
    Export Administration Regulations, Section 740.13) for both object
    code and source code.

    The following provides more details on the included cryptographic software:

    This software uses the SSL libraries from the Jetty project written
    by mortbay.org.
    Hadoop Yarn Server Web Proxy uses the BouncyCastle Java
    cryptography APIs written by the Legion of the Bouncy Castle Inc.

### hadoop-mapreduce-client-core-3.4.1.jar

#### META-INF/NOTICE.txt

    Apache Hadoop
    Copyright 2006 and onwards The Apache Software Foundation.

    This product includes software developed at
    The Apache Software Foundation (http://www.apache.org/).

    Export Control Notice
    ---------------------

    This distribution includes cryptographic software.  The country in
    which you currently reside may have restrictions on the import,
    possession, use, and/or re-export to another country, of
    encryption software.  BEFORE using any encryption software, please
    check your country's laws, regulations and policies concerning the
    import, possession, or use, and re-export of encryption software, to
    see if this is permitted.  See <http://www.wassenaar.org/> for more
    information.

    The U.S. Government Department of Commerce, Bureau of Industry and
    Security (BIS), has classified this software as Export Commodity
    Control Number (ECCN) 5D002.C.1, which includes information security
    software using or performing cryptographic functions with asymmetric
    algorithms.  The form and manner of this Apache Software Foundation
    distribution makes it eligible for export under the License Exception
    ENC Technology Software Unrestricted (TSU) exception (see the BIS
    Export Administration Regulations, Section 740.13) for both object
    code and source code.

    The following provides more details on the included cryptographic software:

    This software uses the SSL libraries from the Jetty project written
    by mortbay.org.
    Hadoop Yarn Server Web Proxy uses the BouncyCastle Java
    cryptography APIs written by the Legion of the Bouncy Castle Inc.

### hadoop-shaded-guava-1.3.0.jar

#### META-INF/NOTICE.txt

    Apache Hadoop Third-party Libs
    Copyright 2020 and onwards The Apache Software Foundation.

    This product includes software developed at
    The Apache Software Foundation (http://www.apache.org/).

### hadoop-shaded-protobuf_3_25-1.3.0.jar

#### META-INF/NOTICE.txt

    Apache Hadoop Third-party Libs
    Copyright 2020 and onwards The Apache Software Foundation.

    This product includes software developed at
    The Apache Software Foundation (http://www.apache.org/).

### http-auth-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### http-auth-aws-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### http-auth-aws-eventstream-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### http-auth-spi-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### http-client-spi-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### httpclient-4.5.13.jar

#### META-INF/NOTICE


    Apache HttpClient
    Copyright 1999-2020 The Apache Software Foundation

    This product includes software developed at
    The Apache Software Foundation (http://www.apache.org/).

### httpcore-4.4.16.jar

#### META-INF/NOTICE


    Apache HttpCore
    Copyright 2005-2022 The Apache Software Foundation

    This product includes software developed at
    The Apache Software Foundation (http://www.apache.org/).

### identity-spi-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### jackson-annotations-2.22.jar

#### META-INF/NOTICE

    # Jackson JSON processor

    Jackson is a high-performance, Free/Open Source JSON processing library.
    It was originally written by Tatu Saloranta (tatu.saloranta@iki.fi), and has
    been in development since 2007.
    It is currently developed by a community of developers.

    ## Copyright

    Copyright 2007-, Tatu Saloranta (tatu.saloranta@iki.fi)

    ## Licensing

    Jackson 2.x core and extension components are licensed under Apache License 2.0
    To find the details that apply to this artifact see the accompanying LICENSE file.

    ## Credits

    A list of contributors may be found from CREDITS(-2.x) file, which is included
    in some artifacts (usually source distributions); but is always available
    from the source code management (SCM) system project uses.

### jackson-core-2.22.2.jar

#### META-INF/NOTICE

    # Jackson JSON processor

    Jackson is a high-performance, Free/Open Source JSON processing library.
    It was originally written by Tatu Saloranta (tatu.saloranta@iki.fi), and has
    been in development since 2007.
    It is currently developed by a community of developers.

    ## Copyright

    Copyright 2007-, Tatu Saloranta (tatu.saloranta@iki.fi)

    ## Licensing

    Jackson 2.x core and extension components are licensed under Apache License 2.0
    To find the details that apply to this artifact see the accompanying LICENSE file.

    ## Credits

    A list of contributors may be found from CREDITS(-2.x) file, which is included
    in some artifacts (usually source distributions); but is always available
    from the source code management (SCM) system project uses.

    ## FastDoubleParser

    jackson-core bundles a shaded copy of FastDoubleParser <https://github.com/wrandelshofer/FastDoubleParser>.
    That code is available under an MIT license <https://github.com/wrandelshofer/FastDoubleParser/blob/main/LICENSE>
    under the following copyright.

    Copyright © 2023 Werner Randelshofer, Switzerland. MIT License.

    See FastDoubleParser-LICENSE and also FastDoubleParser-ThirdParty-LICENSE for details of other source code
    included in FastDoubleParser and the licenses and copyrights that apply to that code.

    ## Schubfach

    jackson-core bundles a copy of the Schubfach number writing code <https://github.com/c4f7fcce9cb06515/Schubfach>.
    That code is available under an MIT license <https://github.com/c4f7fcce9cb06515/Schubfach/blob/master/todec/LICENSE>
    under the following copyright.

    Copyright 2018-2020 Raffaello Giulietti

    See Schubfach-LICENSE.

### jackson-databind-2.22.2.jar

#### META-INF/NOTICE

    # Jackson JSON processor

    Jackson is a high-performance, Free/Open Source JSON processing library.
    It was originally written by Tatu Saloranta (tatu.saloranta@iki.fi), and has
    been in development since 2007.
    It is currently developed by a community of developers.

    ## Copyright

    Copyright 2007-, Tatu Saloranta (tatu.saloranta@iki.fi)

    ## Licensing

    Jackson 2.x core and extension components are licensed under Apache License 2.0
    To find the details that apply to this artifact see the accompanying LICENSE file.

    ## Credits

    A list of contributors may be found from CREDITS(-2.x) file, which is included
    in some artifacts (usually source distributions); but is always available
    from the source code management (SCM) system project uses.

### jakarta.activation-api-1.2.1.jar

#### META-INF/NOTICE.md

    # Notices for Eclipse Project for JAF

    This content is produced and maintained by the Eclipse Project for JAF project.

    * Project home: https://projects.eclipse.org/projects/ee4j.jaf

    ## Copyright

    All content is the property of the respective authors or their employers. For
    more information regarding authorship of content, please consult the listed
    source code repository logs.

    ## Declared Project Licenses

    This program and the accompanying materials are made available under the terms
    of the Eclipse Distribution License v. 1.0,
    which is available at http://www.eclipse.org/org/documents/edl-v10.php.

    SPDX-License-Identifier: BSD-3-Clause

    ## Source Code

    The project maintains the following source code repositories:

    * https://github.com/eclipse-ee4j/jaf

    ## Third-party Content

    This project leverages the following third party content.

    JUnit (4.12)

    * License: Eclipse Public License

### json-utils-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### metrics-spi-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### micrometer-commons-1.17.0.jar

#### META-INF/NOTICE

    Micrometer

    Copyright (c) 2017-Present VMware, Inc. All Rights Reserved.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

    -------------------------------------------------------------------------------

    This product contains a modified portion of 'io.netty.util.internal.logging',
    in the Netty/Common library distributed by The Netty Project:

      * Copyright 2013 The Netty Project
      * License: Apache License v2.0
      * Homepage: https://netty.io

    This product contains a modified portion of 'StringUtils.isBlank()',
    in the Commons Lang library distributed by The Apache Software Foundation:

      * Copyright 2001-2019 The Apache Software Foundation
      * License: Apache License v2.0
      * Homepage: https://commons.apache.org/proper/commons-lang/

    This product contains a modified portion of 'JsonUtf8Writer',
    in the Moshi library distributed by Square, Inc:

      * Copyright 2010 Google Inc.
      * License: Apache License v2.0
      * Homepage: https://github.com/square/moshi

    This product contains a modified portion of the 'org.springframework.lang'
    package in the Spring Framework library, distributed by VMware, Inc:

      * Copyright 2002-2019 the original author or authors.
      * License: Apache License v2.0
      * Homepage: https://spring.io/projects/spring-framework

### micrometer-core-1.17.0.jar

#### META-INF/NOTICE

    Micrometer

    Copyright (c) 2017-Present VMware, Inc. All Rights Reserved.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

    -------------------------------------------------------------------------------

    This product contains a modified portion of 'io.netty.util.internal.logging',
    in the Netty/Common library distributed by The Netty Project:

      * Copyright 2013 The Netty Project
      * License: Apache License v2.0
      * Homepage: https://netty.io

    This product contains a modified portion of 'StringUtils.isBlank()',
    in the Commons Lang library distributed by The Apache Software Foundation:

      * Copyright 2001-2019 The Apache Software Foundation
      * License: Apache License v2.0
      * Homepage: https://commons.apache.org/proper/commons-lang/

    This product contains a modified portion of 'JsonUtf8Writer',
    in the Moshi library distributed by Square, Inc:

      * Copyright 2010 Google Inc.
      * License: Apache License v2.0
      * Homepage: https://github.com/square/moshi

    This product contains a modified portion of the 'org.springframework.lang'
    package in the Spring Framework library, distributed by VMware, Inc:

      * Copyright 2002-2019 the original author or authors.
      * License: Apache License v2.0
      * Homepage: https://spring.io/projects/spring-framework

### micrometer-observation-1.17.0.jar

#### META-INF/NOTICE

    Micrometer

    Copyright (c) 2017-Present VMware, Inc. All Rights Reserved.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

    -------------------------------------------------------------------------------

    This product contains a modified portion of 'io.netty.util.internal.logging',
    in the Netty/Common library distributed by The Netty Project:

      * Copyright 2013 The Netty Project
      * License: Apache License v2.0
      * Homepage: https://netty.io

    This product contains a modified portion of 'StringUtils.isBlank()',
    in the Commons Lang library distributed by The Apache Software Foundation:

      * Copyright 2001-2019 The Apache Software Foundation
      * License: Apache License v2.0
      * Homepage: https://commons.apache.org/proper/commons-lang/

    This product contains a modified portion of 'JsonUtf8Writer',
    in the Moshi library distributed by Square, Inc:

      * Copyright 2010 Google Inc.
      * License: Apache License v2.0
      * Homepage: https://github.com/square/moshi

    This product contains a modified portion of the 'org.springframework.lang'
    package in the Spring Framework library, distributed by VMware, Inc:

      * Copyright 2002-2019 the original author or authors.
      * License: Apache License v2.0
      * Homepage: https://spring.io/projects/spring-framework

### micrometer-registry-otlp-1.17.0.jar

#### META-INF/NOTICE

    Micrometer

    Copyright (c) 2017-Present VMware, Inc. All Rights Reserved.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

    -------------------------------------------------------------------------------

    This product contains a modified portion of 'io.netty.util.internal.logging',
    in the Netty/Common library distributed by The Netty Project:

      * Copyright 2013 The Netty Project
      * License: Apache License v2.0
      * Homepage: https://netty.io

    This product contains a modified portion of 'StringUtils.isBlank()',
    in the Commons Lang library distributed by The Apache Software Foundation:

      * Copyright 2001-2019 The Apache Software Foundation
      * License: Apache License v2.0
      * Homepage: https://commons.apache.org/proper/commons-lang/

    This product contains a modified portion of 'JsonUtf8Writer',
    in the Moshi library distributed by Square, Inc:

      * Copyright 2010 Google Inc.
      * License: Apache License v2.0
      * Homepage: https://github.com/square/moshi

    This product contains a modified portion of the 'org.springframework.lang'
    package in the Spring Framework library, distributed by VMware, Inc:

      * Copyright 2002-2019 the original author or authors.
      * License: Apache License v2.0
      * Homepage: https://spring.io/projects/spring-framework

### parquet-jackson-1.15.1.jar

#### META-INF/NOTICE

    # Jackson JSON processor

    Jackson is a high-performance, Free/Open Source JSON processing library.
    It was originally written by Tatu Saloranta (tatu.saloranta@iki.fi), and has
    been in development since 2007.
    It is currently developed by a community of developers.

    ## Copyright

    Copyright 2007-, Tatu Saloranta (tatu.saloranta@iki.fi)

    ## Licensing

    Jackson 2.x core and extension components are licensed under Apache License 2.0
    To find the details that apply to this artifact see the accompanying LICENSE file.

    ## Credits

    A list of contributors may be found from CREDITS(-2.x) file, which is included
    in some artifacts (usually source distributions); but is always available
    from the source code management (SCM) system project uses.

    ## FastDoubleParser

    jackson-core bundles a shaded copy of FastDoubleParser <https://github.com/wrandelshofer/FastDoubleParser>.
    That code is available under an MIT license <https://github.com/wrandelshofer/FastDoubleParser/blob/main/LICENSE>
    under the following copyright.

    Copyright © 2023 Werner Randelshofer, Switzerland. MIT License.

    See FastDoubleParser-NOTICE for details of other source code included in FastDoubleParser
    and the licenses and copyrights that apply to that code.

### profiles-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### protocol-core-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### regions-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### retries-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### retries-spi-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### s3-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### sdk-core-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### sts-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### third-party-jackson-core-2.31.78.jar

#### META-INF/NOTICE

    # Jackson JSON processor

    Jackson is a high-performance, Free/Open Source JSON processing library.
    It was originally written by Tatu Saloranta (tatu.saloranta@iki.fi), and has
    been in development since 2007.
    It is currently developed by a community of developers.

    ## Copyright

    Copyright 2007-, Tatu Saloranta (tatu.saloranta@iki.fi)

    ## Licensing

    Jackson 2.x core and extension components are licensed under Apache License 2.0
    To find the details that apply to this artifact see the accompanying LICENSE file.

    ## Credits

    A list of contributors may be found from CREDITS(-2.x) file, which is included
    in some artifacts (usually source distributions); but is always available
    from the source code management (SCM) system project uses.

    ## FastDoubleParser

    jackson-core bundles a shaded copy of FastDoubleParser <https://github.com/wrandelshofer/FastDoubleParser>.
    That code is available under an MIT license <https://github.com/wrandelshofer/FastDoubleParser/blob/main/LICENSE>
    under the following copyright.

    Copyright © 2023 Werner Randelshofer, Switzerland. MIT License.

    See FastDoubleParser-NOTICE for details of other source code included in FastDoubleParser
    and the licenses and copyrights that apply to that code.

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).

### utils-2.31.78.jar

#### META-INF/NOTICE.txt

    AWS SDK for Java 2.0
    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

    This product includes software developed by
    Amazon Technologies, Inc (http://www.amazon.com/).

    **********************
    THIRD PARTY COMPONENTS
    **********************
    This software includes third party software subject to the following copyrights:
    - XML parsing and utility functions from JetS3t - Copyright 2006-2009 James Murty.
    - PKCS#1 PEM encoded private key parsing and utility functions from oauth.googlecode.com - Copyright 1998-2010 AOL Inc.
    - Apache Commons Lang - https://github.com/apache/commons-lang
    - Netty Reactive Streams - https://github.com/playframework/netty-reactive-streams
    - Jackson-core - https://github.com/FasterXML/jackson-core
    - Jackson-dataformat-cbor - https://github.com/FasterXML/jackson-dataformats-binary

    The licenses for these third party components are included in LICENSE.txt

    - For Apache Commons Lang see also this required NOTICE:
      Apache Commons Lang
      Copyright 2001-2020 The Apache Software Foundation

      This product includes software developed at
      The Apache Software Foundation (https://www.apache.org/).
