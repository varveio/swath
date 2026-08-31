# Third-Party Notices

This file is generated from the resolved `:swath-replay` main and conformance runtime
classpaths, the exact dependency closure packaged in the replay application distribution.
Do not edit it by hand.

Regenerate and verify it with:

    ./gradlew generateReplayThirdPartyNotices verifyReplayThirdPartyNotices

The inventory is derived from the dependency-license-report JSON. Embedded upstream
notices are copied from matching `META-INF/NOTICE*` resources; the pinned Zstandard
wrapper and native-library terms are rendered explicitly because zstd-jni's binary jar
does not carry those source-tree license files.
The replay distribution separately retains each dependency jar's embedded legal
resources.

## Runtime dependency inventory

- `ch.qos.logback:logback-classic:1.5.38` — Eclipse Public License - v 2.0; GNU LESSER GENERAL PUBLIC LICENSE, Version 2.1
- `ch.qos.logback:logback-core:1.5.38` — Eclipse Public License - v 2.0; GNU LESSER GENERAL PUBLIC LICENSE, Version 2.1
- `com.bucket4j:bucket4j-core:8.10.1` — Apache License, Version 2.0
- `com.fasterxml.jackson.core:jackson-annotations:2.22` — Apache License, Version 2.0
- `com.fasterxml.jackson.core:jackson-core:2.22.2` — Apache License, Version 2.0
- `com.fasterxml.jackson.core:jackson-databind:2.22.2` — Apache License, Version 2.0
- `com.github.luben:zstd-jni:1.5.7-11` — The 2-Clause BSD License
- `com.google.protobuf:protobuf-java:4.34.0` — The 3-Clause BSD License
- `commons-codec:commons-codec:1.16.1` — Apache License, Version 2.0
- `commons-pool:commons-pool:1.6` — Apache License, Version 2.0
- `info.picocli:picocli:4.7.6` — Apache License, Version 2.0
- `io.airlift:aircompressor:2.0.3` — Apache License, Version 2.0
- `io.micrometer:micrometer-commons:1.17.0` — Apache License, Version 2.0
- `io.micrometer:micrometer-core:1.17.0` — Apache License, Version 2.0
- `io.micrometer:micrometer-observation:1.17.0` — Apache License, Version 2.0
- `io.micrometer:micrometer-registry-otlp:1.17.0` — Apache License, Version 2.0
- `io.opentelemetry.proto:opentelemetry-proto:1.10.0-alpha` — Apache License, Version 2.0
- `javax.annotation:javax.annotation-api:1.3.2` — CDDL + GPLv2 with classpath exception; Common Development and Distribution License 1.0
- `org.apache.parquet:parquet-column:1.18.0` — Apache License, Version 2.0
- `org.apache.parquet:parquet-common:1.18.0` — Apache License, Version 2.0
- `org.apache.parquet:parquet-encoding:1.18.0` — Apache License, Version 2.0
- `org.apache.parquet:parquet-format-structures:1.18.0` — Apache License, Version 2.0
- `org.apache.parquet:parquet-hadoop:1.18.0` — Apache License, Version 2.0
- `org.apache.parquet:parquet-jackson:1.18.0` — Apache License, Version 2.0
- `org.duckdb:duckdb_jdbc:1.5.4.0` — No license declared in resolved metadata; MIT License
- `org.eclipse.jetty:jetty-http:12.1.10` — Apache License, Version 2.0; Eclipse Public License - v 2.0
- `org.eclipse.jetty:jetty-io:12.1.10` — Apache License, Version 2.0; Eclipse Public License - v 2.0
- `org.eclipse.jetty:jetty-server:12.1.10` — Apache License, Version 2.0; Eclipse Public License - v 2.0
- `org.eclipse.jetty:jetty-util:12.1.10` — Apache License, Version 2.0; Eclipse Public License - v 2.0
- `org.hdrhistogram:HdrHistogram:2.2.2` — Creative Commons Legal Code; PUBLIC DOMAIN; The 2-Clause BSD License
- `org.jspecify:jspecify:1.0.0` — Apache License, Version 2.0
- `org.locationtech.jts:jts-core:1.20.0` — No license declared in resolved metadata; Eclipse Public License - v 2.0; The 3-Clause BSD License
- `org.slf4j:slf4j-api:2.0.17` — No license declared in resolved metadata; MIT License
- `org.xerial.snappy:snappy-java:1.1.10.8` — Apache License, Version 2.0
- `org.xerial:sqlite-jdbc:3.47.1.0` — Apache License, Version 2.0

## Bundled Zstandard legal notices

The `com.github.luben:zstd-jni:1.5.7-11` runtime contains both the zstd-jni wrapper and bundled native
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

### commons-codec-1.16.1.jar

#### META-INF/NOTICE.txt

    Apache Commons Codec
    Copyright 2002-2024 The Apache Software Foundation

    This product includes software developed at
    The Apache Software Foundation (https://www.apache.org/).

### commons-pool-1.6.jar

#### META-INF/NOTICE.txt

    Apache Commons Pool
    Copyright 2001-2012 The Apache Software Foundation

    This product includes software developed by
    The Apache Software Foundation (http://www.apache.org/).

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

### parquet-column-1.18.0.jar

#### META-INF/NOTICE

    Apache Parquet Column
    Copyright 2026 The Apache Software Foundation


    This product includes software developed at
    The Apache Software Foundation (http://www.apache.org/).

### parquet-common-1.18.0.jar

#### META-INF/NOTICE

    Apache Parquet Common
    Copyright 2026 The Apache Software Foundation


    This product includes software developed at
    The Apache Software Foundation (http://www.apache.org/).

### parquet-encoding-1.18.0.jar

#### META-INF/NOTICE

    Apache Parquet Encodings
    Copyright 2026 The Apache Software Foundation


    This product includes software developed at
    The Apache Software Foundation (http://www.apache.org/).

### parquet-format-structures-1.18.0.jar

#### META-INF/NOTICE

    Apache Parquet Format Structures
    Copyright 2026 The Apache Software Foundation


    This product includes software developed at
    The Apache Software Foundation (http://www.apache.org/).

### parquet-hadoop-1.18.0.jar

#### META-INF/NOTICE

    Apache Parquet Hadoop
    Copyright 2026 The Apache Software Foundation


    This product includes software developed at
    The Apache Software Foundation (http://www.apache.org/).

### parquet-jackson-1.18.0.jar

#### META-INF/NOTICE

    Apache Parquet Jackson
    Copyright 2026 The Apache Software Foundation


    This product includes software developed at
    The Apache Software Foundation (http://www.apache.org/).
