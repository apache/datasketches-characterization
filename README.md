<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Characterization Java & C++ Component
We define characterization as the task of comprehensively measuring accuracy 
or speed performance of our library. These characterization tests are often long running 
(some can run for days) and very resource intensive, which makes them unsuitable for including 
in unit tests.  The code in this repository are some of the test suites we use to create 
some of the plots on our website and provide evidence for our speed and accuracy claims.  
This code is shared here so that others can duplicate our own characterizations.

The code here is shared "as-is" and does not pretend to have the same level of quality as the 
primary repositories (java, pig, hive and vector).  This code is not archived to Maven Central 
and will change from time-to-time as we grow these characterization suites.

## Documentation

### [DataSketches Library Website](https://datasketches.apache.org/)

### [Java Core Overview](https://datasketches.apache.org/docs/TheChallenge.html)

### [Java Core Javadocs](https://datasketches.apache.org/api/java/snapshot/apidocs/index.html)

## Build Instructions (Java)

### JDK8 is required to compile
This Java classes of this DataSketches component must be compiled using JDK 8.

### Recommended Build Tool
This DataSketches component is structured as a Maven project and Maven is the recommended Build Tool.

There are two types of tests: normal unit tests and tests run by the strict profile.  

To run normal unit tests:

    $ mvn clean test

To run the strict profile tests:

    $ mvn clean test -P strict

### Dependencies

#### Run-time
See the pom.xml for the top-level dependencies.

#### Testing
See the pom.xml file for test dependencies.

## Build Instructions (C++)

### From within Eclipse
1. After your project is created, from "Project Properties"
2. From the Eclipse C++ Build Menu, check "Generate Makefiles automatically".
3. Under "Settings", select "Compiler", then "Includes" and add incude directories for the appropriate sketches and common.
4. Under "Optimization" select "-O3" and "-DNDEBUG".

## Resources

### [Issues for datasketches-characterization](https://github.com/apache/incubator-datasketches-characterization/issues)

### [Forum](https://groups.google.com/forum/#!forum/sketches-user)

### [Dev mailing list](dev@datasketches.apache.org)
