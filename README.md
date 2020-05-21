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

Please visit the main [DataSketches website](https://datasketches.apache.org) for more information. 

If you are interested in making contributions to this site please see our [Community](https://datasketches.apache.org/docs/Community/) page for how to contact us.

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

## Build / Run Instructions (Java)

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

### Run
* The characterization tests are called profiles and are located by type under the directories:
    * src/main/java/org/apache/datasketches/characterization/&lt;type&gt;/&lt;test&gt;.java
* These tests have many parameters that are specified in a corresponding configuration ".conf" file located in directories:
    * src/main/resources/&lt;type&gt;/&lt;test&gt;.conf
    * One of the parameters specified by the .conf file is the specific "Job Profile" that is to be run using that configuration.
* It is recommended that you use your IDE and run the test by executing *org.apache.datasketches.job.main(&lt;location of .conf file&gt;)*. 
The IDE should resolve all the required dependencies specified by the pom.xml file for you.  With Eclipse, the command is "run as java application".
IntelliJ should have something similar.  The output is sent to Standard Out.

## Build Instructions (C++)

### Using Eclipse

#### **Installing CDT** 
If you already have Eclipse you will need to install the CDT extensions, or you can install Eclipse with CDT only.  We had to upgrade our Eclipse to the latest version before we could successfully install the CDT extensions.
#### **Setting up the Eclipse Project** 
We have found it convenient to setup two projects in Eclipse:

* **Java project**: where the root directory is the root of your local copy of the datasketches-characterization repository. We named it "datasketches-characterization".
* **C++ project**: where the root directory is the cpp directory just under the Java project root. We named it "datasketches-characterization-CPP".

#### **Choosing the Tool Chain** 
After your project is created, open *Project Properties*

* **C/C++ Build** In this menu select *Use default build command*, *Generate Makefiles automatically*, and *Expand Env. Variable Refs in Makefiles*.
    
    * **Tool Chain Editor** Choose the compatible tool chain for your system. We use *MacOSX GCC* and *Gnu Make Builder*.
    * **Settings/Tool Settings**
        * **GCC C++ Compiler**
            * **Dialect** *Other dialect flags*: "-std=c++11"
            * **Includes** Select from the directory where you have datasketches-cpp installed. Then add complete paths for:
                * .../datasketches-cpp/common/include
                * .../datasketches-cpp/cpc/include
                * .../datasketches-cpp/fi/include
                * .../datasketches-cpp/hll/include
                * .../datasketches-cpp/kll/include
                * .../datasketches-cpp/theta/include
            * **Optimization**
                * **Optimization Level** Optimize most (-O3)
                * **Other optimization flags** "-DNDEBUG"
            * **Warnings** Check *All Warnings (-Wall)*
            * **Miscellaneous** *Other flags* "-c fmessage-length=0"

* **C/C++ General**
    * **Paths and Symbols** Tab: *Source Locations*, Action: *Add Folder...*: Add reference to "/datasketches-characterization_CPP/src"

#### **Build Project**
After this setup you should be able to *Build Project* from the top-level *Eclipse / Project* Menu.  You may need to unselect the *Build Automatically* option.

----

Disclaimer: Apache DataSketches is an effort undergoing incubation at The Apache Software Foundation (ASF), sponsored by the Apache Incubator. Incubation is required of all newly accepted projects until a further review indicates that the infrastructure, communications, and decision making process have stabilized in a manner consistent with other successful ASF projects. While incubation status is not necessarily a reflection of the completeness or stability of the code, it does indicate that the project has yet to be fully endorsed by the ASF.

