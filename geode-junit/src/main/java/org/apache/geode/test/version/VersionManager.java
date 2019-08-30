/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.test.version;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.BiConsumer;

import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;

/**
 * VersionManager loads the class-paths for all of the releases of Geode configured for
 * backward-compatibility testing in the geode-core build.gradle file.
 * <p>
 * Tests may use these versions in launching VMs to run older clients or servers.
 *
 * see Host.getVM(String, int)
 */
public class VersionManager {
  public static final String CURRENT_VERSION = "000";
  public static final String GEODE_110 = "110";
  public static final String GEODE_120 = "120";
  public static final String GEODE_130 = "130";
  public static final String GEODE_140 = "140";

  private static VersionManager instance;

  protected String loadFailure = "";

  /**
   * returns the ordinal of the Version of Geode used in this JVM. Use this
   * instead of Version.CURRENT or Version.CURRENT_ORDINAL in test code
   */
  public short getCurrentVersionOrdinal() {
    return geodeCurrentVersionOrdinal;
  }

  private short geodeCurrentVersionOrdinal = -1;

  protected static void init() {
    instance = new VersionManager();
    final String fileName = "geodeOldVersionClasspaths.txt";
    final String installLocations = "geodeOldVersionInstalls.txt";
    instance.findVersions(fileName);
    instance.findInstalls(installLocations);
    instance.establishGeodeVersionOrdinal();
    // System.out
    // .println("VersionManager has loaded the following classpaths:\n" + instance.classPaths);
  }

  public static VersionManager getInstance() {
    if (instance == null) {
      init();
    }
    return instance;
  }

  /**
   * for unit testing, this creates a VersionManager with paths loaded from the given file, which
   * may or may not exist. The instance is not retained
   */
  protected static VersionManager getInstance(String classpathsFileName, String installFileName) {
    VersionManager result = new VersionManager();
    result.findVersions(classpathsFileName);
    return result;
  }

  /**
   * classPaths for old versions loaded from a file generated by Gradle
   */
  private Map<String, String> classPaths = new HashMap<>();

  private List<String> testVersions = new ArrayList<String>(10);

  private Map<String, String> installs = new HashMap();

  /**
   * Test to see if a version string is known to VersionManager. Versions are either CURRENT_VERSION
   * or one of the versions returned by VersionManager#getVersions()
   */
  public boolean isValidVersion(String version) {
    return version.equals(CURRENT_VERSION) || classPaths.containsKey(version);
  }

  /**
   * Returns true if the version is equal to the CURRENT_VERSION constant
   */
  public static boolean isCurrentVersion(String version) {
    return version.equals(CURRENT_VERSION);
  }

  /**
   * Returns the classPath for the given version, or null if the version is not valid. Use
   * CURRENT_VERSION for the version in development.
   */
  public String getClasspath(String version) {
    return classPaths.get(version);
  }


  public String getInstall(String version) {
    return installs.get(version);
  }

  /**
   * Returns a list of older versions available for testing
   */
  public List<String> getVersions() {
    checkForLoadFailure();
    return Collections.unmodifiableList(testVersions);
  }

  public List<String> getVersionsWithoutCurrent() {
    checkForLoadFailure();
    List<String> result = new ArrayList<>(testVersions);
    result.remove(CURRENT_VERSION);
    return result;
  }


  private void checkForLoadFailure() {
    if (loadFailure.length() > 0) {
      throw new InternalError(loadFailure);
    }
  }

  private void findVersions(String fileName) {
    // this file is created by the gradle task createClasspathsPropertiesFile
    readVersionsFile(fileName, (version, path) -> {
      Optional<String> parsedVersion = parseVersion(version);
      if (parsedVersion.isPresent()) {
        if (parsedVersion.get().equals("140")
            && SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9)) {
          // Serialization filtering was added in 140, but the support for them in java 9+ was added
          // in 150. As a result, 140 servers and clients will fail categorically when run in
          // Java 9+ even with the additional libs (jaxb and activation) in the classpath
          System.err.println(
              "Geode version 140 is incompatible with Java 9 and higher.  Skipping this version.");
        } else {
          classPaths.put(parsedVersion.get(), path);
          testVersions.add(parsedVersion.get());
        }
      }
    });
    Collections.sort(testVersions);
  }

  private void findInstalls(String fileName) {
    readVersionsFile(fileName, (version, install) -> {
      Optional<String> parsedVersion = parseVersion(version);
      if (parsedVersion.isPresent()) {
        installs.put(parsedVersion.get(), install);
      }
    });
  }

  private Optional<String> parseVersion(String version) {
    String parsedVersion = null;
    if (version.startsWith("test") && version.length() >= "test".length()) {
      if (version.equals("test")) {
        parsedVersion = CURRENT_VERSION;
      } else {
        parsedVersion = version.substring("test".length());
      }
    }
    return Optional.ofNullable(parsedVersion);
  }

  private void readVersionsFile(String fileName, BiConsumer<String, String> consumer) {
    Properties props = readPropertiesFile(fileName);
    props.forEach((k, v) -> {
      consumer.accept(k.toString(), v.toString());
    });
  }

  public Properties readPropertiesFile(String fileName) {
    // this file is created by the gradle task createClasspathsPropertiesFile
    Properties props = new Properties();
    URL url = VersionManager.class.getResource("/" + fileName);
    if (url == null) {
      loadFailure = "VersionManager: unable to locate " + fileName + " in class-path";
      return props;
    }
    try (InputStream in = VersionManager.class.getResource("/" + fileName).openStream()) {
      props.load(in);
    } catch (IOException e) {
      loadFailure = "VersionManager: unable to read resource " + fileName;
      return props;
    }
    return props;
  }

  public void establishGeodeVersionOrdinal() {
    Class versionClass;
    Field currentOrdinalField;
    // GEODE's Version class was repackaged when serialization was modularized
    try {
      versionClass = Class.forName("org.apache.geode.internal.Version");
    } catch (ClassNotFoundException e) {
      try {
        versionClass = Class.forName("org.apache.geode.internal.serialization.Version");
      } catch (ClassNotFoundException e2) {
        System.out.println("classpath is " + System.getProperty("java.class.path"));
        throw new IllegalStateException(
            "Unable to locate Version.java in order to establish the product's serialization version",
            e2);
      }
    }
    try {
      currentOrdinalField = versionClass.getDeclaredField("CURRENT_ORDINAL");
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException(
          "Unable to locate Version.java's CURRENT_ORDINAL field in order to establish the product's serialization version",
          e);
    }
    currentOrdinalField.setAccessible(true);
    try {
      geodeCurrentVersionOrdinal = currentOrdinalField.getShort(null);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(
          "Unable to retrieve Version.java's CURRENT_ORDINAL field in order to establlish the product's serialization version",
          e);
    }
  }
}
