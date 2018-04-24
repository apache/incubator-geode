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
package org.apache.geode.management.internal.cli;

import java.nio.file.Path;
import java.text.DateFormat;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.management.internal.cli.json.GfJsonObject;
import org.apache.geode.management.internal.cli.result.model.CompositeResultModel;
import org.apache.geode.management.internal.cli.result.model.ErrorResultModel;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.SectionResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;

/**
 * @since GemFire 7.0
 */
public class CommandResponse {

  private final String sender;
  private final String version;
  private final int status;
  private final String contentType;
  private final String page;
  private final String when;
  private final String tokenAccessor;
  private final String debugInfo;
  private final Data data;
  private final boolean failedToPersist;
  private final String fileToDownload;
  private final boolean isLegacy;

  CommandResponse(String sender, String contentType, int status, String page, String tokenAccessor,
      String debugInfo, String header, GfJsonObject content, String footer, boolean failedToPersist,
      Path fileToDownload) {
    this.sender = sender;
    this.contentType = contentType;
    this.status = status;
    this.page = page;
    this.tokenAccessor = tokenAccessor;
    this.debugInfo = debugInfo;
    this.data = new LegacyData(header, content, footer);
    this.when = DateFormat.getInstance().format(new java.util.Date());
    this.version = GemFireVersion.getGemFireVersion();
    this.failedToPersist = failedToPersist;
    if (fileToDownload != null) {
      this.fileToDownload = fileToDownload.toString();
    } else {
      this.fileToDownload = null;
    }
    this.isLegacy = true;
  }

  CommandResponse(ResultModel result, String sender, String page, String tokenAccessor,
      String debugInfo, Path fileToDownload) {
    this.sender = sender;
    this.contentType = result.getType();
    this.status = result.getStatus().getCode();
    this.page = page;
    this.tokenAccessor = tokenAccessor;
    this.debugInfo = debugInfo;
    this.data = result;
    this.when = DateFormat.getInstance().format(new java.util.Date());
    this.version = GemFireVersion.getGemFireVersion();
    this.failedToPersist = false; // result.failedToPersist();
    if (fileToDownload != null) {
      this.fileToDownload = fileToDownload.toString();
    } else {
      this.fileToDownload = null;
    }
    this.isLegacy = false;
  }

  // For de-serializing
  CommandResponse(GfJsonObject jsonObject) {
    this.sender = jsonObject.getString("sender");
    this.contentType = jsonObject.getString("contentType");
    this.status = jsonObject.getInt("status");
    this.page = jsonObject.getString("page");
    this.tokenAccessor = jsonObject.getString("tokenAccessor");
    this.debugInfo = jsonObject.getString("debugInfo");
    this.data = new LegacyData(jsonObject.getJSONObject("data"));
    this.when = jsonObject.getString("when");
    this.version = jsonObject.getString("version");
    this.failedToPersist = jsonObject.getBoolean("failedToPersist");
    this.fileToDownload = jsonObject.getString("fileToDownload");
    this.isLegacy = true;
  }

  /**
   * @return the sender
   */
  public String getSender() {
    return sender;
  }

  /**
   * @return the version
   */
  public String getVersion() {
    return version;
  }

  /**
   * @return the status
   */
  public int getStatus() {
    return status;
  }

  /**
   * @return the contentType
   */
  public String getContentType() {
    return contentType;
  }

  /**
   * @return the page
   */
  public String getPage() {
    return page;
  }

  public String getFileToDownload() {
    return fileToDownload;
  }

  /**
   * @return the when
   */
  public String getWhen() {
    return when;
  }

  /**
   * @return the tokenAccessor
   */
  public String getTokenAccessor() {
    return tokenAccessor;
  }

  /**
   * @return the data
   */
  public Data getData() {
    return data;
  }

  /**
   * @return the debugInfo
   */
  public String getDebugInfo() {
    return debugInfo;
  }

  public boolean isFailedToPersist() {
    return failedToPersist;
  }

  public boolean isLegacy() {
    return isLegacy;
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
      property = "modelClass")
  @JsonSubTypes({@JsonSubTypes.Type(value = TabularResultModel.class),
      @JsonSubTypes.Type(value = CompositeResultModel.class),
      @JsonSubTypes.Type(value = SectionResultModel.class),
      @JsonSubTypes.Type(value = InfoResultModel.class),
      @JsonSubTypes.Type(value = ErrorResultModel.class)})
  public interface Data {
    String getHeader();

    String getFooter();

    Object getContent();
  }

  public static class LegacyData implements Data {
    private String header;
    private GfJsonObject content;
    private String footer;

    public LegacyData(String header, GfJsonObject content, String footer) {
      this.header = header;
      this.content = content;
      this.footer = footer;
    }

    public LegacyData(GfJsonObject dataJsonObject) {
      this.header = dataJsonObject.getString("header");
      this.content = dataJsonObject.getJSONObject("content");
      this.footer = dataJsonObject.getString("footer");
    }

    /**
     * @return the header
     */
    public String getHeader() {
      return header;
    }

    /**
     * @return the content
     */
    public Object getContent() {
      return content.getInternalJsonObject();
    }

    /**
     * @return the footer
     */
    public String getFooter() {
      return footer;
    }

    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("Data [header=").append(header).append(", content=").append(content)
          .append(", footer=").append(footer).append("]");
      return builder.toString();
    }
  }

  public static class ResultModelData implements Data {
    private ResultModel content;

    public ResultModelData(ResultModel content) {
      this.content = content;
    }

    /**
     * @return the header
     */
    public String getHeader() {
      return content.getHeader();
    }

    /**
     * @return the content
     */
    public Object getContent() {
      return content;
    }

    /**
     * @return the footer
     */
    public String getFooter() {
      return content.getFooter();
    }

    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("Data [header=").append(content.getHeader()).append(", content=")
          .append(content).append(", footer=").append(content.getFooter()).append("]");
      return builder.toString();
    }
  }
}
