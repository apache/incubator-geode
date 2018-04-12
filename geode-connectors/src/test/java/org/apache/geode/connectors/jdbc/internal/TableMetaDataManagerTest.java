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
package org.apache.geode.connectors.jdbc.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.JdbcConnectorException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class TableMetaDataManagerTest {
  private static final String TABLE_NAME = "testTable";
  private static final String KEY_COLUMN = "keyColumn";

  private TableMetaDataManager tableMetaDataManager;
  private Connection connection;
  DatabaseMetaData databaseMetaData;
  ResultSet tablesResultSet;
  ResultSet primaryKeysResultSet;
  ResultSet columnResultSet;

  @Before
  public void setup() throws Exception {
    tableMetaDataManager = new TableMetaDataManager();
    connection = mock(Connection.class);
    databaseMetaData = mock(DatabaseMetaData.class);
    when(connection.getMetaData()).thenReturn(databaseMetaData);
    tablesResultSet = mock(ResultSet.class);
    when(databaseMetaData.getTables(any(), any(), any(), any())).thenReturn(tablesResultSet);
    primaryKeysResultSet = mock(ResultSet.class);
    when(databaseMetaData.getPrimaryKeys(any(), any(), anyString()))
        .thenReturn(primaryKeysResultSet);
    columnResultSet = mock(ResultSet.class);
    when(databaseMetaData.getColumns(any(), any(), eq(TABLE_NAME), any()))
        .thenReturn(columnResultSet);
  }

  @Test
  public void returnsSinglePrimaryKeyColumnName() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);

    TableMetaDataView data = tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);
    assertThat(data.getKeyColumnName()).isEqualTo(KEY_COLUMN);
    verify(connection).getMetaData();
  }

  @Test
  public void returnsDefaultQuoteString() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);

    TableMetaDataView data = tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);

    assertThat(data.getIdentifierQuoteString()).isEqualTo("");
    verify(connection).getMetaData();
  }

  @Test
  public void returnsQuoteStringFromMetaData() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    String expectedQuoteString = "123";
    when(databaseMetaData.getIdentifierQuoteString()).thenReturn(expectedQuoteString);

    TableMetaDataView data = tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);

    assertThat(data.getIdentifierQuoteString()).isEqualTo(expectedQuoteString);
    verify(connection).getMetaData();
  }

  @Test
  public void secondCallDoesNotUseMetaData() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);

    tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);
    tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);
    verify(connection).getMetaData();
  }

  @Test
  public void throwsExceptionWhenFailsToGetTableMetadata() throws Exception {
    SQLException cause = new SQLException("sql message");
    when(connection.getMetaData()).thenThrow(cause);

    assertThatThrownBy(() -> tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class).hasMessageContaining("sql message");
  }

  @Test
  public void throwsExceptionWhenDesiredTableNotFound() throws Exception {
    when(tablesResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.getString("TABLE_NAME")).thenReturn("otherTable");

    assertThatThrownBy(() -> tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class)
        .hasMessage("no table was found that matches testTable");
  }

  @Test
  public void throwsExceptionIfTableHasCompositePrimaryKey() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true);

    assertThatThrownBy(() -> tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class)
        .hasMessage("The table " + TABLE_NAME + " has more than one primary key column.");
  }

  @Test
  public void returnsExactMatchTableNameWhenTwoTablesHasCaseInsensitiveSameName() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    when(tablesResultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME.toUpperCase())
        .thenReturn(TABLE_NAME);

    TableMetaDataView data = tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);

    assertThat(data.getTableName()).isEqualTo(TABLE_NAME);
  }

  @Test
  public void returnsMatchTableNameWhenMetaDataHasOneInexactMatch() throws Exception {
    setupPrimaryKeysMetaData();
    when(databaseMetaData.getColumns(any(), any(), eq(TABLE_NAME.toUpperCase()), any()))
        .thenReturn(columnResultSet);
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME.toUpperCase());

    TableMetaDataView data = tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);

    assertThat(data.getTableName()).isEqualTo(TABLE_NAME.toUpperCase());
  }

  @Test
  public void throwsExceptionWhenTwoTablesHasCaseInsensitiveSameName() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);

    when(tablesResultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME.toLowerCase())
        .thenReturn(TABLE_NAME.toUpperCase());

    assertThatThrownBy(() -> tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class)
        .hasMessage("Duplicate tables that match region name");
  }

  @Test
  public void throwsExceptionWhenNoPrimaryKeyInTable() throws Exception {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(false);

    assertThatThrownBy(() -> tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME))
        .isInstanceOf(JdbcConnectorException.class)
        .hasMessage("The table " + TABLE_NAME + " does not have a primary key column.");
  }

  @Test
  public void unknownColumnsDataTypeIsZero() throws SQLException {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);

    TableMetaDataView data = tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);
    int dataType = data.getColumnDataType("unknownColumn");

    assertThat(dataType).isEqualTo(0);
  }

  @Test
  public void validateExpectedDataTypes() throws SQLException {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    when(columnResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    String columnName1 = "columnName1";
    int columnDataType1 = 1;
    String columnName2 = "columnName2";
    int columnDataType2 = 2;
    when(columnResultSet.getString("COLUMN_NAME")).thenReturn(columnName1).thenReturn(columnName2);
    when(columnResultSet.getInt("DATA_TYPE")).thenReturn(columnDataType1)
        .thenReturn(columnDataType2);

    TableMetaDataView data = tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);
    int dataType1 = data.getColumnDataType(columnName1);
    int dataType2 = data.getColumnDataType(columnName2);

    assertThat(dataType1).isEqualTo(columnDataType1);
    assertThat(dataType2).isEqualTo(columnDataType2);
    verify(primaryKeysResultSet).close();
    verify(columnResultSet).close();
  }

  @Test
  public void validateExpectedColumnNames() throws SQLException {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);
    when(columnResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    String columnName1 = "columnName1";
    int columnDataType1 = 1;
    String columnName2 = "columnName2";
    int columnDataType2 = 2;
    when(columnResultSet.getString("COLUMN_NAME")).thenReturn(columnName1).thenReturn(columnName2);
    when(columnResultSet.getInt("DATA_TYPE")).thenReturn(columnDataType1)
        .thenReturn(columnDataType2);
    Set<String> expectedColumnNames = new HashSet<>(Arrays.asList(columnName1, columnName2));

    TableMetaDataView data = tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);
    Set<String> columnNames = data.getColumnNames();

    assertThat(columnNames).isEqualTo(expectedColumnNames);
  }

  @Test
  public void validateThatCloseOnPrimaryKeysResultSetIsCalledByGetTableMetaDataView()
      throws SQLException {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);

    TableMetaDataView data = tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);

    verify(primaryKeysResultSet).close();
  }

  @Test
  public void validateThatCloseOnColumnResultSetIsCalledByGetTableMetaDataView()
      throws SQLException {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);

    TableMetaDataView data = tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);

    verify(columnResultSet).close();
  }

  @Test
  public void validateTableNameIsSetByGetTableMetaDataView() throws SQLException {
    setupPrimaryKeysMetaData();
    when(primaryKeysResultSet.next()).thenReturn(true).thenReturn(false);

    TableMetaDataView data = tableMetaDataManager.getTableMetaDataView(connection, TABLE_NAME);

    assertThat(data.getTableName()).isEqualTo(TABLE_NAME);
  }

  private void setupPrimaryKeysMetaData() throws SQLException {
    when(primaryKeysResultSet.getString("COLUMN_NAME")).thenReturn(KEY_COLUMN);
    when(tablesResultSet.next()).thenReturn(true).thenReturn(false);
    when(tablesResultSet.getString("TABLE_NAME")).thenReturn(TABLE_NAME);
  }
}
