/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.io.jdbc;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class OracleJdbcAutoSchemaSinkTest {
    private OracleJdbcAutoSchemaSink oracleJdbcAutoSchemaSink = new OracleJdbcAutoSchemaSink();

    protected JdbcUtils.TableId tableId;
    protected JdbcUtils.ColumnId columnPK1;
    protected JdbcUtils.ColumnId columnPK2;
    protected JdbcUtils.ColumnId columnA;
    protected JdbcUtils.ColumnId columnB;
    protected JdbcUtils.ColumnId columnC;
    protected JdbcUtils.ColumnId columnD;
    protected List<String> pkColumns;
    protected List<String> columnsAtoD;

    @Before
    public void setup() {

//        tableId = JdbcUtils.TableId.of(null, null, "myTable");
//        columnPK1 = JdbcUtils.ColumnId.of(tableId, "id1", sqlDataType, typeName, position);
//        columnPK2 = JdbcUtils.ColumnId.of(tableId, "id2", sqlDataType, typeName, position);
//        columnA = JdbcUtils.ColumnId.of(tableId, "columnA", sqlDataType, typeName, position);
//        columnB = JdbcUtils.ColumnId.of(tableId, "columnB", sqlDataType, typeName, position);
//        columnC = JdbcUtils.ColumnId.of(tableId, "columnC", sqlDataType, typeName, position);
//        columnD = JdbcUtils.ColumnId.of(tableId, "columnD", sqlDataType, typeName, position);
        pkColumns = Arrays.asList("id1", "id2");
        columnsAtoD = Arrays.asList("columnA", "columnB", "columnC", "columnD");
//        JdbcUtils.TableDefinition tableDefinition = JdbcUtils.TableDefinition.of(tableId, all, pkColumns, columnsAtoD);
        oracleJdbcAutoSchemaSink = new OracleJdbcAutoSchemaSink();
    }

    @Test
    public void generateUpsertQueryStatement() {
        String expected = "merge into \"myTable\" using (select ? \"id1\", ? \"id2\", ? \"columnA\", " +
                "? \"columnB\", ? \"columnC\", ? \"columnD\" FROM dual) incoming on" +
                "(\"myTable\".\"id1\"=incoming.\"id1\" and \"myTable\".\"id2\"=incoming" +
                ".\"id2\") when matched then update set \"myTable\".\"columnA\"=incoming" +
                ".\"columnA\",\"myTable\".\"columnB\"=incoming.\"columnB\",\"myTable\"" +
                ".\"columnC\"=incoming.\"columnC\",\"myTable\".\"columnD\"=incoming" +
                ".\"columnD\" when not matched then insert(\"myTable\".\"columnA\"," +
                "\"myTable\".\"columnB\",\"myTable\".\"columnC\",\"myTable\".\"columnD\"," +
                "\"myTable\".\"id1\",\"myTable\".\"id2\") values(incoming.\"columnA\"," +
                "incoming.\"columnB\",incoming.\"columnC\",incoming.\"columnD\",incoming" +
                ".\"id1\",incoming.\"id2\")";
        String sql = oracleJdbcAutoSchemaSink.generateUpsertQueryStatementFrom("myTable", pkColumns, columnsAtoD);
        assertEquals(expected, sql);
    }
}