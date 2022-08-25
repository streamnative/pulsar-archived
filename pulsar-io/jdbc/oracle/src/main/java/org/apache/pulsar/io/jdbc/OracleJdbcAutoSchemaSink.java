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

import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.pulsar.io.jdbc.JdbcUtils.createListOfColumns;
import static org.apache.pulsar.io.jdbc.JdbcUtils.quoted;

@Connector(
    name = "jdbc-oracle",
    type = IOType.SINK,
    help = "A simple JDBC sink for Oracle that writes pulsar messages to a database table",
    configClass = JdbcSinkConfig.class
)
public class OracleJdbcAutoSchemaSink extends BaseJdbcAutoSchemaSink {
    @Override
    public String generateUpsertQueryStatement() {
        List<String> keyColumns = tableDefinition.getKeyColumns().stream().map(JdbcUtils.ColumnId::getName).collect(Collectors.toList());
        List<String> nonKeyColumns = tableDefinition.getNonKeyColumns().stream().map(JdbcUtils.ColumnId::getName).collect(Collectors.toList());
        String tableName = tableDefinition.getTableId().getTableName();
        return generateUpsertQueryStatementFrom(tableName, keyColumns, nonKeyColumns);
    }

    protected String generateUpsertQueryStatementFrom(String tableName, List<String> keyColumns, List<String> nonKeyColumns) {
        List<String> allColumns = Stream.concat(nonKeyColumns.stream(), keyColumns.stream())
                .collect(Collectors.toList());

        StringBuilder builder = new StringBuilder();
        // define the transform
        final UnaryOperator<String> transform = (col) -> {
            StringBuilder sb = new StringBuilder();
            return sb.append(quoted(tableName))
                    .append(".")
                    .append(quoted(col))
                    .append("=incoming.")
                    .append(quoted(col)).toString();
        };

        builder.append("merge into ");
        builder.append(quoted(tableName));
        builder.append(" using (select ");
        // append list 1
        builder.append(createListOfColumns(Stream.concat(keyColumns.stream(), nonKeyColumns.stream())
                .collect(Collectors.toList()), ", ",  s -> "? " + quoted(s)));
        builder.append(" FROM dual) incoming on(");
        // append list 2
        builder.append(createListOfColumns(keyColumns, " and ", transform));
        builder.append(")");
        // append list 3
        if (nonKeyColumns != null && !nonKeyColumns.isEmpty()) {
            builder.append(" when matched then update set ");
            builder.append(createListOfColumns(nonKeyColumns, ",", transform));
        }
        builder.append(" when not matched then insert(");
        // append list 4
        builder.append(createListOfColumns(Stream.concat(nonKeyColumns.stream(), keyColumns.stream())
                .collect(Collectors.toList()), ",", s -> quoted(tableName) + "." + quoted(s)));
        builder.append(") values(");
        // append list 5
        builder.append(createListOfColumns(Stream.concat(nonKeyColumns.stream(), keyColumns.stream())
                .collect(Collectors.toList()), ",", s -> "incoming." + quoted(s)));
        builder.append(")");
        return builder.toString();
    }
}
