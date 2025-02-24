// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Nullability;
import io.r2dbc.spi.RowMetadata;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public class JdbcRowMetadata implements RowMetadata {
    public static RowMetadata of(final ResultSet resultSet, final Codecs codecs) throws SQLException {
        if (resultSet == null) {
            return new JdbcRowMetadata(Collections.emptyList());
        }

        final ResultSetMetaData metaData = resultSet.getMetaData();
        final List<ColumnMetadata> list = new ArrayList<>();

        for (int column = 1; column <= metaData.getColumnCount(); column++) {
            final String name = metaData.getColumnLabel(column);
            final int sqlType = metaData.getColumnType(column);
            final JDBCType jdbcType = JDBCType.valueOf(sqlType);
            final int precision = metaData.getPrecision(column);
            final int scale = metaData.getScale(column);

            final Nullability nullability = switch (metaData.isNullable(column)) {
                case ResultSetMetaData.columnNoNulls -> Nullability.NON_NULL;
                case ResultSetMetaData.columnNullable -> Nullability.NULLABLE;

                default -> Nullability.UNKNOWN;
            };

            final Class<?> javaType = codecs.getJavaType(jdbcType);

            list.add(new JdbcColumnMetadata(name, column - 1, javaType, jdbcType, nullability, precision, scale));
        }

        return new JdbcRowMetadata(list);
    }

    private final Map<String, ColumnMetadata> columnMetaDataByName = new LinkedHashMap<>();
    private final List<ColumnMetadata> columnMetaDataList;

    public JdbcRowMetadata(final List<ColumnMetadata> columnMetaDataList) {
        super();

        this.columnMetaDataList = Objects.requireNonNull(columnMetaDataList, "columnMetaDataList required");

        columnMetaDataList.forEach(cmd -> {
            // Bei Spalten mit identischen Namen, immer den ersten nehmen, laut Spezifikation.
            final String name = cmd.getName().toLowerCase();

            final ColumnMetadata old = columnMetaDataByName.put(name, cmd);

            if (old != null) {
                columnMetaDataByName.put(name, old);
            }
        });
    }

    @Override
    public boolean contains(final String columnName) {
        return columnMetaDataByName.containsKey(columnName.toLowerCase());
    }

    @Override
    public ColumnMetadata getColumnMetadata(final String name) {
        if (name == null) {
            throw new IllegalArgumentException("name is null");
        }

        final ColumnMetadata metaData = columnMetaDataByName.get(name.toLowerCase());

        if (metaData == null) {
            throw new NoSuchElementException("No MetaData for Name: " + name);
        }

        return metaData;
    }

    @Override
    public ColumnMetadata getColumnMetadata(final int index) {
        if (index < 0 || index >= columnMetaDataList.size()) {
            throw new ArrayIndexOutOfBoundsException("Index: " + index + ", Size: " + columnMetaDataList.size());
        }

        final ColumnMetadata metaData = columnMetaDataList.get(index);

        if (metaData == null) {
            throw new NoSuchElementException("No MetaData for Index: " + index);
        }

        return metaData;
    }

    @Override
    public List<ColumnMetadata> getColumnMetadatas() {
        return List.copyOf(columnMetaDataList);
    }
}
