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

        ResultSetMetaData metaData = resultSet.getMetaData();
        List<ColumnMetadata> list = new ArrayList<>();

        for (int column = 1; column <= metaData.getColumnCount(); column++) {
            String name = metaData.getColumnLabel(column);
            int sqlType = metaData.getColumnType(column);
            JDBCType jdbcType = JDBCType.valueOf(sqlType);
            int precision = metaData.getPrecision(column);
            int scale = metaData.getScale(column);

            Nullability nullability = switch (metaData.isNullable(column)) {
                case ResultSetMetaData.columnNoNulls -> Nullability.NON_NULL;
                case ResultSetMetaData.columnNullable -> Nullability.NULLABLE;

                default -> Nullability.UNKNOWN;
            };

            Class<?> javaType = codecs.getJavaType(jdbcType);

            list.add(new JdbcColumnMetadata(name, column - 1, javaType, jdbcType, nullability, precision, scale));
        }

        return new JdbcRowMetadata(list);
    }

    private final List<ColumnMetadata> columnMetaDatas;

    private final Map<String, ColumnMetadata> columnMetaDatasByName = new LinkedHashMap<>();

    public JdbcRowMetadata(final List<ColumnMetadata> columnMetaDatas) {
        super();

        this.columnMetaDatas = Objects.requireNonNull(columnMetaDatas, "columnMetaDatas must not be null");

        columnMetaDatas.forEach(cmd -> {
            // Bei Spalten mit identischen Namen, immer den ersten nehmen, laut Spezifikation.
            String name = cmd.getName().toLowerCase();

            ColumnMetadata old = this.columnMetaDatasByName.put(name, cmd);

            if (old != null) {
                this.columnMetaDatasByName.put(name, old);
            }
        });
    }

    @Override
    public boolean contains(final String columnName) {
        return this.columnMetaDatasByName.containsKey(columnName.toLowerCase());
    }

    @Override
    public ColumnMetadata getColumnMetadata(final String name) {
        if (name == null) {
            throw new IllegalArgumentException("name is null");
        }

        ColumnMetadata metaData = this.columnMetaDatasByName.get(name.toLowerCase());

        if (metaData == null) {
            throw new NoSuchElementException("No MetaData for Name: " + name);
        }

        return metaData;
    }

    @Override
    public ColumnMetadata getColumnMetadata(final int index) {
        if ((index < 0) || (index >= this.columnMetaDatas.size())) {
            throw new ArrayIndexOutOfBoundsException("Index: " + index + ", Size: " + this.columnMetaDatas.size());
        }

        ColumnMetadata metaData = this.columnMetaDatas.get(index);

        if (metaData == null) {
            throw new NoSuchElementException("No MetaData for Index: " + index);
        }

        return metaData;
    }

    @Override
    public List<ColumnMetadata> getColumnMetadatas() {
        return List.copyOf(this.columnMetaDatas);
    }
}
