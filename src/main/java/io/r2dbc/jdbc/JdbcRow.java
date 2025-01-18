// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.nio.ByteBuffer;
import java.sql.JDBCType;
import java.util.Map;
import java.util.Objects;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public class JdbcRow implements Row, Result.RowSegment {
    private final Codecs codecs;
    private final RowMetadata rowMetadata;
    private final Map<Integer, Object> values;

    public JdbcRow(final RowMetadata rowMetadata, final Map<Integer, Object> values, final Codecs codecs) {
        super();

        this.rowMetadata = Objects.requireNonNull(rowMetadata, "rowMetadata required");
        this.values = Objects.requireNonNull(values, "values required");
        this.codecs = Objects.requireNonNull(codecs, "codecs required");
    }

    @Override
    public Object get(final int index) {
        final ColumnMetadata metadata = getMetadata().getColumnMetadata(index);

        if (Clob.class.equals(metadata.getJavaType())) {
            // Clobs immer als String liefern.
            return get(index, String.class);
        }
        else if (Blob.class.equals(metadata.getJavaType())) {
            // Blobs immer als ByteBuffer liefern.
            return get(index, ByteBuffer.class);
        }

        return get(index, Object.class);
    }

    @Override
    public <T> T get(final int index, final Class<T> type) {
        if (type == null) {
            throw new IllegalArgumentException("type is null");
        }

        final Object value = values.get(index);

        if (value == null) {
            return null;
        }

        final ColumnMetadata metadata = getMetadata().getColumnMetadata(index);
        final JDBCType jdbcType = (JDBCType) metadata.getNativeTypeMetadata();

        return codecs.mapTo(jdbcType, type, value);
    }

    @Override
    public Object get(final String name) {
        final ColumnMetadata metadata = getMetadata().getColumnMetadata(name);

        final int column = ((JdbcColumnMetadata) metadata).getColumn();

        return get(column);
    }

    @Override
    public <T> T get(final String name, final Class<T> type) {
        if (name == null) {
            throw new IllegalArgumentException("name is null");
        }

        if (type == null) {
            throw new IllegalArgumentException("type is null");
        }

        final ColumnMetadata metadata = getMetadata().getColumnMetadata(name);

        final int column = ((JdbcColumnMetadata) metadata).getColumn();

        return get(column, type);
    }

    @Override
    public RowMetadata getMetadata() {
        return rowMetadata;
    }

    @Override
    public Row row() {
        return this;
    }
}
