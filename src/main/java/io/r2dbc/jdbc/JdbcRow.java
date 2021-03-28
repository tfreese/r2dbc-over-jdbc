// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.sql.JDBCType;
import java.util.Map;
import java.util.Objects;

import io.r2dbc.jdbc.codecs.Codecs;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Row;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public class JdbcRow implements Row
{
    /**
    *
    */
    private final Codecs codecs;

    /**
     *
     */
    private final JdbcRowMetadata rowMetadata;

    /**
     *
     */
    private final Map<Integer, Object> values;

    /**
     * Erstellt ein neues {@link JdbcRow} Object.
     *
     * @param rowMetadata {@link JdbcRowMetadata}
     * @param values {@link Map}
     * @param codecs {@link Codecs}
     */
    public JdbcRow(final JdbcRowMetadata rowMetadata, final Map<Integer, Object> values, final Codecs codecs)
    {
        super();

        this.rowMetadata = Objects.requireNonNull(rowMetadata, "rowMetadata required");
        this.values = Objects.requireNonNull(values, "values required");
        this.codecs = Objects.requireNonNull(codecs, "codecs required");
    }

    /**
     * @see io.r2dbc.spi.Row#get(int, java.lang.Class)
     */
    @Override
    public <T> T get(final int index, final Class<T> type)
    {
        if (type == null)
        {
            throw new IllegalArgumentException("type is null");
        }

        Object value = this.values.get(index);

        if (value == null)
        {
            return null;
        }

        ColumnMetadata metadata = this.rowMetadata.getColumnMetadata(index);
        JDBCType jdbcType = (JDBCType) metadata.getNativeTypeMetadata();

        T mappedValue = this.codecs.mapTo(jdbcType, type, value);

        return mappedValue;
    }

    /**
     * @see io.r2dbc.spi.Row#get(java.lang.String, java.lang.Class)
     */
    @Override
    public <T> T get(final String name, final Class<T> type)
    {
        if (name == null)
        {
            throw new IllegalArgumentException("name is null");
        }

        if (type == null)
        {
            throw new IllegalArgumentException("type is null");
        }

        ColumnMetadata metadata = this.rowMetadata.getColumnMetadata(name);

        int column = ((JdbcColumnMetadata) metadata).getColumn();

        return get(column, type);
    }
}
