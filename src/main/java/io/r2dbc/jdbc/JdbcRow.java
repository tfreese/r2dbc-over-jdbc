// Created: 14.06.2019
package io.r2dbc.jdbc;

import java.util.Map;
import java.util.Objects;

import io.r2dbc.jdbc.converter.Converters;
import io.r2dbc.jdbc.converter.transformer.ObjectTransformer;
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
    private final JdbcRowMetadata rowMetadata;

    /**
     *
     */
    private final Map<Object, Object> values;

    /**
     * Erstellt ein neues {@link JdbcRow} Object.
     *
     * @param rowMetadata {@link JdbcRowMetadata}
     * @param values {@link Map}
     */
    public JdbcRow(final JdbcRowMetadata rowMetadata, final Map<Object, Object> values)
    {
        super();

        this.rowMetadata = Objects.requireNonNull(rowMetadata, "rowMetadata required");
        this.values = Objects.requireNonNull(values, "values required");
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
            // throw new IllegalArgumentException(
            // String.format("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String column name.", index));
            return null;
        }

        if (Object.class.equals(type))
        {
            // loosest possible match
            ObjectTransformer<T> transformer = Converters.getTransformer(this.rowMetadata.getColumnMetadata(index).getJavaType());

            return transformer.transform(value);
        }

        ObjectTransformer<T> transformer = Converters.getTransformer(type);

        return transformer.transform(value);

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

        String key = name.toLowerCase();
        Object value = this.values.get(key);

        if (value == null)
        {
            key = name.toUpperCase();
            value = this.values.get(key);
        }

        if (value == null)
        {
            // throw new IllegalArgumentException(String.format("Column identifier '%s' does not exist", name));
            return null;
        }

        if (Object.class.equals(type))
        {
            // loosest possible match
            ObjectTransformer<T> transformer = Converters.getTransformer(this.rowMetadata.getColumnMetadata(key).getJavaType());

            return transformer.transform(value);
        }

        ObjectTransformer<T> transformer = Converters.getTransformer(type);

        return transformer.transform(value);
    }
}
