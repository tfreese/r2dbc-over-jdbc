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
    private Map<Object, Object> values;

    /**
     * Erstellt ein neues {@link JdbcRow} Object.
     *
     * @param values {@link Map}
     */
    public JdbcRow(final Map<Object, Object> values)
    {
        super();

        this.values = Objects.requireNonNull(values, "values must not be null");
    }

    /**
     * @see io.r2dbc.spi.Row#get(int, java.lang.Class)
     */
    @Override
    public <T> T get(final int index, final Class<T> type)
    {
        Object value = this.values.get(index);

        if (value == null)
        {
            throw new IllegalArgumentException(
                    String.format("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String column name.", index));
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

        Object value = this.values.get(name.toLowerCase());

        if (value == null)
        {
            value = this.values.get(name.toUpperCase());
        }

        if (value == null)
        {
            throw new IllegalArgumentException(String.format("Column identifier '%s' does not exist", name));
        }

        ObjectTransformer<T> transformer = Converters.getTransformer(type);

        return transformer.transform(value);
    }
}
