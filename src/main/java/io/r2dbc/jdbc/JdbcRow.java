/**
 * Created: 14.06.2019
 */

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
        return getByIdentifier(index, type);
    }

    /**
     * @see io.r2dbc.spi.Row#get(java.lang.String, java.lang.Class)
     */
    @Override
    public <T> T get(final String name, final Class<T> type)
    {
        return getByIdentifier(name, type);
    }

    /**
     * @param <T> Value Type
     * @param identifier Object
     * @param type Class
     * @return OBject
     */
    private <T> T getByIdentifier(final Object identifier, final Class<T> type)
    {
        Objects.requireNonNull(identifier, "identifier must not be null");
        Objects.requireNonNull(type, "type must not be null");

        final Object key;

        if (identifier instanceof String)
        {
            key = ((String) identifier).toUpperCase();
        }
        else if (identifier instanceof Integer)
        {
            key = identifier;
        }
        else
        {
            throw new IllegalArgumentException(
                    String.format("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String column name.", identifier));
        }

        if (!this.values.containsKey(key))
        {
            throw new IllegalArgumentException(String.format("Column identifier '%s' does not exist", key));
        }

        Object value = this.values.get(key);

        if (value == null)
        {
            return null;
        }

        ObjectTransformer<T> transformer = Converters.getTransformer(type);

        return transformer.transform(value);
    }
}
