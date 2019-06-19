/**
 * Created: 14.06.2019
 */

package io.r2dbc.jdbc;

import java.util.Map;
import java.util.Objects;
import io.r2dbc.jdbc.codec.Codecs;
import io.r2dbc.jdbc.codec.converter.Converter;
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
    private Map<Object, Object> values = null;

    /**
     * Erstellt ein neues {@link JdbcRow} Object.
     *
     * @param values {@link Map}
     */
    public JdbcRow(final Map<Object, Object> values)
    {
        super();

        this.values = Objects.requireNonNull(values, "values must not be null");

        // if (this.values.isEmpty())
        // {
        //
        // }
    }

    /**
     * @see io.r2dbc.spi.Row#get(java.lang.Object, java.lang.Class)
     */
    @Override
    public <T> T get(final Object identifier, final Class<T> type)
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

        Converter<T> converter = Codecs.getConverter(type);

        return converter.convert(value);
    }
}
