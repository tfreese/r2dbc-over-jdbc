/**
 * Created: 14.06.2019
 */

package io.r2dbc.jdbc;

import java.util.Map;
import java.util.Objects;
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
     * @param valuesByColumnName {@link Map}
     */
    public JdbcRow(final Map<Object, Object> valuesByColumnName)
    {
        super();

        this.values = Objects.requireNonNull(valuesByColumnName, "values must not be null");
    }

    /**
     * @see io.r2dbc.spi.Row#get(java.lang.Object, java.lang.Class)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(final Object identifier, final Class<T> type)
    {
        Objects.requireNonNull(identifier, "identifier must not be null");
        Objects.requireNonNull(type, "type must not be null");

        Object key = identifier;

        if (key instanceof String)
        {
            key = ((String) identifier).toUpperCase();
        }
        else if (identifier instanceof Integer)
        {
            // NO OP
        }
        else
        {
            throw new IllegalArgumentException(
                    String.format("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String column name.", identifier));
        }

        Object value = this.values.get(key);

        return (T) value;
    }
}
