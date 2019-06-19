/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.codec.converter;

/**
 * @author Thomas Freese
 */
public class LongConverter extends AbstractConverter<Long>
{
    /**
     * Erstellt ein neues {@link LongConverter} Object.
     */
    public LongConverter()
    {
        super(Long.class);
    }

    /**
     * @see AbstractConverter#doConvertNullSafe(Object)
     */
    @Override
    protected Long doConvertNullSafe(final Object value)
    {
        if (value instanceof Number)
        {
            return ((Number) value).longValue();
        }
        else if (value instanceof String)
        {
            return Long.valueOf((String) value);
        }

        throw createCanNotConvertException(value);
    }
}
