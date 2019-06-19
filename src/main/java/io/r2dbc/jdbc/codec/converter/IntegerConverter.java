/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.codec.converter;

/**
 * @author Thomas Freese
 */
public class IntegerConverter extends AbstractConverter<Integer>
{
    /**
     * Erstellt ein neues {@link IntegerConverter} Object.
     */
    public IntegerConverter()
    {
        super(Integer.class);
    }

    /**
     * @see io.r2dbc.jdbc.codec.converter.AbstractConverter#doConvertNullSafe(java.lang.Object)
     */
    @Override
    protected Integer doConvertNullSafe(final Object value)
    {
        if (value instanceof Number)
        {
            return ((Number) value).intValue();
        }
        else if (value instanceof String)
        {
            return Integer.valueOf((String) value);
        }

        throw createCanNotConvertException(value);
    }
}
