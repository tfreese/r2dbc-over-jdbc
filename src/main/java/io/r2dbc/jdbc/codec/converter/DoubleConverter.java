/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.codec.converter;

/**
 * @author Thomas Freese
 */
public class DoubleConverter extends AbstractConverter<Double>
{
    /**
     * Erstellt ein neues {@link DoubleConverter} Object.
     */
    public DoubleConverter()
    {
        super(Double.class);
    }

    /**
     * @see AbstractConverter#doConvertNullSafe(Object)
     */
    @Override
    protected Double doConvertNullSafe(final Object value)
    {
        if (value instanceof Number)
        {
            return ((Number) value).doubleValue();
        }
        else if (value instanceof String)
        {
            return Double.valueOf((String) value);
        }

        throw createCanNotConvertException(value);
    }
}
