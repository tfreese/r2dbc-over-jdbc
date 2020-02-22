/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.codec.converter;

/**
 * @author Thomas Freese
 */
public class ByteConverter extends AbstractConverter<Byte>
{
    /**
     * Erstellt ein neues {@link ByteConverter} Object.
     */
    public ByteConverter()
    {
        super();
    }

    /**
     * @see AbstractConverter#doConvertNullSafe(Object)
     */
    @Override
    protected Byte doConvertNullSafe(final Object value)
    {
        if (value instanceof Number)
        {
            return ((Number) value).byteValue();
        }
        else if (value instanceof String)
        {
            return Byte.valueOf((String) value);
        }

        throw createCanNotConvertException(value);
    }
}
