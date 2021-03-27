// Created: 14.06.2019
package io.r2dbc.jdbc.converter.transformer;

/**
 * @author Thomas Freese
 */
public class ByteTransformer extends AbstractObjectTransformer<Byte>
{
    /**
     * @see io.r2dbc.jdbc.converter.transformer.ObjectTransformer#transform(java.lang.Object)
     */
    @Override
    public Byte transform(final Object value)
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
