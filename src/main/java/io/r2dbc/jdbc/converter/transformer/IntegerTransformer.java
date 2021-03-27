// Created: 14.06.2019
package io.r2dbc.jdbc.converter.transformer;

/**
 * @author Thomas Freese
 */
public class IntegerTransformer extends AbstractObjectTransformer<Integer>
{
    /**
     * @see io.r2dbc.jdbc.converter.transformer.ObjectTransformer#transform(java.lang.Object)
     */
    @Override
    public Integer transform(final Object value)
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
