// Created: 14.06.2019
package io.r2dbc.jdbc.converter.transformer;

/**
 * @author Thomas Freese
 */
public class LongTransformer extends AbstractObjectTransofrmer<Long>
{
    /**
     * @see io.r2dbc.jdbc.converter.transformer.ObjectTransformer#transform(java.lang.Object)
     */
    @Override
    public Long transform(final Object value)
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
