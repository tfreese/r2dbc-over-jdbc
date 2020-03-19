/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.converter.transformer;

/**
 * @author Thomas Freese
 */
public class DoubleTransformer extends AbstractObjectTransofrmer<Double>
{
    /**
     * Erstellt ein neues {@link DoubleTransformer} Object.
     */
    public DoubleTransformer()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.converter.transformer.ObjectTransformer#transform(java.lang.Object)
     */
    @Override
    public Double transform(final Object value)
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
