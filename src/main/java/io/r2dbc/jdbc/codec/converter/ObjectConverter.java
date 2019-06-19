/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.codec.converter;

/**
 * Fallback-Converter.
 *
 * @author Thomas Freese
 */
public class ObjectConverter extends AbstractConverter<Object>
{
    /**
     * Fallback-Converter.
     */
    public static final Converter<Object> INSTANCE = new ObjectConverter();

    /**
     * Erstellt ein neues {@link ObjectConverter} Object.
     */
    public ObjectConverter()
    {
        super(Object.class);
    }

    /**
     * @see io.r2dbc.jdbc.codec.converter.AbstractConverter#doConvertNullSafe(java.lang.Object)
     */
    @Override
    protected Object doConvertNullSafe(final Object value)
    {
        return value;
    }
}
