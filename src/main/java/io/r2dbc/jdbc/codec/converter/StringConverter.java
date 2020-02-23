/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.codec.converter;

import io.r2dbc.jdbc.util.R2dbcUtils;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;

/**
 * @author Thomas Freese
 */
public class StringConverter extends AbstractConverter<String>
{
    /**
     * Erstellt ein neues {@link StringConverter} Object.
     */
    public StringConverter()
    {
        super();
    }

    /**
     * @see AbstractConverter#doConvertNullSafe(Object)
     */
    @Override
    protected String doConvertNullSafe(final Object value)
    {
        if (value instanceof String)
        {
            return (String) value;
        }
        else if (value instanceof Number)
        {
            return ((Number) value).toString();
        }
        else if (value instanceof Blob)
        {
            Blob blob = (Blob) value;

            String string = R2dbcUtils.blobToString(blob);

            return string;
        }
        else if (value instanceof Clob)
        {
            Clob clob = (Clob) value;

            String string = R2dbcUtils.clobToString(clob);

            return string;
        }

        return value.toString();
    }
}
