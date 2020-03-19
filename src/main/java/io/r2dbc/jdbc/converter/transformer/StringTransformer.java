/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.converter.transformer;

import io.r2dbc.jdbc.util.R2dbcUtils;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;

/**
 * @author Thomas Freese
 */
public class StringTransformer extends AbstractObjectTransofrmer<String>
{
    /**
     * Erstellt ein neues {@link StringTransformer} Object.
     */
    public StringTransformer()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.converter.transformer.ObjectTransformer#transform(java.lang.Object)
     */
    @Override
    public String transform(final Object value)
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
