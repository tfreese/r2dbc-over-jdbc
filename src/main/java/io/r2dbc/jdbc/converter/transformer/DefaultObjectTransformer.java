/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.converter.transformer;

import java.nio.ByteBuffer;
import io.r2dbc.jdbc.util.R2dbcUtils;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;

/**
 * Fallback-Converter, returns the same Object.
 *
 * @author Thomas Freese
 */
public class DefaultObjectTransformer extends AbstractObjectTransofrmer<Object>
{
    /**
     * Fallback-Converter, returns the same Object.
     */
    public static final ObjectTransformer<Object> INSTANCE = new DefaultObjectTransformer();

    /**
     * Erstellt ein neues {@link DefaultObjectTransformer} Object.
     */
    public DefaultObjectTransformer()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.converter.transformer.ObjectTransformer#transform(java.lang.Object)
     */
    @Override
    public Object transform(final Object value)
    {
        if (value instanceof Clob)
        {
            Clob clob = (Clob) value;

            String string = R2dbcUtils.clobToString(clob);

            return string;
        }

        if (value instanceof Blob)
        {
            Blob blob = (Blob) value;

            ByteBuffer byteBuffer = R2dbcUtils.blobToByteBuffer(blob);

            return byteBuffer;
        }

        return value;
    }
}
