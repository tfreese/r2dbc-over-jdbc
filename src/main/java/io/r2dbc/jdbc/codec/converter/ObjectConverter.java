/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.codec.converter;

import java.nio.ByteBuffer;
import io.r2dbc.jdbc.util.R2dbcUtils;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;

/**
 * Fallback-Converter, returns the same Object.
 *
 * @author Thomas Freese
 */
public class ObjectConverter extends AbstractConverter<Object>
{
    /**
     * Fallback-Converter, returns the same Object.
     */
    public static final Converter<Object> INSTANCE = new ObjectConverter();

    /**
     * Erstellt ein neues {@link ObjectConverter} Object.
     */
    public ObjectConverter()
    {
        super();
    }

    /**
     * @see io.r2dbc.jdbc.codec.converter.AbstractConverter#doConvertNullSafe(java.lang.Object)
     */
    @Override
    protected Object doConvertNullSafe(final Object value)
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
