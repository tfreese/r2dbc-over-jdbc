/**
 * Created: 22.02.2020
 */

package io.r2dbc.jdbc.codec.converter;

import io.r2dbc.spi.Blob;

/**
 * @author Thomas Freese
 */
public class BlobConverter extends AbstractConverter<Blob>
{
    /**
     * Erstellt ein neues {@link BlobConverter} Object.
     */
    public BlobConverter()
    {
        super();

    }

    /**
     * @see io.r2dbc.jdbc.codec.converter.AbstractConverter#doConvertNullSafe(java.lang.Object)
     */
    @Override
    protected Blob doConvertNullSafe(final Object value)
    {
        if (value instanceof Blob)
        {
            return (Blob) value;
        }

        throw createCanNotConvertException(value);
    }
}
