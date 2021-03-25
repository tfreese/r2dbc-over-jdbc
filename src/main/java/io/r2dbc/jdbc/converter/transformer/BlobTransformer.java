// Created: 14.06.2019
package io.r2dbc.jdbc.converter.transformer;

import io.r2dbc.spi.Blob;

/**
 * @author Thomas Freese
 */
public class BlobTransformer extends AbstractObjectTransofrmer<Blob>
{
    /**
     * @see io.r2dbc.jdbc.converter.transformer.ObjectTransformer#transform(java.lang.Object)
     */
    @Override
    public Blob transform(final Object value)
    {
        if (value instanceof Blob)
        {
            return (Blob) value;
        }

        throw createCanNotConvertException(value);
    }
}
