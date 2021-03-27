// Created: 14.06.2019
package io.r2dbc.jdbc.converter.transformer;

import io.r2dbc.spi.Clob;
import reactor.core.publisher.Mono;

/**
 * @author Thomas Freese
 */
public class ClobTransformer extends AbstractObjectTransformer<Clob>
{
    /**
     * @see io.r2dbc.jdbc.converter.transformer.ObjectTransformer#transform(java.lang.Object)
     */
    @Override
    public Clob transform(final Object value)
    {
        if (value instanceof Clob)
        {
            return (Clob) value;
        }

        if (value instanceof CharSequence)
        {
            return Clob.from((Mono.just((CharSequence) value)));
        }

        throw createCanNotConvertException(value);
    }
}
