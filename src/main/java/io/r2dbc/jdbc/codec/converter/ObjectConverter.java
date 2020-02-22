/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.codec.converter;

import java.nio.ByteBuffer;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

            // @formatter:off
            String string = Flux.from(clob.stream())
                    .reduce(new StringBuilder(), StringBuilder::append)
                    .map(StringBuilder::toString)
                    .concatWith(Mono.from(clob.discard())
                            .then(Mono.empty()))
                    .blockFirst();
            // @formatter:on

            return string;
        }

        if (value instanceof Blob)
        {
            Blob blob = (Blob) value;

            // @formatter:off
            ByteBuffer byteBuffer = Flux.from(blob.stream())
                .reduce(ByteBuffer::put)
                .concatWith(Mono.from(blob.discard())
                        .then(Mono.empty())
                        )
                .blockFirst();
            // @formatter:on

            byteBuffer.limit(byteBuffer.capacity());
            // byteBuffer.compact();
            // byteBuffer.flip();

            return byteBuffer;
        }

        return value;
    }
}
