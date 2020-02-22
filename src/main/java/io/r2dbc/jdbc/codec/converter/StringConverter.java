/**
 * Created: 19.06.2019
 */

package io.r2dbc.jdbc.codec.converter;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

            // @formatter:off
            ByteBuffer byteBuffer = Flux.from(blob.stream())
                .reduce(ByteBuffer::put)
                .concatWith(Mono.from(blob.discard())
                        .then(Mono.empty())
                        )
                .blockFirst();
            // @formatter:on

            byteBuffer.flip();

            byteBuffer = Base64.getEncoder().encode(byteBuffer);
            CharBuffer charBuffer = StandardCharsets.UTF_8.decode(byteBuffer);

            String string = charBuffer.toString();

            return string;
        }
        else if (value instanceof Clob)
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

        return value.toString();
    }
}
