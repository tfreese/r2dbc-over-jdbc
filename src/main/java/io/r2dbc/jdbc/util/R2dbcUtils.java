// Created: 14.06.2019
package io.r2dbc.jdbc.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Base64;
import java.util.stream.Collectors;

import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Clob;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Thomas Freese
 */
public final class R2dbcUtils
{
    public static byte[] blobToByteArray(final Blob blob)
    {
        // ByteArrayOutputStream baos = new ByteArrayOutputStream();
        //
        // Flux.from(blob.stream()).subscribe(byteBuffer -> {
        // // baos.writeBytes(byteBuffer.array());
        // while (byteBuffer.hasRemaining())
        // {
        // baos.write(byteBuffer.get());
        // }
        // });
        //
        // blob.discard();
        //
        // byte[] bytes = baos.toByteArray();

        // @formatter:off
        return Flux.from(blob.stream())
                .reduce(new ByteArrayOutputStream(), (baos, byteBuffer) -> {
                    // baos.writeBytes(byteBuffer.array());
                    while (byteBuffer.hasRemaining())
                    {
                        baos.write(byteBuffer.get());
                    }

                    return baos;
                })
                .map(ByteArrayOutputStream::toByteArray)
                .concatWith(Mono.from(blob.discard())
                        .then(Mono.empty()))
                .blockFirst()
                ;
        // @formatter:on
    }

    public static ByteBuffer blobToByteBuffer(final Blob blob)
    {
        //        // @formatter:off
//        ByteBuffer byteBuffer = Flux.from(blob.stream())
//            .reduce(ByteBuffer::put)
//            .concatWith(Mono.from(blob.discard())
//                    .then(Mono.empty())
//                    )
//            .blockFirst();
//        // @formatter:on
        //
        // byteBuffer.limit(byteBuffer.capacity());
        // // byteBuffer.flip();
        //
        // return byteBuffer;

        byte[] bytes = blobToByteArray(blob);

        // ByteBuffer byteBuffer = ByteBuffer.allocateDirect(bytes.length);
        // byteBuffer.put(bytes);
        // byteBuffer.flip();

        return ByteBuffer.wrap(bytes);
    }

    public static InputStream blobToInputStream(final Blob blob)
    {
        byte[] bytes = blobToByteArray(blob);

        return new ByteArrayInputStream(bytes);
    }

    public static String blobToString(final Blob blob)
    {
        ByteBuffer byteBuffer = blobToByteBuffer(blob);

        byteBuffer = Base64.getEncoder().encode(byteBuffer);
        CharBuffer charBuffer = StandardCharsets.UTF_8.decode(byteBuffer);

        return charBuffer.toString();
    }

    public static Blob byteArrayToBlob(final byte[] bytes)
    {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

        return byteBufferToBlob(byteBuffer);
    }

    public static Blob byteBufferToBlob(final ByteBuffer byteBuffer)
    {
        return Blob.from((Mono.just(byteBuffer)));
    }

    public static byte[] byteBufferToByteArray(final ByteBuffer byteBuffer)
    {
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);

        return bytes;
    }

    public static InputStream byteBufferToInputStream(final ByteBuffer byteBuffer)
    {
        return new ByteArrayInputStream(byteBuffer.array());
    }

    public static String clobToString(final Clob clob)
    {
        // @formatter:off
        return Flux.from(clob.stream())
                .reduce(new StringBuilder(), StringBuilder::append)
                .map(StringBuilder::toString)
                .concatWith(Mono.from(clob.discard())
                        .then(Mono.empty()))
                .blockFirst()
                ;
        // @formatter:on
    }

    public static Blob inputStreamToBlob(final InputStream inputStream)
    {
        ByteBuffer byteBuffer = inputStreamToByteBuffer(inputStream);

        return byteBufferToBlob(byteBuffer);
    }

    public static ByteBuffer inputStreamToByteBuffer(final InputStream inputStream)
    {
        try
        {
            ByteBuffer byteBuffer = null;

            try (InputStream in = new BufferedInputStream(inputStream))
            {
                byte[] bytes = in.readAllBytes();

                byteBuffer = ByteBuffer.wrap(bytes);
            }

            return byteBuffer;
        }
        catch (IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }

    public static String readerToString(final Reader reader)
    {
        try
        {
            String string = null;

            try (BufferedReader bufferedReader = new BufferedReader(reader))
            {
                string = bufferedReader.lines().collect(Collectors.joining());
            }

            return string;
        }
        catch (IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }

    public static Blob sqlBlobToBlob(final java.sql.Blob sqlBlob)
    {
        ByteBuffer byteBuffer = null;

        try (InputStream inputStream = sqlBlob.getBinaryStream())
        {
            byteBuffer = inputStreamToByteBuffer(inputStream);
        }
        catch (IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
        catch (SQLException ex)
        {
            throw new RuntimeException(ex);
        }

        return byteBufferToBlob(byteBuffer);
    }

    public static Clob sqlClobToClob(final java.sql.Clob sqlClob)
    {
        String string = null;

        try (Reader reader = sqlClob.getCharacterStream())
        {
            string = R2dbcUtils.readerToString(reader);
        }
        catch (IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
        catch (SQLException ex)
        {
            throw new RuntimeException(ex);
        }

        return stringToClob(string);
    }

    public static Blob stringToBlob(final String value)
    {
        ByteBuffer byteBuffer = StandardCharsets.UTF_8.encode(value);

        return byteBufferToBlob(byteBuffer);
    }

    public static Clob stringToClob(final String value)
    {
        return Clob.from((Mono.just(value)));
    }

    private R2dbcUtils()
    {
        super();
    }
}
