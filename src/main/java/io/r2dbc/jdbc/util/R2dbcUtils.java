/**
 * Created: 23.02.2020
 */

package io.r2dbc.jdbc.util;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
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
    /**
     * @param blob {@link Blob}
     * @return {@link ByteBuffer}
     */
    public static final ByteBuffer blobToByteBuffer(final Blob blob)
    {
        // @formatter:off
        ByteBuffer byteBuffer = Flux.from(blob.stream())
            .reduce(ByteBuffer::put)
            .concatWith(Mono.from(blob.discard())
                    .then(Mono.empty())
                    )
            .blockFirst();
        // @formatter:on

        byteBuffer.limit(byteBuffer.capacity());
        // byteBuffer.flip();

        return byteBuffer;
    }

    /**
     * @param blob {@link Blob}
     * @return String
     */
    public static final String blobToString(final Blob blob)
    {
        ByteBuffer byteBuffer = blobToByteBuffer(blob);

        byteBuffer = Base64.getEncoder().encode(byteBuffer);
        CharBuffer charBuffer = StandardCharsets.UTF_8.decode(byteBuffer);

        String string = charBuffer.toString();

        return string;
    }

    /**
     * @param clob {@link Clob}
     * @return String
     */
    public static final String clobToString(final Clob clob)
    {
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

    /**
     * @param inputStream {@link InputStream}
     * @return {@link ByteBuffer}
     * @throws IOException Falls was schief geht.
     */
    public static final ByteBuffer inputStreamToByteBuffer(final InputStream inputStream) throws IOException
    {
        ByteBuffer byteBuffer = null;

        try (InputStream in = new BufferedInputStream(inputStream))
        {
            byte[] bytes = in.readAllBytes();

            byteBuffer = ByteBuffer.wrap(bytes);
        }

        byteBuffer.flip();

        return byteBuffer;
    }

    /**
     * @param reader {@link java.sql.Blob}
     * @return String
     * @throws IOException Falls was schief geht.
     */
    public static final String readerToString(final Reader reader) throws IOException
    {
        String string = null;

        try (BufferedReader buffer = new BufferedReader(reader))
        {
            string = buffer.lines().collect(Collectors.joining());
        }

        return string;
    }

    /**
     * @param sqlBlob {@link java.sql.Blob}
     * @return {@link Blob}
     * @throws SQLException Falls was schief geht.
     */
    public static final Blob sqlBlobToBlob(final java.sql.Blob sqlBlob) throws SQLException
    {
        ByteBuffer byteBuffer = null;

        try (InputStream inputStream = sqlBlob.getBinaryStream())
        {
            byteBuffer = inputStreamToByteBuffer(inputStream);
        }
        catch (IOException ex)
        {
            throw new SQLException(ex);
        }

        Blob blob = Blob.from((Mono.just(byteBuffer)));

        return blob;
    }

    /**
     * @param sqlClob {@link java.sql.Clob}
     * @return {@link Clob}
     * @throws SQLException Falls was schief geht.
     */
    public static final Clob sqlClobToClob(final java.sql.Clob sqlClob) throws SQLException
    {
        String string = null;

        try (Reader reader = sqlClob.getCharacterStream())
        {
            string = R2dbcUtils.readerToString(reader);
        }
        catch (IOException ex)
        {
            throw new SQLException(ex);
        }

        Clob clob = Clob.from((Mono.just(string)));

        return clob;
    }

    /**
     * Erstellt ein neues {@link R2dbcUtils} Object.
     */
    private R2dbcUtils()
    {
        super();
    }
}
