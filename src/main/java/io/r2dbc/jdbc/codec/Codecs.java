/**
 * Created: 16.06.2019
 */

package io.r2dbc.jdbc.codec;

import java.sql.JDBCType;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.r2dbc.jdbc.codec.decoder.BigIntDecoder;
import io.r2dbc.jdbc.codec.decoder.BinaryDecoder;
import io.r2dbc.jdbc.codec.decoder.BitDecoder;
import io.r2dbc.jdbc.codec.decoder.BlobDecoder;
import io.r2dbc.jdbc.codec.decoder.BooleanDecoder;
import io.r2dbc.jdbc.codec.decoder.CharDecoder;
import io.r2dbc.jdbc.codec.decoder.ClobDecoder;
import io.r2dbc.jdbc.codec.decoder.DateDecoder;
import io.r2dbc.jdbc.codec.decoder.DecimalDecoder;
import io.r2dbc.jdbc.codec.decoder.Decoder;
import io.r2dbc.jdbc.codec.decoder.DoubleDecoder;
import io.r2dbc.jdbc.codec.decoder.FloatDecoder;
import io.r2dbc.jdbc.codec.decoder.IntegerDecoder;
import io.r2dbc.jdbc.codec.decoder.NumericDecoder;
import io.r2dbc.jdbc.codec.decoder.ObjectDecoder;
import io.r2dbc.jdbc.codec.decoder.SmallIntDecoder;
import io.r2dbc.jdbc.codec.decoder.TimeDecoder;
import io.r2dbc.jdbc.codec.decoder.TimestampDecoder;
import io.r2dbc.jdbc.codec.decoder.VarCharDecoder;
import io.r2dbc.jdbc.codec.encoder.BlobEncoder;
import io.r2dbc.jdbc.codec.encoder.BooleanEncoder;
import io.r2dbc.jdbc.codec.encoder.ClobEncoder;
import io.r2dbc.jdbc.codec.encoder.DateEncoder;
import io.r2dbc.jdbc.codec.encoder.DoubleEncoder;
import io.r2dbc.jdbc.codec.encoder.Encoder;
import io.r2dbc.jdbc.codec.encoder.IntegerEncoder;
import io.r2dbc.jdbc.codec.encoder.LongEncoder;
import io.r2dbc.jdbc.codec.encoder.ObjectEncoder;
import io.r2dbc.jdbc.codec.encoder.StringEncoder;

/**
 * Encodes and decodes objects.
 *
 * @author Thomas Freese
 */
public final class Codecs
{
    /**
     *
     */
    public static final Decoder<?> FALLBACK_OBJECT_DECODER = ObjectDecoder.INSTANCE;

    /**
    *
    */
    public static final Encoder<?> FALLBACK_OBJECT_ENCODER = ObjectEncoder.INSTANCE;

    /**
     *
     */
    private static final Codecs INSTANCE = new Codecs();

    /**
     *
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Codecs.class);

    /**
     * @param sqlType int
     * @param <T> Type
     * @return {@link Decoder}
     */
    public static <T> Decoder<T> getDecoder(final int sqlType)
    {
        return INSTANCE.get(sqlType);
    }

    /**
     * @param javaType {@link Class}
     * @param <T> Type
     * @return {@link Encoder}
     */
    public static <T> Encoder<T> getEncoder(final Class<?> javaType)
    {
        return INSTANCE.get(javaType);
    }

    /**
     * Register a new {@link Decoder} for a {@link JDBCType}.
     *
     * @param decoder {@link Decoder}
     */
    public static void registerDecoder(final Decoder<?> decoder)
    {
        INSTANCE.register(decoder);
    }

    /**
     * @param encoder {@link Encoder}
     */
    public static void registerEncoder(final Encoder<?> encoder)
    {
        INSTANCE.register(encoder);
    }

    /**
     *
     */
    private final Map<Integer, Decoder<?>> decoderMap = new HashMap<>();

    /**
    *
    */
    private final Map<Class<?>, Encoder<?>> encoderMap = new HashMap<>();

    /**
     * Erstellt ein neues {@link Codecs} Object.
     */
    private Codecs()
    {
        super();

        // Default-Decoder
        register(new BigIntDecoder());
        register(new BitDecoder());
        register(new BooleanDecoder());
        register(new BinaryDecoder());
        register(new BlobDecoder());
        register(new CharDecoder());
        register(new ClobDecoder());
        register(new DateDecoder());
        register(new DecimalDecoder());
        register(new DoubleDecoder());
        register(new FloatDecoder());
        register(new IntegerDecoder());
        register(new NumericDecoder());
        register(new SmallIntDecoder());
        register(new TimeDecoder());
        register(new TimestampDecoder());
        register(new VarCharDecoder());

        // Default-Encoder
        register(new BlobEncoder());
        register(new BooleanEncoder());
        register(new ClobEncoder());
        register(new DateEncoder());
        register(new DoubleEncoder());
        register(new IntegerEncoder());
        register(new LongEncoder());
        register(new StringEncoder());
    }

    /**
     * @param <T> Type
     * @param javaType {@link Class}
     * @return {@link Encoder}
     */
    @SuppressWarnings("unchecked")
    public <T> Encoder<T> get(final Class<?> javaType)
    {
        Encoder<?> encoder = this.encoderMap.get(javaType);

        if (encoder == null)
        {
            // May be an anonymous implementation.
            Optional<Encoder<?>> optional = this.encoderMap.values().stream().filter(e -> e.getJavaType().isAssignableFrom(javaType)).findFirst();

            if (optional.isPresent())
            {
                encoder = optional.get();
                this.encoderMap.put(javaType, encoder);
            }
        }

        if (encoder == null)
        {
            LOGGER.warn("Class '{}' not mapped, using {} as default", javaType, FALLBACK_OBJECT_ENCODER.getClass().getSimpleName());

            encoder = FALLBACK_OBJECT_ENCODER;
        }

        return (Encoder<T>) encoder;
    }

    /**
     * @param <T> Type
     * @param sqlType int
     * @return {@link Decoder}
     */
    @SuppressWarnings("unchecked")
    public <T> Decoder<T> get(final int sqlType)
    {
        Decoder<?> codec = this.decoderMap.get(sqlType);

        if (codec == null)
        {
            if (sqlType != FALLBACK_OBJECT_DECODER.getSqlType())
            {
                LOGGER.warn("sqlType '{}/{}' not mapped, using {} as default", sqlType, JDBCType.valueOf(sqlType).getName(),
                        FALLBACK_OBJECT_DECODER.getClass().getSimpleName());
            }

            codec = FALLBACK_OBJECT_DECODER;
        }

        return (Decoder<T>) codec;
    }

    /**
     * Register a new {@link Decoder} for a {@link JDBCType}.
     *
     * @param decoder {@link Decoder}
     */
    public void register(final Decoder<?> decoder)
    {
        this.decoderMap.put(decoder.getSqlType(), decoder);
    }

    /**
     * Register a new {@link Encoder} for a Java-Class.
     *
     * @param encoder {@link Encoder}
     */
    public void register(final Encoder<?> encoder)
    {
        this.encoderMap.put(encoder.getJavaType(), encoder);
    }
}
