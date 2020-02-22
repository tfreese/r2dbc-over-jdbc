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
import io.r2dbc.jdbc.codec.converter.ByteConverter;
import io.r2dbc.jdbc.codec.converter.Converter;
import io.r2dbc.jdbc.codec.converter.DateConverter;
import io.r2dbc.jdbc.codec.converter.DoubleConverter;
import io.r2dbc.jdbc.codec.converter.IntegerConverter;
import io.r2dbc.jdbc.codec.converter.LocalDateConverter;
import io.r2dbc.jdbc.codec.converter.LocalDateTimeConverter;
import io.r2dbc.jdbc.codec.converter.LocalTimeConverter;
import io.r2dbc.jdbc.codec.converter.LongConverter;
import io.r2dbc.jdbc.codec.converter.ObjectConverter;
import io.r2dbc.jdbc.codec.converter.StringConverter;
import io.r2dbc.jdbc.codec.decoder.BigIntDecoder;
import io.r2dbc.jdbc.codec.decoder.BinaryDecoder;
import io.r2dbc.jdbc.codec.decoder.BitDecoder;
import io.r2dbc.jdbc.codec.decoder.BlobDecoder;
import io.r2dbc.jdbc.codec.decoder.BooleanDecoder;
import io.r2dbc.jdbc.codec.decoder.CharDecoder;
import io.r2dbc.jdbc.codec.decoder.ClobDecoder;
import io.r2dbc.jdbc.codec.decoder.DateDecoder;
import io.r2dbc.jdbc.codec.decoder.DecimalDecoder;
import io.r2dbc.jdbc.codec.decoder.DoubleDecoder;
import io.r2dbc.jdbc.codec.decoder.FloatDecoder;
import io.r2dbc.jdbc.codec.decoder.IntegerDecoder;
import io.r2dbc.jdbc.codec.decoder.NumericDecoder;
import io.r2dbc.jdbc.codec.decoder.ObjectDecoder;
import io.r2dbc.jdbc.codec.decoder.SmallIntDecoder;
import io.r2dbc.jdbc.codec.decoder.SqlDecoder;
import io.r2dbc.jdbc.codec.decoder.TimeDecoder;
import io.r2dbc.jdbc.codec.decoder.TimestampDecoder;
import io.r2dbc.jdbc.codec.decoder.VarCharDecoder;
import io.r2dbc.jdbc.codec.encoder.BlobEncoder;
import io.r2dbc.jdbc.codec.encoder.BooleanEncoder;
import io.r2dbc.jdbc.codec.encoder.ClobEncoder;
import io.r2dbc.jdbc.codec.encoder.DateEncoder;
import io.r2dbc.jdbc.codec.encoder.DoubleEncoder;
import io.r2dbc.jdbc.codec.encoder.IntegerEncoder;
import io.r2dbc.jdbc.codec.encoder.LongEncoder;
import io.r2dbc.jdbc.codec.encoder.ObjectEncoder;
import io.r2dbc.jdbc.codec.encoder.SqlEncoder;
import io.r2dbc.jdbc.codec.encoder.StringEncoder;

/**
 * Encodes and decodes objects.
 *
 * @author Thomas Freese
 */
public final class Codecs
{
    /**
     * Fallback-Converter.
     */
    public static final Converter<?> FALLBACK_OBJECT_CONVERTER = ObjectConverter.INSTANCE;

    /**
     * Fallback-Decoder.
     */
    public static final SqlDecoder<?> FALLBACK_OBJECT_DECODER = ObjectDecoder.INSTANCE;

    /**
     * Fallback-Encoder.
     */
    public static final SqlEncoder<?> FALLBACK_OBJECT_ENCODER = ObjectEncoder.INSTANCE;

    /**
     *
     */
    private static final Codecs INSTANCE = new Codecs();

    /**
     *
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Codecs.class);

    /**
     * @param javaType {@link Class}
     * @param <T> Type
     * @return {@link Converter}
     */
    public static <T> Converter<T> getConverter(final Class<?> javaType)
    {
        return INSTANCE.findConverter(javaType);
    }

    /**
     * @param sqlType int
     * @param <T> Type
     * @return {@link SqlDecoder}
     */
    public static <T> SqlDecoder<T> getSqlDecoder(final int sqlType)
    {
        return INSTANCE.findSqlDecoder(sqlType);
    }

    /**
     * @param javaType {@link Class}
     * @param <T> Type
     * @return {@link SqlEncoder}
     */
    public static <T> SqlEncoder<T> getSqlEncoder(final Class<?> javaType)
    {
        return INSTANCE.findSqlEncoder(javaType);
    }

    /**
     * @param converter {@link Converter}
     */
    public static void registerConverter(final Converter<?> converter)
    {
        INSTANCE.register(converter);
    }

    /**
     * Register a new {@link SqlDecoder} for a {@link JDBCType}.
     *
     * @param decoder {@link SqlDecoder}
     */
    public static void registerDecoder(final SqlDecoder<?> decoder)
    {
        INSTANCE.register(decoder);
    }

    /**
     * @param encoder {@link SqlEncoder}
     */
    public static void registerEncoder(final SqlEncoder<?> encoder)
    {
        INSTANCE.register(encoder);
    }

    /**
    *
    */
    private final Map<Class<?>, Converter<?>> converterMap = new HashMap<>();

    /**
     *
     */
    private final Map<Integer, SqlDecoder<?>> decoderMap = new HashMap<>();

    /**
    *
    */
    private final Map<Class<?>, SqlEncoder<?>> encoderMap = new HashMap<>();

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

        // Default-Converter
        register(new ByteConverter());
        register(new DateConverter());
        register(new DoubleConverter());
        register(new IntegerConverter());
        register(new LocalDateConverter());
        register(new LocalDateTimeConverter());
        register(new LocalTimeConverter());
        register(new LongConverter());
        register(new StringConverter());
    }

    /**
     * @param <T> Type
     * @param javaType {@link Class}
     * @return {@link Converter}
     */
    @SuppressWarnings("unchecked")
    private <T> Converter<T> findConverter(final Class<?> javaType)
    {
        Converter<?> converter = this.converterMap.get(javaType);

        if (converter == null)
        {
            // May be an anonymous implementation.
            Optional<Converter<?>> optional = this.converterMap.values().stream().filter(e -> e.getJavaType().isAssignableFrom(javaType)).findFirst();

            if (optional.isPresent())
            {
                converter = optional.get();
                this.converterMap.put(javaType, converter);
            }
        }

        if (converter == null)
        {
            LOGGER.warn("Class '{}' not mapped, using {} as default", javaType, FALLBACK_OBJECT_CONVERTER.getClass().getSimpleName());

            converter = FALLBACK_OBJECT_CONVERTER;
            this.converterMap.put(javaType, converter);
        }

        return (Converter<T>) converter;
    }

    /**
     * @param <T> Type
     * @param sqlType int
     * @return {@link SqlDecoder}
     */
    @SuppressWarnings("unchecked")
    private <T> SqlDecoder<T> findSqlDecoder(final int sqlType)
    {
        SqlDecoder<?> decoder = this.decoderMap.get(sqlType);

        if (decoder == null)
        {
            if (sqlType != FALLBACK_OBJECT_DECODER.getSqlType())
            {
                LOGGER.warn("sqlType '{}/{}' not mapped, using {} as default", sqlType, JDBCType.valueOf(sqlType).getName(),
                        FALLBACK_OBJECT_DECODER.getClass().getSimpleName());
            }

            decoder = FALLBACK_OBJECT_DECODER;
            this.decoderMap.put(sqlType, decoder);
        }

        return (SqlDecoder<T>) decoder;
    }

    /**
     * @param <T> Type
     * @param javaType {@link Class}
     * @return {@link SqlEncoder}
     */
    @SuppressWarnings("unchecked")
    private <T> SqlEncoder<T> findSqlEncoder(final Class<?> javaType)
    {
        SqlEncoder<?> encoder = this.encoderMap.get(javaType);

        if (encoder == null)
        {
            // May be an anonymous implementation.
            Optional<SqlEncoder<?>> optional = this.encoderMap.values().stream().filter(e -> e.getJavaType().isAssignableFrom(javaType)).findFirst();

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
            this.encoderMap.put(javaType, encoder);
        }

        return (SqlEncoder<T>) encoder;
    }

    /**
     * Register a new {@link Converter} for a Java-Class.
     *
     * @param converter {@link Converter}
     */
    public void register(final Converter<?> converter)
    {
        this.converterMap.put(converter.getJavaType(), converter);
    }

    /**
     * Register a new {@link SqlDecoder} for a {@link JDBCType}.
     *
     * @param sqlDecoder {@link SqlDecoder}
     */
    public void register(final SqlDecoder<?> sqlDecoder)
    {
        this.decoderMap.put(sqlDecoder.getSqlType(), sqlDecoder);
    }

    /**
     * Register a new {@link SqlEncoder} for a Java-Class.
     *
     * @param sqlEncoder {@link SqlEncoder}
     */
    public void register(final SqlEncoder<?> sqlEncoder)
    {
        this.encoderMap.put(sqlEncoder.getJavaType(), sqlEncoder);
    }
}
