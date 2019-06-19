/**
 * Created: 14.06.2019
 */

package io.r2dbc.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.r2dbc.jdbc.codec.Codecs;
import io.r2dbc.jdbc.codec.decoder.Decoder;
import io.r2dbc.jdbc.codec.encoder.Encoder;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * R2DBC Adapter for JDBC.
 *
 * @author Thomas Freese
 */
public abstract class AbstractJdbcStatement implements Statement
{
    /**
     * @author Thomas Freese
     */
    static class Bindings
    {
        /**
         *
         */
        private final List<Map<Integer, Object>> bindings = new ArrayList<>();

        /**
         *
         */
        private Map<Integer, Object> current;

        /**
         * Erstellt ein neues {@link Bindings} Object.
         */
        public Bindings()
        {
            super();
        }

        /**
         *
         */
        void finish()
        {
            this.current = null;
        }

        /**
         * @return {@link Map}
         */
        Map<Integer, Object> getCurrent()
        {
            if (this.current == null)
            {
                this.current = new HashMap<>();
                this.bindings.add(this.current);
            }

            return this.current;
        }

        /**
         * @param preparedStatement {@link PreparedStatement}
         * @throws SQLException Falls was schief geht.
         */
        void prepareBatch(final PreparedStatement preparedStatement) throws SQLException
        {
            if (this.bindings.isEmpty())
            {
                preparedStatement.addBatch();
                return;
            }

            for (Map<Integer, Object> binding : this.bindings)
            {
                if (binding.isEmpty())
                {
                    continue;
                }

                prepareStatement(preparedStatement, binding);

                preparedStatement.addBatch();
            }
        }

        /**
         * @param preparedStatement {@link PreparedStatement}
         * @param binding {@link Map}
         * @throws SQLException Falls was schief geht.
         */
        void prepareStatement(final PreparedStatement preparedStatement, final Map<Integer, Object> binding) throws SQLException
        {
            for (Integer index : binding.keySet())
            {
                // JDBC fängt bei 1 an !
                int parameterIndex = index + 1;

                Object value = binding.get(index);

                if (value == null)
                {
                    preparedStatement.setObject(parameterIndex, null);
                }
                else
                {
                    Encoder<Object> encoder = Codecs.getEncoder(value.getClass());

                    encoder.encode(preparedStatement, parameterIndex, value);
                }
            }
        }
    }

    /**
     *
     */
    private final Bindings bindings = new Bindings();

    /**
     *
     */
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     *
     */
    private final java.sql.PreparedStatement preparedStatement;

    /**
     * Erstellt ein neues {@link AbstractJdbcStatement} Object.
     *
     * @param preparedStatement {@link java.sql.PreparedStatement}
     */
    public AbstractJdbcStatement(final java.sql.PreparedStatement preparedStatement)
    {
        super();

        this.preparedStatement = Objects.requireNonNull(preparedStatement, "statement must not be null");
    }

    /**
     * @see io.r2dbc.spi.Statement#add()
     */
    @Override
    public Statement add()
    {
        // try
        // {
        // getStatement().addBatch();
        // }
        // catch (SQLException sex)
        // {
        // throw JdbcR2dbcExceptionFactory.create(sex);
        // }

        getBindings().finish();

        return this;
    }

    /**
     * @see io.r2dbc.spi.Statement#bind(int, java.lang.Object)
     */
    @Override
    public Statement bind(final int index, final Object value)
    {
        getBindings().getCurrent().put(index, value);

        return this;
    }

    /**
     * @see io.r2dbc.spi.Statement#bind(java.lang.Object, java.lang.Object)
     */
    @Override
    public Statement bind(final Object identifier, final Object value)
    {
        if (identifier instanceof Integer)
        {
            return bind(((Integer) identifier).intValue(), value);
        }
        else if (identifier instanceof String)
        {
            return bind(Integer.parseInt((String) identifier), value);
        }

        throw new IllegalArgumentException(
                String.format("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String represented integer.", identifier));
    }

    /**
     * @see io.r2dbc.spi.Statement#bindNull(int, java.lang.Class)
     */
    @Override
    public Statement bindNull(final int index, final Class<?> type)
    {
        getBindings().getCurrent().put(index, null);

        return this;
    }

    /**
     * @see io.r2dbc.spi.Statement#bindNull(java.lang.Object, java.lang.Class)
     */
    @Override
    public Statement bindNull(final Object identifier, final Class<?> type)
    {
        if (identifier instanceof Integer)
        {
            return bindNull(((Integer) identifier).intValue(), type);
        }

        throw new UnsupportedOperationException();
    }

    /**
     * @return {@link Mono}
     */
    @SuppressWarnings("resource")
    protected Mono<Tuple2<ResultSet, int[]>> createExecuteMono()
    {
        return Mono.fromCallable(() -> {
            getLogger().debug("execute statement");

            getBindings().prepareBatch(getStatement());

            int[] affectedRows = getStatement().executeBatch();
            affectedRows = normalizeAffectedRowsForReactive(affectedRows);

            ResultSet resultSetGenKeys = getStatement().getGeneratedKeys();

            return Tuples.of(resultSetGenKeys, affectedRows);
        });
    }

    /**
     * @param resultSet {@link ResultSet}
     * @param affectedRows int[]
     * @return {@link JdbcResult}
     * @throws SQLException Falls was schief geht.
     */
    protected JdbcResult createResult(final ResultSet resultSet, final int[] affectedRows) throws SQLException
    {
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(resultSet);
        List<JdbcColumnMetadata> columnMetaDatas = rowMetadata.getColumnMetadatas();

        Flux<JdbcRow> rows = Flux.generate((final SynchronousSink<Map<Object, Object>> sink) -> {
            try
            {
                if (resultSet.next())
                {
                    Map<Object, Object> row = new HashMap<>();

                    int index = 0;

                    for (JdbcColumnMetadata columnMetaData : columnMetaDatas)
                    {
                        String columnLabel = columnMetaData.getName();
                        int sqlType = (int) columnMetaData.getNativeTypeMetadata();

                        Decoder<?> decoder = Codecs.getDecoder(sqlType);
                        Object value = decoder.decode(resultSet, columnLabel);

                        row.put(columnLabel, value);
                        row.put(index, value);
                        index++;
                    }

                    sink.next(row);
                }
                else
                {
                    getLogger().debug("close resultSet");
                    resultSet.close();

                    getLogger().debug("close statement");
                    getStatement().close();

                    sink.complete();
                }
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).map(JdbcRow::new).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create);

        JdbcResult result = new JdbcResult(rows, Mono.just(rowMetadata), Mono.just(affectedRows.length));

        return result;
    }

    /**
     * @param resultSet {@link ResultSet}
     * @param affectedRows int[]
     * @return {@link JdbcResult}
     * @throws SQLException Falls was schief geht.
     */
    protected JdbcResult createResultAffectedRows(final ResultSet resultSet, final int[] affectedRows) throws SQLException
    {
        JdbcRowMetadata rowMetadata = new JdbcRowMetadata(resultSet);

        resultSet.close();

        AtomicInteger counter = new AtomicInteger(0);

        Flux<JdbcRow> rows = Flux.generate((final SynchronousSink<Map<Object, Object>> sink) -> {
            if (counter.get() < affectedRows.length)
            {
                Map<Object, Object> row = new HashMap<>();
                row.put(0, affectedRows[counter.get()]);

                counter.incrementAndGet();

                sink.next(row);
            }
            else
            {
                sink.complete();
            }
        }).map(JdbcRow::new);

        JdbcResult result = new JdbcResult(rows, Mono.just(rowMetadata), Mono.just(affectedRows.length));

        return result;
    }

    /**
     * @see io.r2dbc.spi.Statement#execute()
     */
    @SuppressWarnings("resource")
    @Override
    public Mono<JdbcResult> execute()
    {
        return createExecuteMono().handle((tuple, sink) -> {
            try
            {
                ResultSet resultSet = tuple.getT1();
                int[] affectedRows = tuple.getT2();

                JdbcResult result = createResult(resultSet, affectedRows);

                sink.next(result);

                sink.complete();
            }
            catch (SQLException sex)
            {
                sink.error(sex);
            }
        }).onErrorMap(SQLException.class, JdbcR2dbcExceptionFactory::create).cast(JdbcResult.class);
    }

    /**
     * @return {@link Bindings}
     */
    protected Bindings getBindings()
    {
        return this.bindings;
    }

    /**
     * @return {@link Logger}
     */
    protected Logger getLogger()
    {
        return this.logger;
    }

    /**
     * @return {@link java.sql.PreparedStatement}
     */
    protected java.sql.PreparedStatement getStatement()
    {
        return this.preparedStatement;
    }

    /**
     * Beim DELETE hat das Array nur ein Element pro Batch mit der Anzahl gelöschter Zeilen.<br>
     * Dies muss für das reaktive Verhalten auf n-Elemente erweitert werden.<br>
     * [2, 1] -> [1, 1, 1]
     *
     * @param affectedRows int[]
     * @return int[]
     * @see JdbcPreparedStatementDelete
     */
    protected int[] normalizeAffectedRowsForReactive(final int[] affectedRows)
    {
        // return affectedRows;
        // int[] rows = IntStream.of(affectedRows).flatMap(ar -> {
        // int[] r = new int[ar];
        // Arrays.fill(r, 1);
        // return IntStream.of(r);
        // }).toArray();

        int[] rows = new int[IntStream.of(affectedRows).sum()];
        Arrays.fill(rows, 1);

        return rows;
    }

    /**
     * @see io.r2dbc.jdbc.AbstractJdbcStatement#returnGeneratedValues(java.lang.String[])
     */
    @Override
    public Statement returnGeneratedValues(final String...columns)
    {
        throw new UnsupportedOperationException();
    }
}
