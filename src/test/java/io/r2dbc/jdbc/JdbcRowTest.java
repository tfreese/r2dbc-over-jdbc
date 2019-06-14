package io.r2dbc.jdbc;

import java.util.Collections;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.core.JdbcOperations;
import io.r2dbc.jdbc.util.HsqldbServerExtension;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.test.Example;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * @author Thomas Freese
 */
final class JdbcRowTest
{
    /**
     *
     */
    @RegisterExtension
    static final HsqldbServerExtension SERVER = new HsqldbServerExtension();

    /**
     *
     */
    private final ConnectionFactory connectionFactory =
            ConnectionFactories.get(ConnectionFactoryOptions.builder().option(JdbcConnectionFactoryProvider.DATASOURCE, SERVER.getDataSource()).build());

    /**
     *
     */
    @BeforeEach
    void createTable()
    {
        getJdbcOperations().execute("CREATE TABLE test ( value INTEGER )");
    }

    /**
     *
     */
    @AfterEach
    void dropTable()
    {
        getJdbcOperations().execute("DROP TABLE test");
    }

    /**
     * @return {@link JdbcOperations}
     */
    private JdbcOperations getJdbcOperations()
    {
        JdbcOperations jdbcOperations = SERVER.getJdbcOperations();

        if (jdbcOperations == null)
        {
            throw new IllegalStateException("JdbcOperations not yet initialized");
        }

        return jdbcOperations;
    }

    /**
    *
    */
    @Test
    void selectWithAliases()
    {
        getJdbcOperations().execute("INSERT INTO test VALUES (100)");

        // @formatter:off
        Mono.from(this.connectionFactory.create())
            .flatMapMany(connection -> Flux.from(connection .createStatement("SELECT value as ALIASED_VALUE FROM test").execute())
                .flatMap(result -> Flux.from(result.map((row, rowMetadata) -> {
                            Integer value = row.get("ALIASED_VALUE", Integer.class);
                            return value;
                            }
                            )
                        )
                        .collectList())
                .concatWith(Example.close(connection)))
            .as(StepVerifier::create)
            .expectNext(Collections.singletonList(100))
            .verifyComplete();
        // @formatter:on
    }

    /**
    *
    */
    @Test
    void selectWithoutAliases()
    {
        getJdbcOperations().execute("INSERT INTO test VALUES (100)");

        // @formatter:off
        Mono.from(this.connectionFactory.create())
            .flatMapMany(connection -> Flux.from(connection.createStatement("SELECT value FROM test").execute())
                .flatMap(Example::extractColumns)
                .concatWith(Example.close(connection)))
        .as(StepVerifier::create)
        .expectNext(Collections.singletonList(100))
        .verifyComplete();
        // @formatter:on
    }

    // private final List<Column> columns = Arrays.asList(
    // new Column(TEST.buffer(4).writeInt(100), 200, BINARY, "test-name-1"),
    // new Column(TEST.buffer(4).writeInt(300), 400, TEXT, "test-name-2"),
    // new Column(null, 400, TEXT, "test-name-3")
    // );
    //
    // @Test
    // void constructorNoCodecs() {
    // assertThatIllegalArgumentException().isThrownBy(() -> new H2Row(null, Collections.emptyList()))
    // .withMessage("codecs must not be null");
    // }
    //
    // @Test
    // void constructorNoColumns() {
    // assertThatIllegalArgumentException().isThrownBy(() -> new H2Row(MockCodecs.empty(), null))
    // .withMessage("columns must not be null");
    // }
    //
    // @Test
    // void getDefaultType() {
    // Object value = new Object();
    //
    // MockCodecs codecs = MockCodecs.builder()
    // .decoding(TEST.buffer(4).writeInt(300), 400, TEXT, Object.class, value)
    // .build();
    //
    // assertThat(new H2Row(codecs, this.columns).get("test-name-2")).isSameAs(value);
    // }
    //
    // @Test
    // void getAfterRelease() {
    // Object value = new Object();
    //
    // MockCodecs codecs = MockCodecs.builder()
    // .decoding(TEST.buffer(4).writeInt(300), 400, TEXT, Object.class, value)
    // .build();
    //
    // H2Row row = new H2Row(codecs, this.columns);
    // row.release();
    //
    // assertThatIllegalStateException().isThrownBy(() -> row.get("test-name-2", Object.class))
    // .withMessage("Value cannot be retrieved after row has been released");
    // }
    //
    // @Test
    // void getIndex() {
    // Object value = new Object();
    //
    // MockCodecs codecs = MockCodecs.builder()
    // .decoding(TEST.buffer(4).writeInt(300), 400, TEXT, Object.class, value)
    // .build();
    //
    // assertThat(new H2Row(codecs, this.columns).get(1, Object.class)).isSameAs(value);
    // }
    //
    // @Test
    // void getInvalidIndex() {
    // assertThatIllegalArgumentException().isThrownBy(() -> new H2Row(MockCodecs.empty(), this.columns).get(3, Object.class))
    // .withMessage("Column index 3 is larger than the number of columns 3");
    // }
    //
    // @Test
    // void getInvalidName() {
    // assertThatIllegalArgumentException().isThrownBy(() -> new H2Row(MockCodecs.empty(), this.columns).get("test-name-4", Object.class))
    // .withMessageMatching("Column name 'test-name-4' does not exist in column names \\[test-name-[\\d], test-name-[\\d], test-name-[\\d]\\]");
    // }
    //
    // @Test
    // void getName() {
    // Object value = new Object();
    //
    // MockCodecs codecs = MockCodecs.builder()
    // .decoding(TEST.buffer(4).writeInt(300), 400, TEXT, Object.class, value)
    // .build();
    //
    // assertThat(new H2Row(codecs, this.columns).get("test-name-2", Object.class)).isSameAs(value);
    // }
    //
    // @Test
    // void getNoIdentifier() {
    // assertThatIllegalArgumentException().isThrownBy(() -> new H2Row(MockCodecs.empty(), this.columns).get(null, Object.class))
    // .withMessage("identifier must not be null");
    // }
    //
    // @Test
    // void getNoType() {
    // assertThatIllegalArgumentException().isThrownBy(() -> new H2Row(MockCodecs.empty(), this.columns).get(new Object(), null))
    // .withMessage("type must not be null");
    // }
    //
    // @Test
    // void getNull() {
    // MockCodecs codecs = MockCodecs.builder()
    // .decoding(null, 400, TEXT, Object.class, null)
    // .build();
    //
    // assertThat(new H2Row(codecs, this.columns).get("test-name-3", Object.class)).isNull();
    // }
    //
    // @Test
    // void getWrongIdentifierType() {
    // Object identifier = new Object();
    //
    // assertThatIllegalArgumentException().isThrownBy(() -> new H2Row(MockCodecs.empty(), this.columns).get(identifier, Object.class))
    // .withMessage("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String column name.", identifier);
    // }
    //
    // @Test
    // void toRow() {
    // Object value = new Object();
    //
    // MockCodecs codecs = MockCodecs.builder()
    // .decoding(TEST.buffer(4).writeInt(100), 300, TEXT, Object.class, value)
    // .build();
    //
    // H2Row row = H2Row.toRow(codecs, new DataRow(Collections.singletonList(TEST.buffer(4).writeInt(100))),
    // new RowDescription(Collections.singletonList(new RowDescription.Field((short) 200, 300, (short) 400, (short) 500, TEXT, "test-name-1", 600))));
    //
    // assertThat(row.get(0, Object.class)).isSameAs(value);
    // }
    //
    // @Test
    // void toRowNoCodecs() {
    // assertThatIllegalArgumentException().isThrownBy(() -> H2Row.toRow(null, new DataRow(Collections.singletonList(TEST.buffer(4).writeInt(100))),
    // new RowDescription(Collections.emptyList())))
    // .withMessage("codecs must not be null");
    // }
    //
    // @Test
    // void toRowNoDataRow() {
    // assertThatIllegalArgumentException().isThrownBy(() -> H2Row.toRow(MockCodecs.empty(), null, new RowDescription(Collections.emptyList())))
    // .withMessage("dataRow must not be null");
    // }
    //
    // @Test
    // void toRowNoRowDescription() {
    // assertThatIllegalArgumentException().isThrownBy(() -> H2Row.toRow(MockCodecs.empty(), new
    // DataRow(Collections.singletonList(TEST.buffer(4).writeInt(100))), null))
    // .withMessage("rowDescription must not be null");
    // }
}
