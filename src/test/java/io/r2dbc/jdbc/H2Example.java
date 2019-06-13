///*
// * Copyright 2018 the original author or authors. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// * with the License. You may obtain a copy of the License at https://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
// * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and limitations under the License.
// */
//
//package io.r2dbc.jdbc;
//
//import io.r2dbc.jdbc.util.HsqldbServerExtension;
//import io.r2dbc.spi.Connection;
//import io.r2dbc.spi.ConnectionFactories;
//import io.r2dbc.spi.ConnectionFactory;
//import io.r2dbc.spi.ConnectionFactoryOptions;
//import io.r2dbc.spi.test.Example;
//import org.junit.jupiter.api.Disabled;
//import org.junit.jupiter.api.Nested;
//import org.junit.jupiter.api.extension.RegisterExtension;
//import org.springframework.jdbc.core.JdbcOperations;
//import reactor.core.publisher.Mono;
//import static io.r2dbc.jdbc.JdbcConnectionFactoryProvider.H2_DRIVER;
//import static io.r2dbc.jdbc.JdbcConnectionFactoryProvider.URL;
//import static io.r2dbc.spi.ConnectionFactoryOptions.DRIVER;
//import static io.r2dbc.spi.ConnectionFactoryOptions.PASSWORD;
//import static io.r2dbc.spi.ConnectionFactoryOptions.USER;
//
//final class H2Example
//{
//
//    @Nested
//    @Disabled("TODO: Fix H2Statement so it properly handles plain JDBC placeholders.")
//    final class JdbcStyle implements Example<Integer>
//    {
//
//        @Override
//        public ConnectionFactory getConnectionFactory()
//        {
//            return H2Example.this.connectionFactory;
//        }
//
//        @Override
//        public Integer getIdentifier(final int index)
//        {
//            return index;
//        }
//
//        @Override
//        public JdbcOperations getJdbcOperations()
//        {
//            JdbcOperations jdbcOperations = SERVER.getJdbcOperations();
//
//            if (jdbcOperations == null)
//            {
//                throw new IllegalStateException("JdbcOperations not yet initialized");
//            }
//
//            return jdbcOperations;
//        }
//
//        @Override
//        public String getPlaceholder(final int index)
//        {
//            return "?";
//        }
//    }
//
//    @Nested
//    final class NamedParameterStyle implements Example<String>
//    {
//
//        @Override
//        public ConnectionFactory getConnectionFactory()
//        {
//            return H2Example.this.connectionFactory;
//        }
//
//        // @Test
//        // @Override
//        // public void columnMetadata()
//        // {
//        // getJdbcOperations().execute("INSERT INTO test_two_column VALUES (100, 'hello')");
//        //
//        // Mono.from(getConnectionFactory().create()).flatMapMany(connection -> Flux.from(connection
//        //
//        // .createStatement("SELECT col1 AS value, col2 AS value FROM test_two_column").execute()).flatMap(result -> {
//        // return result.map((row, rowMetadata) -> {
//        // Collection<String> columnNames = rowMetadata.getColumnNames();
//        // return Arrays.asList(rowMetadata.getColumnMetadata("value").getName(), rowMetadata.getColumnMetadata("VALUE").getName(),
//        // columnNames.contains("value"), columnNames.contains("VALUE"));
//        // });
//        // }).flatMapIterable(Function.identity()).concatWith(close(connection))).as(StepVerifier::create).expectNext("VALUE").as("Column label col1")
//        // .expectNext("VALUE").as("Column label col1 (get by uppercase)").expectNext(true).as("getColumnNames.contains(value)").expectNext(true)
//        // .as("getColumnNames.contains(VALUE)").verifyComplete();
//        // }
//
//        @Override
//        public String getIdentifier(final int index)
//        {
//            return getPlaceholder(index);
//        }
//
//        @Override
//        public JdbcOperations getJdbcOperations()
//        {
//            JdbcOperations jdbcOperations = SERVER.getJdbcOperations();
//
//            if (jdbcOperations == null)
//            {
//                throw new IllegalStateException("JdbcOperations not yet initialized.");
//            }
//
//            return jdbcOperations;
//        }
//
//        @Override
//        public String getPlaceholder(final int index)
//        {
//            return String.format("?%d", index + 1);
//        }
//
//        <T> Mono<T> close(final Connection connection)
//        {
//            return Mono.from(connection.close()).then(Mono.empty());
//        }
//    }
//
//    @Nested
//    final class PostgresqlStyle implements Example<String>
//    {
//
//        @Override
//        public ConnectionFactory getConnectionFactory()
//        {
//            return H2Example.this.connectionFactory;
//        }
//
//        // @Test
//        // @Override
//        // public void columnMetadata()
//        // {
//        // getJdbcOperations().execute("INSERT INTO test_two_column VALUES (100, 'hello')");
//        //
//        // Mono.from(getConnectionFactory().create()).flatMapMany(connection -> Flux.from(connection
//        //
//        // .createStatement("SELECT col1 AS value, col2 AS value FROM test_two_column").execute()).flatMap(result -> {
//        // return result.map((row, rowMetadata) -> {
//        // Collection<String> columnNames = rowMetadata.getColumnNames();
//        // return Arrays.asList(rowMetadata.getColumnMetadata("value").getName(), rowMetadata.getColumnMetadata("VALUE").getName(),
//        // columnNames.contains("value"), columnNames.contains("VALUE"));
//        // });
//        // }).flatMapIterable(Function.identity()).concatWith(close(connection))).as(StepVerifier::create).expectNext("VALUE").as("Column label col1")
//        // .expectNext("VALUE").as("Column label col1 (get by uppercase)").expectNext(true).as("getColumnNames.contains(value)").expectNext(true)
//        // .as("getColumnNames.contains(VALUE)").verifyComplete();
//        // }
//
//        @Override
//        public String getIdentifier(final int index)
//        {
//            return getPlaceholder(index);
//        }
//
//        @Override
//        public JdbcOperations getJdbcOperations()
//        {
//            JdbcOperations jdbcOperations = SERVER.getJdbcOperations();
//
//            if (jdbcOperations == null)
//            {
//                throw new IllegalStateException("JdbcOperations not yet initialized");
//            }
//
//            return jdbcOperations;
//        }
//
//        @Override
//        public String getPlaceholder(final int index)
//        {
//            return String.format("$%d", index + 1);
//        }
//
//        <T> Mono<T> close(final Connection connection)
//        {
//            return Mono.from(connection.close()).then(Mono.empty());
//        }
//    }
//
//    @RegisterExtension
//    static final HsqldbServerExtension SERVER = new HsqldbServerExtension();
//
//    private final ConnectionFactory connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder().option(DRIVER, H2_DRIVER)
//            .option(PASSWORD, SERVER.getPassword()).option(URL, SERVER.getUrl()).option(USER, SERVER.getUsername()).build());
//}
