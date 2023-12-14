// Created: 14.06.2019
package io.r2dbc.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

import java.util.stream.Stream;

import io.r2dbc.jdbc.util.DbServerExtension;
import io.r2dbc.jdbc.util.MultiDatabaseExtension;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import reactor.test.StepVerifier;

/**
 * @author Thomas Freese
 */
final class ParameterizedConnectionFactoryTest {
    @RegisterExtension
    static final MultiDatabaseExtension DATABASE_EXTENSION = new MultiDatabaseExtension();

    static Stream<Arguments> getDatabases() {
        return DATABASE_EXTENSION.getServers().stream().map(server -> Arguments.of(server.getDatabaseType(), server));
    }

    @Test
    void testConstructorNoConfiguration() {
        assertThatNullPointerException().isThrownBy(() -> new JdbcConnectionFactory(null, null)).withMessage("dataSource must not be null");
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testCreate") // Ohne Parameter
    @MethodSource("getDatabases")
    void testCreate(final EmbeddedDatabaseType databaseType, final DbServerExtension server) {
        final JdbcConnectionConfiguration configuration = JdbcConnectionConfiguration.builder().dataSource(server.getDataSource()).build();

        new JdbcConnectionFactory(configuration).create().as(StepVerifier::create).expectNextCount(1).verifyComplete();
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testGetMetadata") // Ohne Parameter
    @MethodSource("getDatabases")
    void testGetMetadata(final EmbeddedDatabaseType databaseType, final DbServerExtension server) {
        final JdbcConnectionConfiguration configuration = JdbcConnectionConfiguration.builder().dataSource(server.getDataSource()).build();

        assertThat(new JdbcConnectionFactory(configuration).getMetadata()).isNotNull();
    }

    @ParameterizedTest(name = "{index} -> {0}")
    @DisplayName("testOptions") // Ohne Parameter
    @MethodSource("getDatabases")
    void testOptions(final EmbeddedDatabaseType databaseType, final DbServerExtension server) {
        final JdbcConnectionConfiguration configuration = JdbcConnectionConfiguration.builder().dataSource(server.getDataSource()).build();

        assertThat(configuration.getDataSource()).isEqualTo(server.getDataSource());
    }
}
