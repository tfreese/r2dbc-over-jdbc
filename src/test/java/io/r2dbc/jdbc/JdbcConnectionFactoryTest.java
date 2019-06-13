package io.r2dbc.jdbc;

import io.r2dbc.jdbc.util.HsqldbServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import reactor.test.StepVerifier;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/**
 * @author Thomas Freese
 */
final class JdbcConnectionFactoryTest
{
    /**
     *
     */
    @RegisterExtension
    static final HsqldbServerExtension SERVER = new HsqldbServerExtension();

    /**
     *
     */
    @Test
    void constructorNoConfiguration()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcConnectionFactory((DataSource) null)).withMessage("dataSource must not be null");
    }

    /**
     *
     */
    @Test
    void create()
    {
        JdbcConnectionConfiguration configuration = JdbcConnectionConfiguration.builder().dataSource(SERVER.getDataSource()).build();

        new JdbcConnectionFactory(configuration).create().as(StepVerifier::create).expectNextCount(1).verifyComplete();
    }

    /**
     *
     */
    @Test
    void getMetadata()
    {
        JdbcConnectionConfiguration configuration = JdbcConnectionConfiguration.builder().dataSource(SERVER.getDataSource()).build();

        assertThat(new JdbcConnectionFactory(configuration).getMetadata()).isNotNull();
    }

    /**
     *
     */
    @Test
    void options()
    {
        JdbcConnectionConfiguration configuration = JdbcConnectionConfiguration.builder().dataSource(SERVER.getDataSource()).build();

        assertThat(configuration.getDataSource()).isEqualTo(SERVER.getDataSource());
    }
}
