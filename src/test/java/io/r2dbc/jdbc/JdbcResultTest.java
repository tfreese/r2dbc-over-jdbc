package io.r2dbc.jdbc;

import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_SMART_NULLS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Thomas Freese
 */
final class JdbcResultTest
{
    /**
     *
     */
    @Test
    void constructorNoRowMetadata()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcResult(Flux.empty(), null, Mono.empty())).withMessage("rowMetadata must not be null");
    }

    /**
     *
     */
    @Test
    void constructorNoRows()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcResult(null, Mono.empty(), Mono.empty())).withMessage("rows must not be null");
    }

    /**
     *
     */
    @Test
    void constructorNoRowsUpdated()
    {
        assertThatNullPointerException().isThrownBy(() -> new JdbcResult(Flux.empty(), Mono.empty(), null)).withMessage("rowsUpdated must not be null");
    }

    /**
     *
     */
    @Test
    void toResultErrorResponse()
    {
        JdbcResult result = mock(JdbcResult.class, RETURNS_SMART_NULLS);

        when(result.map(any())).thenAnswer(arg ->
        {
            return Flux.error(new SQLIntegrityConstraintViolationException("can't do something", "some state", 999)).onErrorMap(SQLException.class,
                    JdbcR2dbcExceptionFactory::create);
        });
        when(result.getRowsUpdated()).thenReturn(Mono.empty());

        result.map((row, rowMetadata) -> row).as(StepVerifier::create).verifyError(R2dbcDataIntegrityViolationException.class);

        result.getRowsUpdated().as(StepVerifier::create).verifyComplete();
    }
}
