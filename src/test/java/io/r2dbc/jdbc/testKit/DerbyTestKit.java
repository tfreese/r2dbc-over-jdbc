// Created: 06.04.2021
package io.r2dbc.jdbc.testKit;

import io.r2dbc.jdbc.util.DbServerExtension;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

/**
 * @author Thomas Freese
 */
public class DerbyTestKit extends AbstractTestKit {
    @RegisterExtension
    static final DbServerExtension SERVER = new DbServerExtension(EmbeddedDatabaseType.DERBY);

    /**
     * @see io.r2dbc.jdbc.testKit.AbstractTestKit#getServer()
     */
    @Override
    DbServerExtension getServer() {
        return SERVER;
    }
}
