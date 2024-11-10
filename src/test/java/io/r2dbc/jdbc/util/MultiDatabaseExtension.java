// Created: 05.04.2021
package io.r2dbc.jdbc.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

/**
 * Dient zum Starten und herunterfahren der DB-Instanzen.
 *
 * @author Thomas Freese
 */
public class MultiDatabaseExtension implements BeforeAllCallback, AfterAllCallback // , ArgumentsProvider
{
    private final Map<EmbeddedDatabaseType, DbServerExtension> servers = new HashMap<>();

    /**
     * Die Junit-{@link Extension} braucht zwingend einen Default-Constructor!
     */
    public MultiDatabaseExtension() {
        super();

        this.servers.computeIfAbsent(EmbeddedDatabaseType.H2, DbServerExtension::new);
        this.servers.computeIfAbsent(EmbeddedDatabaseType.HSQL, DbServerExtension::new);
        this.servers.computeIfAbsent(EmbeddedDatabaseType.DERBY, DbServerExtension::new);
    }

    @Override
    public void afterAll(final ExtensionContext context) throws Exception {
        for (DbServerExtension server : this.servers.values()) {
            server.afterAll(context);
        }
    }

    @Override
    public void beforeAll(final ExtensionContext context) {
        for (DbServerExtension server : this.servers.values()) {
            server.beforeAll(context);
        }
    }

    public DbServerExtension getServer(final EmbeddedDatabaseType databaseType) {
        return this.servers.get(databaseType);
    }

    public Collection<DbServerExtension> getServers() {
        return this.servers.values();
    }

    // @Override
    // public Stream<? extends Arguments> provideArguments(final ExtensionContext context) throws Exception {
    // return getServers().stream().map(server -> Arguments.of(server.getDatabaseType(), server));
    // }
}
