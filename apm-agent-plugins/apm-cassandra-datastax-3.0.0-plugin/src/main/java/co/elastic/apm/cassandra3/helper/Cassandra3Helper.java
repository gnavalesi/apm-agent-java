package co.elastic.apm.cassandra3.helper;

import co.elastic.apm.impl.transaction.AbstractSpan;
import co.elastic.apm.impl.transaction.Span;
import com.datastax.driver.core.Session;

import javax.annotation.Nullable;

public interface Cassandra3Helper {
    @Nullable
    Span createCassandra3Span(@Nullable String sql, Session session, @Nullable AbstractSpan<?> parent);
}
