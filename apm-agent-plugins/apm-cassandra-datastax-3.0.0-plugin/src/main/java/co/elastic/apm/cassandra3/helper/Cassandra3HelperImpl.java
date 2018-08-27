/*-
 * #%L
 * Elastic APM Java agent
 * %%
 * Copyright (C) 2018 Elastic and contributors
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package co.elastic.apm.cassandra3.helper;

import co.elastic.apm.impl.transaction.AbstractSpan;
import co.elastic.apm.impl.transaction.Span;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

public class Cassandra3HelperImpl implements Cassandra3Helper {

    @Override
    @Nullable
    public Span createCassandra3Span(@Nullable String cql, Session session, @Nullable AbstractSpan<?> parent) {
        if (cql == null || isAlreadyMonitored(parent) || parent == null || !parent.isSampled()) {
            return null;
        }
        Span span = parent.createSpan().activate();
        span.setName(getMethod(cql));
        // temporarily setting the type here is important
        // getting the meta data can result in another jdbc call
        // if that is traced as well -> StackOverflowError
        // to work around that, isAlreadyMonitored checks if the parent span is a db span and ignores them
        span.withType("db.cassandra3.cql");
        span.getContext().getDb()
            .withStatement(cql)
            .withType("cql");

        return span;
    }

    @Nullable
    private String getMethod(@Nullable String sql) {
        if (sql == null) {
            return null;
        }
        // don't allocate objects for the common case
        if (sql.startsWith("SELECT") || sql.startsWith("select")) {
            return "SELECT";
        }
        sql = sql.trim();
        final int indexOfWhitespace = sql.indexOf(' ');
        if (indexOfWhitespace > 0) {
            return sql.substring(0, indexOfWhitespace).toUpperCase();
        } else {
            // for example COMMIT
            return sql.toUpperCase();
        }
    }

    /*
     * This makes sure that even when there are wrappers for the statement,
     * we only record each JDBC call once.
     */
    private boolean isAlreadyMonitored(@Nullable AbstractSpan<?> parent) {
        if (!(parent instanceof Span)) {
            return false;
        }
        Span parentSpan = (Span) parent;
        // a db span can't be the child of another db span
        // this means the span has already been created for this db call
        return parentSpan.getType() != null && parentSpan.getType().startsWith("db.");
    }
}
