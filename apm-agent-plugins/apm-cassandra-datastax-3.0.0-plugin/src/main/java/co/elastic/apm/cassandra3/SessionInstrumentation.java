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
package co.elastic.apm.cassandra3;

import co.elastic.apm.bci.ElasticApmInstrumentation;
import co.elastic.apm.bci.HelperClassManager;
import co.elastic.apm.bci.VisibleForAdvice;
import co.elastic.apm.cassandra3.helper.Cassandra3Helper;
import co.elastic.apm.impl.ElasticApmTracer;
import com.datastax.driver.core.Session;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import co.elastic.apm.impl.transaction.Span;
import net.bytebuddy.matcher.ElementMatcher;

import javax.annotation.Nullable;
import com.datastax.driver.core.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

import static net.bytebuddy.matcher.ElementMatchers.*;

/**
 * Matches the various {@link Session#execute} method
 * and keeps a reference to from the resulting {@link Statement} to the sql.
 */
public class SessionInstrumentation extends ElasticApmInstrumentation {

    static final String CASSANDRA_3_INSTRUMENTATION_GROUP = "cassandra-3";
    @VisibleForAdvice
    public static final Map<Object, String> statementCqlMap = Collections.synchronizedMap(new WeakHashMap<Object, String>());

    @Nullable
    @VisibleForAdvice
    public static HelperClassManager<Cassandra3Helper> cassandra3Helper;

    @Nullable
    @VisibleForAdvice
    @Advice.OnMethodEnter
    public static Span onBeforeExecute(@Advice.This Session session, @Advice.Argument(0) Statement statement) {
        if (tracer != null && cassandra3Helper != null) {
            final String cql = statement.toString();
            return cassandra3Helper.getForClassLoaderOfClass(Statement.class).createCassandra3Span(cql, session, tracer.getActive());
        }
        return null;
    }

    @VisibleForAdvice
    @Advice.OnMethodExit(onThrowable = Throwable.class)
    public static void onAfterExecute(@Advice.Enter @Nullable Span span, @Advice.Thrown Throwable t) {
        if (span != null) {
            span.captureException(t)
                .deactivate()
                .end();
        }
    }

    @Override
    public void init(ElasticApmTracer tracer) {
        cassandra3Helper = HelperClassManager.ForSingleClassLoader.of(tracer, "co.elastic.apm.cassandra3.helper.Cassandra3HelperImpl",
            "co.elastic.apm.cassandra3.helper.Cassandra3HelperImpl$SessionMetaData");
    }

    /**
     * Returns the CQL statement belonging to provided {@link Statement}.
     * <p>
     * Might return {@code null} when the provided {@link Statement} is a wrapper of the actual statement.
     * </p>
     *
     * @return the CQL statement belonging to provided {@link Statement}, or {@code null}
     */
    @Nullable
    @VisibleForAdvice
    public static String getCqlForStatement(Object statement) {
        final String cql = statementCqlMap.get(statement);
        if (cql != null) {
            statementCqlMap.remove(statement);
        }
        return cql;
    }

    @Override
    public ElementMatcher<? super TypeDescription> getTypeMatcher() {
        return not(isInterface())
            // pre-select candidates for the more expensive hasSuperType matcher
            .and(nameContains("Session"))
            .and(hasSuperType(named("com.datastax.driver.core.Session")));
    }

    @Override
    public ElementMatcher<? super MethodDescription> getMethodMatcher() {
        return nameStartsWith("execute")
            .and(isPublic())
            .and(returns(hasSuperType(named("com.datastax.driver.core.Statement"))))
            .and(takesArgument(0, String.class));
    }

    @Override
    public Collection<String> getInstrumentationGroupNames() {
        return Collections.singleton(CASSANDRA_3_INSTRUMENTATION_GROUP);
    }

}
