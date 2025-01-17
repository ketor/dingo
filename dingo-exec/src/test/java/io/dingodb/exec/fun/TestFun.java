/*
 * Copyright 2021 DataCanvas
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.dingodb.exec.fun;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.dingodb.expr.parser.exception.ExprCompileException;
import io.dingodb.expr.parser.exception.ExprParseException;
import io.dingodb.expr.parser.parser.DingoExprCompiler;
import io.dingodb.expr.test.ExprTestUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Timestamp;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
public class TestFun {
    private static DingoExprCompiler compiler;

    @BeforeAll
    public static void setupAll() {
        compiler = new DingoExprCompiler(DingoFunFactory.getInstance());
    }

    private static @NonNull Stream<Arguments> getParameters() {
        return Stream.of(
            // string fun
            arguments("substring('DingoDatabase', 1, 5)", "Dingo"),
            arguments("substring('DingoDatabase', 1, 100)", "DingoDatabase"),
            arguments("substring('DingoDatabase', 2, int(2.5))", "ing"),
            arguments("substring('DingoDatabase', 2, 2.5)", "ing"),
            arguments("substring('DingoDatabase', 2, -3)", ""),
            arguments("substring('DingoDatabase', -4, 4)", "base"),
            arguments("substring('abcde', 1, 6)", "abcde"),
            arguments("lower('HeLlO')", "hello"),
            arguments("upper('HeLlO')", "HELLO"),
            arguments("trim(' HeLlO \\n\\t')", "HeLlO"),
            arguments("replace('I love $name', '$name', 'Lucia')", "I love Lucia"),
            // date&time
            arguments("date_format(date('1980-2-3'), '%Y:%m:%d')", "1980:02:03"),
            arguments("time_format(time('23:11:25'), '%H-%i-%s')", "23-11-25"),
            arguments("timestamp_format(timestamp('1980-2-3 23:11:25'), '%Y%m%d %T')", "19800203 23:11:25"),
            arguments(
                "timestamp_format(timestamp('1980-2-3 23:11:25'), 'Date: %Y%m%d Time: %T')",
                "Date: 19800203 Time: 23:11:25"
            ),
            arguments(
                "unix_timestamp(timestamp('2022-04-14 00:00:00'))",
                Timestamp.valueOf("2022-04-14 00:00:00").getTime() / 1000L
            ),
            // collection types
            arguments("array(1, 2, 3)", new Object[]{1, 2, 3}),
            arguments("list(4, 5, 6)", ImmutableList.of(4, 5, 6)),
            arguments("map('a', 1, 'b', 2)", ImmutableMap.of("a", 1, "b", 2)),
            arguments("cast_list_items('INT', list(7, 8, 9))", ImmutableList.of(7, 8, 9))
        );
    }

    @ParameterizedTest
    @MethodSource("getParameters")
    public void test(String exprString, Object value) throws ExprCompileException, ExprParseException {
        ExprTestUtils.testEval(compiler, exprString, value);
    }
}
