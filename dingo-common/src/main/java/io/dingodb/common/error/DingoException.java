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

package io.dingodb.common.error;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashMap;
import java.util.regex.Pattern;

/**
 * DingoException is a RuntimeException implementing DingoError. It has stack trace populated by JVM, at the same time
 * it can be treated as a normal DingoError.
 *
 * <p>A common usage of DingoException is to throw it from deep nested function, caught
 * and cast it as DingoError to caller. Following is an example:
 *
 * <pre>{@code
 *     private void doPrivateThing() {
 *         throw new DingoException(SomeErrors.INVALID_REQUEST, "out of range value in field xxx");
 *     }
 *
 *     public DingoError doPublicThing() {
 *         try {
 *             doPrivateThing();
 *         } catch (DingoException err) {
 *             return err;
 *         }
 *         return null;
 *     }
 * }</pre>
 *
 * <p>When logging, cast it back to DingoException if logger support Throwable logging,
 * this way stack trace got printed. Customized logging function is also ok. Such as:
 *
 * <pre>{@code
 *     public void logError(Logger logger, DingoError err) {
 *         if (err is Throwable) {
 *             String info = String.format("error code: %d, info: %s", err.getCode(), err.getInfo())
 *             logger.warn(info, (Throwable)err)
 *         } else {
 *             logger.warn("error code: {}, info: {}, message: {}", err.getCode(), err.getInfo(), err.getMessage());
 *         }
 *     }
 * }</pre>
 */
public class DingoException extends RuntimeException implements IndirectError {
    private static final long serialVersionUID = 5564571207617481306L;
    /**
     * The following exception patterns are used to convert to DingoException from Calcite.
     */
    public static Integer OBJECT_NOT_FOUND = 90002;
    public static Integer INSERT_NULL_TO_NON_NULL_COLUMN = 90003;
    public static Integer INSERT_NULL_POINTER = 90004;
    public static Integer TYPE_CAST_ERROR = 90005;
    public static Integer TABLE_ALREADY_EXISTS = 90007;
    public static Integer DUPLICATED_COLUMN = 90009;
    public static Integer PRIMARY_KEY_REQUIRED = 90010;
    public static Integer ASSIGNED_MORE_THAN_ONCE = 90011;
    public static Integer INSERT_COLUMN_NUMBER_NOT_EQUAL = 90013;
    public static Integer JOIN_NAME_DUPLICATED = 90015;
    public static Integer JOIN_NO_CONDITION = 90016;
    public static Integer JOIN_SELECT_COLUMN_AMBIGUOUS = 90017;
    public static Integer INTERPRET_ERROR = 90019;
    public static Integer FUNCTION_NOT_SUPPORT = 90022;
    public static Integer EXECUTOR_NODE_FAIL = 90024;
    public static HashMap<Pattern, Integer> CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP;
    public static HashMap<Pattern, Integer> RUNTIME_EXCEPTION_PATTERN_CODE_MAP;
    // TODO
    //public static HashMap<Pattern, Integer> SQL_EXCEPTION_PATTERN_CODE_MAP;
    //public static HashMap<Pattern, Integer> SQL_PARSE_EXCEPTION_PATTERN_CODE_MAP;

    static {
        CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP = new HashMap<>();
        // "Table Not Found" from CalciteContextException (90002)
        CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("Object .* not found"), OBJECT_NOT_FOUND);


        // "Table Already exists" from CalciteContextException (90007)
        CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("Table .* already exists"),
            TABLE_ALREADY_EXISTS);
        // "Column Not Found" from  CalciteContextException (90002)
        CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("Unknown target column.*"),
            OBJECT_NOT_FOUND);
        CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("Column .* not found in any table"),
            OBJECT_NOT_FOUND);

        // "Insert Columns more than once" from CalciteContextException (90011)
        CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("Target column .* is"
            + " assigned more than once"), ASSIGNED_MORE_THAN_ONCE);
        // Insert Columns not equal" from CalciteContextException  (90013)
        CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("Number of INSERT target columns "
            + "\\(.*\\) does not equal number"), INSERT_COLUMN_NUMBER_NOT_EQUAL);
        // Function not found from CalciteContextException (90022)
        CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP
            .put(Pattern.compile("No match found for function signature"), FUNCTION_NOT_SUPPORT);

        // Join name duplicated
        CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile(" Duplicate relation name"),
            JOIN_NAME_DUPLICATED);
        // Join need join condition
        CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("INNER, LEFT, RIGHT or FULL join"
            + " requires a condition"), JOIN_NO_CONDITION);
        CALCITE_CONTEXT_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("Column .* is ambiguous"),
            JOIN_SELECT_COLUMN_AMBIGUOUS);

        RUNTIME_EXCEPTION_PATTERN_CODE_MAP = new HashMap<>();
        // "Table Already exists"  (90007)
        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("Table .* already exists"),
            TABLE_ALREADY_EXISTS);
        // "Insert Null into non-null column"
        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("Column .* has no default "
            + "value and does not allow NULLs"), INSERT_NULL_TO_NON_NULL_COLUMN);
        // "Duplicated Columns" from RuntimeException (90009)
        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("Duplicate column names"), DUPLICATED_COLUMN);
        // "Create Without Primary Key" from RuntimeException (90010)
        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("Primary keys are required"), PRIMARY_KEY_REQUIRED);
        // "Insert Without Primary Key" from NullPointerException based on RuntimeException (90004)
        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("java\\.lang\\.NullPointerException: null"),
            INSERT_NULL_POINTER);
        // "Wrong Function Argument" from RunTimeException(90019)
        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile(".* does not match"), INTERPRET_ERROR);
        // "Time Range Error"
        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile(".* to time/date/datetime"), INTERPRET_ERROR);
        // "Type  Error"
        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("Error while applying rule DingoValuesReduceRule"),
            INTERPRET_ERROR);

        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("exception\\.FailGetEvaluator"), TYPE_CAST_ERROR);
        // "Number Range Error"
        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("exceeds max .* or lower min value"), TYPE_CAST_ERROR);
        // "Coordinator Failover Error"
        RUNTIME_EXCEPTION_PATTERN_CODE_MAP.put(Pattern.compile("Table meta save success, but schedule failed"),
            EXECUTOR_NODE_FAIL);
    }

    private final DingoError category;
    private final DingoError reason;

    /**
     * Construct DingoException from given error, use given error's basic info as its base info and given message as its
     * message.
     *
     * @param err     error object
     * @param message error message
     */
    private DingoException(DingoError err, String message) {
        this(err, OK, message);
    }

    private DingoException(DingoError err, DingoError reason) {
        this(err, reason, err.getMessage());
    }

    private DingoException(@NonNull DingoError err, DingoError reason, String message) {
        super(message);
        this.category = err.getCategory();
        this.reason = reason;
    }

    /**
     * Construct DingoException from given error. Result exception will have category and message same as given
     * error's.
     *
     * @param err error object
     */
    public static @NonNull DingoException from(DingoError err) {
        if (err instanceof DingoException) {
            return (DingoException) err;
        }
        return from(err, err.getMessage());
    }

    /**
     * Construct DingoException from given error. Result exception will have category same as given error's and message
     * equal to given message.
     *
     * @param err     error object
     * @param message error message
     */
    public static @NonNull DingoException from(DingoError err, String message) {
        return new DingoException(err, message);
    }

    /**
     * Construct DingoException from {@link Throwable}. Result exception will be the given throwable if it is an
     * DingoException, or it will have category {@link DingoError#UNKNOWN} and message, stack trace from that
     * throwable.
     *
     * @param throwable a throwable object
     * @return this throwable if it is DingoException, otherwise a new DingoException with category {@link
     *     DingoError#UNKNOWN} and message, stack trace from that throwable.
     */
    public static @NonNull DingoException from(Throwable throwable) {
        if (throwable instanceof DingoException) {
            return (DingoException) throwable;
        }
        DingoException ex = new DingoException(UNKNOWN, throwable.getMessage());
        ex.initCause(throwable);
        return ex;
    }

    /**
     * Construct DingoException with category and message from given error and wrap a reason error beneath it.
     *
     * @param err    error object
     * @param reason reason error
     * @return an DingoException with same category and message as given error, but a reason error beneath it
     */
    public static @NonNull DingoException wrap(DingoError err, DingoError reason) {
        if (reason instanceof Throwable) {
            return wrap(err, (Throwable) reason);
        }
        return new DingoException(err, reason);
    }

    /**
     * Construct DingoException with category and message from given error and wrap a throwable as its reason and
     * exception cause.
     *
     * @param err    error object
     * @param reason a throwable reason
     * @return an DingoException with same category and message as given error, with given reason as it's reason and
     *     exception cause.
     */
    public static @NonNull DingoException wrap(DingoError err, Throwable reason) {
        DingoException ex = new DingoException(err, DingoError.from(reason));
        ex.initCause(reason);
        return ex;
    }

    /**
     * Construct DingoException with category and message from given error and wrap an exception as its reason and
     * exception cause.
     *
     * @param err    error object
     * @param reason exception object
     * @return an DingoException with same category and message as given error, with given exception as its reason and
     *     exception cause.
     */
    public static @NonNull DingoException wrap(DingoError err, DingoException reason) {
        return wrap(err, (Throwable) reason);
    }

    @Override
    public DingoError getCategory() {
        return category;
    }

    @Override
    public DingoError getReason() {
        return reason;
    }

    /**
     * Get stack trace from this exception. It is possible for exception to have zero elements in its stack trace.
     *
     * @return stack trace for DingoException, zero length array if no stack traces.
     * @see Throwable#getStackTrace()
     *     Some virtual machines may, under some circumstances, omit one or more
     *     stack frames from the stack trace. In the extreme case, a virtual machine that has no stack trace information
     *     concerning this throwable is permitted to return a zero-length array from this method.
     */
    @Override
    public StackTraceElement[] getStackTrace() {
        return super.getStackTrace();
    }
}
