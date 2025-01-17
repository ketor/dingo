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

package io.dingodb.mpu.core;

import java.util.function.Consumer;

public interface CoreListener {

    default void primary(long clock) {
    }

    default void back(long clock) {
    }

    default void mirror(long clock) {
    }

    default void losePrimary(long clock) {
    }

    static CoreListener primary(Consumer<Long> primary) {
        return new CoreListener() {
            @Override
            public void primary(long clock) {
                primary.accept(clock);
            }
        };
    }

    static CoreListener back(Consumer<Long> back) {
        return new CoreListener() {
            @Override
            public void primary(long clock) {
                back.accept(clock);
            }
        };
    }

    static CoreListener mirror(Consumer<Long> mirror) {
        return new CoreListener() {
            @Override
            public void primary(long clock) {
                mirror.accept(clock);
            }
        };
    }

    static CoreListener losePrimary(Consumer<Long> losePrimary) {
        return new CoreListener() {
            @Override
            public void primary(long clock) {
                losePrimary.accept(clock);
            }
        };
    }

}
