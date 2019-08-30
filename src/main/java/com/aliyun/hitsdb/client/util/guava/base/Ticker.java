/*
 * Copyright (C) 2012 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aliyun.hitsdb.client.util.guava.base;

/**
 * The implementation of Ticker is derived from the Ticker class of Guava 16.0,
 * which is licensed under the Apache License 2.0
 */
public abstract class Ticker {
    /**
     * Constructor for use by subclasses.
     */
    protected Ticker() {}

    /**
     * Returns the number of nanoseconds elapsed since this ticker's fixed
     * point of reference.
     */
    public abstract long read();

    /**
     * A ticker that reads the current time using {@link System#nanoTime}.
     *
     * @since 10.0
     */
    public static Ticker systemTicker() {
        return SYSTEM_TICKER;
    }

    /**
     * The original implementation of read() is to return Platform.systemNanoTime()
     * However, it is equivalent with System.nanoTime()
     */
    private static final Ticker SYSTEM_TICKER = new Ticker() {
        @Override
        public long read() {
            return System.nanoTime();
        }
    };
}
