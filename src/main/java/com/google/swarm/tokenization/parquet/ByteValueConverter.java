/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.swarm.tokenization.parquet;

import com.google.privacy.dlp.v2.Value;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/** Utility to manipulate {@link Value} objects representing raw bytes. */
public final class ByteValueConverter {

    private ByteValueConverter() {}

    /** Factory to create instance of the Value decorator. */
    public static ByteValueDecorator of(Value value) {
        return new ByteValueDecorator(value);
    }

    /**
     * Returns a {@link Value} object by converting the rawBytes as Base64 encoded string.
     *
     * @param rawBytes the bytes to represent as Value object.
     */
    public static Value convertBytesToValue(byte[] rawBytes) {
        return Value.newBuilder().setStringValue(Base64.getEncoder().encodeToString(rawBytes)).build();
    }

    /** Decorator class providing operations on {@link Value} object to bytes. */
    public static final class ByteValueDecorator {

        private final Value value;

        private ByteValueDecorator(Value value) {
            this.value = value;
        }

        /** Returns string for putting the bytes as JSON field. */
        public String asJsonString() {
            return new String(forBytes().array(), StandardCharsets.ISO_8859_1);
        }

        public ByteBuffer forBytes() {
            return ByteBuffer.wrap(extractValueBytes());
        }

        /**
         * Returns the decoded bytes as a reversal function. {@link
         * ByteValueConverter#convertBytesToValue(byte[])}.
         */
        private byte[] extractValueBytes() {
            return Base64.getDecoder().decode(value.getStringValue());
        }
    }
}

