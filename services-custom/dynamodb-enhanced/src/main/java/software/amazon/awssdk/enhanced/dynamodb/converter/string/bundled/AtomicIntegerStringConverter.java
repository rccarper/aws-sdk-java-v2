/*
 * Copyright 2010-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.enhanced.dynamodb.converter.string.bundled;

import java.util.concurrent.atomic.AtomicInteger;
import software.amazon.awssdk.annotations.Immutable;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.annotations.ThreadSafe;
import software.amazon.awssdk.enhanced.dynamodb.converter.string.StringConverter;
import software.amazon.awssdk.enhanced.dynamodb.model.TypeToken;

/**
 * A converter between {@link AtomicInteger} and {@link String}.
 *
 * <p>
 * This converts values using {@link IntegerStringConverter}.
 */
@SdkPublicApi
@ThreadSafe
@Immutable
public class AtomicIntegerStringConverter implements StringConverter<AtomicInteger> {
    private static IntegerStringConverter INTEGER_CONVERTER = IntegerStringConverter.create();

    private AtomicIntegerStringConverter() { }

    public static AtomicIntegerStringConverter create() {
        return new AtomicIntegerStringConverter();
    }

    @Override
    public TypeToken<AtomicInteger> type() {
        return TypeToken.of(AtomicInteger.class);
    }

    @Override
    public String toString(AtomicInteger object) {
        return INTEGER_CONVERTER.toString(object.get());
    }

    @Override
    public AtomicInteger fromString(String string) {
        return new AtomicInteger(INTEGER_CONVERTER.fromString(string));
    }
}