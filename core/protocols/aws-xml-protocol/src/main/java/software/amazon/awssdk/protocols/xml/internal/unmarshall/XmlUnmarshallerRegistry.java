/*
 * Copyright 2010-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package software.amazon.awssdk.protocols.xml.internal.unmarshall;

import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.protocol.MarshallLocation;
import software.amazon.awssdk.core.protocol.MarshallingType;
import software.amazon.awssdk.protocols.core.AbstractMarshallingRegistry;

@SdkInternalApi
public final class XmlUnmarshallerRegistry extends AbstractMarshallingRegistry {

    private XmlUnmarshallerRegistry(Builder builder) {
        super(builder);
    }

    @SuppressWarnings("unchecked")
    public <T> XmlUnmarshaller<Object> getUnmarshaller(MarshallLocation marshallLocation, MarshallingType<T> marshallingType) {
        return (XmlUnmarshaller<Object>) get(marshallLocation, marshallingType);
    }

    /**
     * @return Builder instance to construct a {@link XmlUnmarshallerRegistry}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for a {@link XmlUnmarshallerRegistry}.
     */
    public static final class Builder extends AbstractMarshallingRegistry.Builder {

        private Builder() {
        }

        public <T> Builder payloadUnmarshaller(MarshallingType<T> marshallingType,
                                               XmlUnmarshaller<T> marshaller) {
            register(MarshallLocation.PAYLOAD, marshallingType, marshaller);
            return this;
        }

        public <T> Builder headerUnmarshaller(MarshallingType<T> marshallingType,
                                              XmlUnmarshaller<T> marshaller) {
            register(MarshallLocation.HEADER, marshallingType, marshaller);
            return this;
        }

        public <T> Builder statusCodeUnmarshaller(MarshallingType<T> marshallingType,
                                                  XmlUnmarshaller<T> marshaller) {
            register(MarshallLocation.STATUS_CODE, marshallingType, marshaller);
            return this;
        }

        /**
         * @return An immutable {@link XmlUnmarshallerRegistry} object.
         */
        public XmlUnmarshallerRegistry build() {
            return new XmlUnmarshallerRegistry(this);
        }
    }
}
