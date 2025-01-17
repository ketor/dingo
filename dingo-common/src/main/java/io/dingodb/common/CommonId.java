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

package io.dingodb.common;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.common.util.ByteArrayUtils;
import io.dingodb.common.util.Utils;
import lombok.EqualsAndHashCode;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class CommonId implements Comparable<CommonId>, Serializable {
    private static final long serialVersionUID = 3355195360067107406L;

    public static final int TYPE_LEN = 1;
    public static final int IDENTIFIER_LEN = 2;
    public static final int DOMAIN_LEN = 4;
    public static final int SEQ_LEN = 4;
    public static final int LEN = TYPE_LEN + IDENTIFIER_LEN + DOMAIN_LEN + SEQ_LEN;

    public static final int TYPE_IDX = 0;
    public static final int IDENTIFIER_IDX = TYPE_IDX + TYPE_LEN;
    public static final int DOMAIN_IDX = IDENTIFIER_IDX + IDENTIFIER_LEN;
    public static final int SEQ_IDX = DOMAIN_IDX + DOMAIN_LEN;

    @EqualsAndHashCode.Include
    private byte[] content;

    private byte type;
    private byte[] identifier;
    private byte[] domain;
    private byte[] seq;

    private transient volatile String str;

    private CommonId() {
    }

    public CommonId(byte type, byte[] identifier, byte[] domain, int seq) {
        this(type, identifier, domain, PrimitiveCodec.encodeInt(seq));
    }

    public CommonId(byte type, byte[] identifier, byte[] domain, byte[] seq) {
        if (identifier.length > IDENTIFIER_LEN) {
            throw new IllegalArgumentException("Identifier length must " + IDENTIFIER_LEN);
        }
        if (domain.length > DOMAIN_LEN) {
            throw new IllegalArgumentException("Identifier length must " + DOMAIN_LEN);
        }
        this.type = type;
        this.identifier = new byte[] {identifier[0], identifier[1]};
        this.domain = new byte[] {domain[0], domain[1], domain[2], domain[3]};
        this.seq = new byte[] {seq[0], seq[1], seq[2], seq[3]};
        this.content = new byte[] {
            type,
            identifier[0], identifier[1],
            domain[0], domain[1], domain[2], domain[3],
            seq[0], seq[1], seq[2], seq[3],
            };
    }

    public byte[] toBytes4Transfer() {
        /**
         * total_field_cnt * 4 + type + identifier + domain + seqInBytes
         */
        int totalLen = 4 * Utils.INTEGER_LEN_IN_BYTES + (1 + identifier.length + domain.length + seq.length);
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalLen);
        byteBuffer.putInt(1)
            .put(type)
            .putInt(identifier.length)
            .put(identifier)
            .putInt(domain.length)
            .put(domain)
            .putInt(seq.length)
            .put(seq);
        return byteBuffer.array();
    }

    public static CommonId fromBytes4Transfer(byte[] bytes) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

        /**
         * get type of byte.
         */
        byteBuffer.getInt();
        byte type = byteBuffer.get();

        /**
         * get identifier of bytes.
         */
        int identifierLen = byteBuffer.getInt();
        byte[] identifier = new byte[identifierLen];
        byteBuffer.get(identifier);

        /**
         * get domain of bytes.
         */
        int domainLen = byteBuffer.getInt();
        byte[] domain = new byte[domainLen];
        byteBuffer.get(domain);

        /**
         * get seq in bytes.
         */
        int seqLen = byteBuffer.getInt();
        byte[] seqInBytes = new byte[seqLen];
        byteBuffer.get(seqInBytes);
        return new CommonId(type, identifier, domain, seqInBytes);
    }

    @Override
    public int compareTo(CommonId other) {
        return ByteArrayUtils.compare(content, other.content);
    }

    public byte[] content() {
        byte[] content = new byte[this.content.length];
        System.arraycopy(this.content, 0, content, 0, content.length);
        return content;
    }

    public byte type() {
        return type;
    }

    public byte[] identifier() {
        return new byte[] {identifier[0], identifier[1]};
    }

    public int domain() {
        return PrimitiveCodec.readInt(domain);
    }

    public byte[] domainContent() {
        return new byte[] {domain[0], domain[1], domain[2], domain[3]};
    }

    public int seq() {
        return PrimitiveCodec.readInt(seq);
    }

    public byte[] seqContent() {
        return new byte[] {seq[0], seq[1], seq[2], seq[3]};
    }

    @Override
    public String toString() {
        if (str == null) {
            String str = new String(new byte[] {type, '-', identifier[0], identifier[1]});
            Integer namespace = PrimitiveCodec.readInt(this.domain);
            if (namespace != null) {
                str = str + "-" + namespace;
            }
            Integer seq = PrimitiveCodec.readInt(this.seq);
            if (seq != null) {
                str = str + "-" + seq;
            }
            this.str = str;
        }
        return str;
    }

    public byte[] encode() {
        byte[] encode = new byte[content.length];
        System.arraycopy(content, 0, encode, 0, encode.length);
        return encode;
    }

    public static CommonId decode(byte[] content) {
        return new CommonId(
            content[0],
            new byte[] {content[IDENTIFIER_IDX], content[IDENTIFIER_IDX + 1]},
            new byte[] {content[DOMAIN_IDX], content[DOMAIN_IDX + 1], content[DOMAIN_IDX + 2], content[DOMAIN_IDX + 3]},
            new byte[] {content[SEQ_IDX], content[SEQ_IDX + 1], content[SEQ_IDX + 2], content[SEQ_IDX + 3]}
        );
    }

    public static CommonId decode(ByteBuffer buffer) {
        CommonId result = decode(buffer.array());
        buffer.position(buffer.position() + LEN);
        return result;
    }

    public static CommonId prefix(byte type) {
        CommonId commonId = new CommonId();
        commonId.type = type;
        commonId.identifier = new byte[] {'0', '0'};
        commonId.content = new byte[] {commonId.type, commonId.identifier[0], commonId.identifier[1]};
        return commonId;
    }

    public static CommonId prefix(byte type, byte[] identifier) {
        CommonId commonId = new CommonId();
        commonId.type = type;
        commonId.identifier = new byte[] {identifier[0], identifier[1]};
        commonId.content = new byte[] {commonId.type, commonId.identifier[0], commonId.identifier[1]};
        return commonId;
    }

    public static CommonId prefix(byte type, byte[] identifier, byte[] domain) {
        CommonId commonId = new CommonId();
        commonId.type = type;
        commonId.identifier = new byte[] {identifier[0], identifier[1]};
        commonId.domain = new byte[] {domain[0], domain[1], domain[2], domain[3]};
        commonId.content = new byte[] {
            commonId.type,
            commonId.identifier[0],
            commonId.identifier[1],
            commonId.domain[0],
            commonId.domain[1],
            commonId.domain[2],
            commonId.domain[3]
        };
        return commonId;
    }

    public static class JacksonSerializer extends StdSerializer<CommonId> {
        protected JacksonSerializer() {
            super(CommonId.class);
        }

        @Override
        public void serialize(CommonId value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeBinary(value.content);
        }
    }

    public static class JacksonDeserializer extends StdDeserializer<CommonId> {
        protected JacksonDeserializer() {
            super(CommonId.class);
        }

        @Override
        public CommonId deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            return decode(parser.getBinaryValue());
        }
    }
}
