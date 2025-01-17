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

package io.dingodb.common.codec.transfer.impl;

import io.dingodb.common.CommonId;
import io.dingodb.common.codec.transfer.KeyValueTransferCodeC;
import io.dingodb.common.store.KeyValue;
import io.dingodb.common.util.Utils;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class UpsertKeyValueUsingListCodec implements KeyValueTransferCodeC {

    public static UpsertKeyValueUsingListCodec INSTANCE = new UpsertKeyValueUsingListCodec();

    /**
     * input byteBuffer(the method name has been reed, the position has been seek)
     * @param byteBuffer input byteBuffer
     * @return CommonId, array of KeyValue
     */
    public Object[] read(ByteBuffer byteBuffer) {
        List<Object> objectArray = new ArrayList<>(2);
        int commonIdLen = byteBuffer.getInt();
        byte[] commonIdInBytes = new byte[commonIdLen];
        byteBuffer.get(commonIdInBytes);

        CommonId commonId = CommonId.fromBytes4Transfer(commonIdInBytes);
        objectArray.add(commonId);

        int keyValueCnt = byteBuffer.getInt();
        List<KeyValue> keyValueList = null;
        if (keyValueCnt > 0) {
            keyValueList = new ArrayList<>(keyValueCnt);

            for (int i = 0; i < keyValueCnt; i++) {
                int keyLen = byteBuffer.getInt();
                byte[] keyInBytes = new byte[keyLen];
                byteBuffer.get(keyInBytes);

                int valueLen = byteBuffer.getInt();
                byte[] valueInBytes = new byte[valueLen];
                byteBuffer.get(valueInBytes);

                keyValueList.add(new KeyValue(keyInBytes, valueInBytes));
            }
        }
        objectArray.add(keyValueList);
        return objectArray.toArray();
    }

    public byte[] write(Object[] objectArray) {
        /**
         * input args: CommonId, List<KeyValue>
         * output format:
         *  size|commonId in bytes|Count(list element)|len(key)|key|len(value)|value|....
         */
        if (objectArray.length != 2) {
            return null;
        }

        CommonId commonId = (CommonId) objectArray[0];
        byte[] commonIdInBytes = commonId.toBytes4Transfer();

        List<KeyValue> keyValueList = (List<KeyValue>) objectArray[1];
        int keyValueCnt = keyValueList.size();

        int totalKeySize = keyValueList
            .stream().map(x -> x == null ? 0 : x.getKey().length).reduce(Integer::sum).get();
        int totalValueSize = keyValueList
            .stream().map(x -> x == null ? 0 : x.getValue().length).reduce(Integer::sum).get();

        /**
         * int(CommonIdSize)|commonId in bytes|Count(list element)|len(key)|key|len(value)|value|....
         */
        int totalLen = (Utils.INTEGER_LEN_IN_BYTES + commonIdInBytes.length)
            + (Utils.INTEGER_LEN_IN_BYTES + 2 * Utils.INTEGER_LEN_IN_BYTES * keyValueCnt)
            + totalKeySize + totalValueSize;

        ByteBuffer byteBuffer = ByteBuffer.allocate(totalLen);
        byteBuffer
            .putInt(commonIdInBytes.length)
            .put(commonIdInBytes);

        // total key value count
        byteBuffer.putInt(keyValueCnt);

        for (KeyValue keyValue: keyValueList) {
            int keyLen = keyValue.getKey() == null ? 0 : keyValue.getKey().length;
            int valueLen = keyValue.getValue() == null ? 0 : keyValue.getValue().length;
            byteBuffer.putInt(keyLen)
                .put(keyValue.getKey())
                .putInt(valueLen)
                .put(keyValue.getValue());
        }
        return byteBuffer.array();
    }
}
