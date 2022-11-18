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

package io.dingodb.net.netty.service;

import io.dingodb.common.codec.PrimitiveCodec;
import io.dingodb.net.Message;
import io.dingodb.net.netty.Channel;
import io.dingodb.net.netty.NetService;
import io.dingodb.net.netty.NetServiceProvider;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;

import static io.dingodb.common.util.NoBreakFunctions.wrap;
import static io.dingodb.net.Message.FILE_TRANSFER;

@Slf4j
public class FileReceiver implements Consumer<ByteBuffer> {

    private static final NetService netService = NetServiceProvider.NET_SERVICE_INSTANCE;
    public static boolean FileReceiverInitialized = false;
/*
    static {
        System.out.println("FILE_TRANSFER FILE_TRANSFER FILE_TRANSFER FILE_TRANSFER");
        netService.registerTagMessageListener(
            FILE_TRANSFER,
            (msg, ch) -> {
                System.out.println(String.format("registerTagMessageListener FILE_TRANSFER path=[%s]", PrimitiveCodec.readString(msg.content())));
                try {
                    ((Channel) ch).directListener(
                        new FileReceiver(Paths.get(PrimitiveCodec.readString(msg.content())), (Channel) ch)
                    );
                } catch (Exception e) {
                    log.info("FileReceiver::registerTagMessageListener Exception {}", e.toString(), e);
                    System.out.println(String.format("FileReceiver::registerTagMessageListener Exception %s", e.toString()));
                    throw new RuntimeException(e);
                }
            }
        );
    }*/

    public static void init() {
        System.out.println("FILE_TRANSFER FILE_TRANSFER FILE_TRANSFER FILE_TRANSFER");
        if (FileReceiverInitialized) {
            return;
        }

        System.out.println("22222 FILE_TRANSFER FILE_TRANSFER FILE_TRANSFER FILE_TRANSFER");
        FileReceiverInitialized = true;

        netService.registerTagMessageListener(
            FILE_TRANSFER,
            (msg, ch) -> {
                System.out.println(String.format("registerTagMessageListener FILE_TRANSFER path=[%s]", PrimitiveCodec.readString(msg.content())));
                try {
                    ((Channel) ch).directListener(
                        new FileReceiver(Paths.get(PrimitiveCodec.readString(msg.content())), (Channel) ch)
                    );
                } catch (Exception e) {
                    log.info("FileReceiver::registerTagMessageListener Exception {}", e.toString(), e);
                    System.out.println(String.format("FileReceiver::registerTagMessageListener Exception %s", e.toString()));
                    throw new RuntimeException(e);
                }
            }
        );
    }

    private final FileChannel fileChannel;
    private final Channel channel;

    public FileReceiver(Path path, Channel channel) throws Exception {
        System.out.println(String.format("FileReceiver::FileReceiver Path=[%s] Parent=[%s]", path.toString(), path.getParent()));
        log.info(String.format("FileReceiver::FileReceiver Path=[%s] Parent=[%s]", path.toString(), path.getParent().toString()));
        Files.deleteIfExists(path);
        Files.createDirectories(path.getParent());
        this.fileChannel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        this.channel = channel;
        channel.setCloseListener(wrap(ch -> {
            fileChannel.close();
        }));
    }

    @Override
    public void accept(ByteBuffer buffer) {
        log.info("FileReceiver::accept");
        try {
            if (!buffer.hasRemaining()) {
                channel.send(Message.EMPTY);
                channel.close();
            } else {
                this.fileChannel.write(buffer);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
