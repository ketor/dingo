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

package io.dingodb.net.netty;

import io.dingodb.common.Location;
import io.dingodb.common.util.Optional;
import io.dingodb.net.NetError;
import io.dingodb.net.api.ApiRegistry;
import io.dingodb.net.netty.api.ApiRegistryImpl;
import io.dingodb.net.service.FileTransferService;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.dingodb.common.concurrent.Executors.executor;
import static io.dingodb.common.util.Optional.ifPresent;

@Slf4j
public class NetService implements io.dingodb.net.NetService {

    @Getter
    private final Map<Integer, NettyServer> servers = new ConcurrentHashMap<>();
    @Getter
    private final String hostname = NetConfiguration.host();
    @Delegate
    private final TagRegistry tagRegistry = TagRegistry.INSTANCE;
    private final Map<Location, Connection> connections = new ConcurrentHashMap<>(8);

    protected NetService() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                close();
            } catch (Exception e) {
                log.error("Close connection error", e);
            }
        }));
    }

    @Override
    public void listenPort(int port) throws Exception {
        if (servers.containsKey(port)) {
            return;
        }
        NettyServer server = NettyServer.builder().port(port).build();
        server.start();
        servers.put(port, server);
        log.info("Start listening {}.", port);
        FileTransferService.getDefault();
    }

    @Override
    public void disconnect(Location location) {
        connections.remove(location).close();
    }

    @Override
    public void cancelPort(int port) throws Exception {
        ifPresent(servers.remove(port), NettyServer::close);
    }

    @Override
    public ApiRegistry apiRegistry() {
        return ApiRegistryImpl.instance();
    }

    @Override
    public Channel newChannel(Location location) {
        return newChannel(location, true);
    }

    @Override
    public Channel newChannel(Location location, boolean keepAlive) {
        Connection connection = connections.get(location);
        if (connection == null) {
            connection = connect(location);
        }
        return connection.newChannel();
    }

    @Override
    public void close() throws Exception {
        for (NettyServer server : servers.values()) {
            server.close();
        }
        connections.values().forEach(Connection::close);
    }

    private Connection connect(Location location) {
        return connections.computeIfAbsent(location, k -> {
            Optional<Connection> connection = Optional.empty();
            NioEventLoopGroup executor = new NioEventLoopGroup(0, executor(location.getUrl() + "/connection"));
            try {
                Bootstrap bootstrap = new Bootstrap();
                bootstrap
                    .channel(NioSocketChannel.class)
                    .group(executor)
                    .remoteAddress(location.toSocketAddress())
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            connection.ifAbsentSet(new Connection("client", location, ch, true));
                            NettyHandlers.initChannelPipeline(ch, connection.get());
                        }
                    });
                bootstrap.connect().sync().await();
                connection
                    .ifPresent(Connection::handshake).ifPresent(Connection::auth)
                    .orElseThrow(() -> new NullPointerException("connection"));
            } catch (InterruptedException e) {
                connection.ifPresent(Connection::close);
                executor.shutdownGracefully();
                NetError.OPEN_CONNECTION_INTERRUPT.throwFormatError(location);
            } catch (Exception e) {
                connection.ifPresent(Connection::close);
                executor.shutdownGracefully();
                throw e;
            }
            connection.get().addCloseListener(__ -> executor.shutdownGracefully());
            connection.ifPresent(__ -> __.addCloseListener(___ -> connections.remove(location, __)));
            return connection.get();
        });
    }


}
