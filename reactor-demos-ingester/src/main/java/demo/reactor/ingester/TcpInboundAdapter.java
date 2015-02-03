package demo.reactor.ingester;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import reactor.Environment;
import reactor.io.buffer.Buffer;
import reactor.io.codec.Codec;
import reactor.io.net.netty.tcp.NettyTcpServer;
import reactor.io.net.tcp.TcpServer;
import reactor.io.net.tcp.spec.TcpServerSpec;


/**
 * @author pidster
 */
public class TcpInboundAdapter extends MessageProducerSupport {

    private final Environment environment;

    private final Codec<Buffer, byte[], byte[]> codec;

    // --------------------------------------------------

    private TcpServer<byte[], byte[]> tcpServer;

    // --------------------------------------------------

    @Value("${tcp.host:127.0.0.1}")
    String host;

    @Value("${tcp.port:40070}")
    int port;

    /**
     * @param environment of reactor
     * @param codec for decoding
     * @param channel to dispatch to
     */
    public TcpInboundAdapter(Environment environment, Codec<Buffer, byte[], byte[]> codec, MessageChannel channel) {
        super();
        this.environment = environment;
        this.codec = codec;

        setOutputChannel(channel);
    }

    /**
     * @throws Exception
     */
    protected void onInit() {

        // set up the tcp server
        this.tcpServer = new TcpServerSpec<byte[], byte[]>(NettyTcpServer.class)
                .env(environment)
                .codec(codec)
                .listen(host, port)
                .get();

        // configure the stream codec & SI message channel
        tcpServer.consume(connection ->

                connection.in().consume(bytes -> {

                    // build a message
                    Message<?> message = super.getMessageBuilderFactory()
                            .withPayload(bytes)
                            .setHeader("bytes", bytes)
                            .build();

                    // and send the message...
                    super.sendMessage(message);
                }));
    }

    @Override
    protected void doStart() {
        try {
            tcpServer.start().await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void doStop() {
        try {
            tcpServer.shutdown().await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
