package demo.reactor.ingester;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.integration.handler.AbstractMessageHandler;
import org.springframework.messaging.Message;
import reactor.Environment;
import reactor.function.Consumer;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;


/**
 * @author swilliams
 */
public class AdaptiveBatcher extends AbstractMessageHandler {

    private static final Long SIGNAL = 0L;

    private final Consumer<byte[]> consumer;

    private final Broadcaster<byte[]> broadcaster;

    private final Broadcaster<Long> adaptive;

    @Value("${batch.size:1024}")
    long maxBatchSize;

    @Value("${batch.timespan:1000L}")
    long maxTimespan;

    /**
     * @param environment to use
     */
    public AdaptiveBatcher(Environment environment, Consumer<byte[]> consumer) {

        this.broadcaster = Broadcaster.create(environment);
        this.adaptive = Broadcaster.create(environment);

        this.consumer = consumer;
    }

    @Override
    protected void onInit() throws Exception {

        broadcaster
                .buffer(() -> Streams.timer(maxTimespan).mergeWith(adaptive))
                .capacity(maxBatchSize)
                .batchConsume(list -> {

                    // example of recombining byte arrays
                    int size = 0;
                    for (byte[] item : list) {
                        size += item.length;
                    }

                    byte[] data = new byte[size];
                    int position = 0;
                    for (byte[] item : list) {
                        System.arraycopy(item, 0, data, position, item.length);
                        position += item.length;
                    }

                    // TODO record time
                    consumer.accept(data);

                }, n -> {
                    adaptive.onNext(SIGNAL);
                    // TODO calculate new batch size
                    return n;
                });

        // control.start();
    }

    @Override
    protected void handleMessageInternal(Message<?> message) throws Exception {
        byte[] bytes = message.getHeaders().get("bytes", byte[].class);
        broadcaster.onNext(bytes);
    }

}
