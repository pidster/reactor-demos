package demo.reactor.router;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import reactor.Environment;
import reactor.rx.Streams;
import reactor.rx.broadcast.Broadcaster;

import java.util.List;


/**
 * @author swilliams
 */
public class AdaptiveBatchingRouter<T> implements InitializingBean {

    private static final Long SIGNAL = 0L;

    private final Broadcaster<T> broadcaster;

    private final Broadcaster<Long> adaptive;

    @Value("${router.size:1024}")
    long maxBatchSize;

    @Value("${router.timespan:1000L}")
    long maxTimespan;

    /**
     * @param environment to use
     */
    public AdaptiveBatchingRouter(Environment environment) {
        this.broadcaster = Broadcaster.create(environment);

        // separate signal stream used to modify batch
        // size based on calculated latency
        this.adaptive = Broadcaster.create(environment);
    }

    @Override
    public void afterPropertiesSet() {

        broadcaster
                // a grouping key splits the stream
                .groupBy(this::groupBy)
                        .consume(grouped -> grouped
                                        // the grouped stream can then be batched
                                        // merge two signal streams to achieve timeout+batch size
                                        .buffer(() -> Streams.timer(maxTimespan).mergeWith(adaptive))
                                                // initial batch size
                                        .capacity(maxBatchSize)
                                        .batchConsume(list -> {

                                            // consume batch for key
                                            dispatch(grouped.key(), list);

                                        }, previous -> {
                                            // adapt batch size
                                            adaptive.onNext(SIGNAL);

                                            return previous;
                                        })
                        );
    }

    private String groupBy(Object obj) {
        // dynamic route assignment is possible
        return String.format("key-%0000000d", obj.hashCode() % 8192);
    }

    private void dispatch(String key, List<T> batch) {
        // TODO Use key to find target for batch
        // TODO track time of delegate, per key
    }

}
