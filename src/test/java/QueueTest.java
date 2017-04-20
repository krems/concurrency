import org.junit.Test;
import ru.mipt.dpqe.queue.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;


public class QueueTest {

    private static Queue<Integer> createBlockingQueue() {
//        return new MyBlockingQueue<>();
        return new MyDisruptorQueue<>(10000);
    }

    private static Queue<Integer> createNonBlockingQueue() {
        return new MSQueue<>();
//        return new SimpleConcurrentQueue<>();
//        return new SimpleQueue<>();
    }

    private static Queue<Integer> createQueue() {
        return createNonBlockingQueue();
    }

    @Test
    public void testEmptyBlockingQueueShouldBlock_whenDequeueIsCalled() {
        Thread thread = Thread.currentThread();
        Executors.newSingleThreadScheduledExecutor().schedule(thread::interrupt, 2, TimeUnit.SECONDS);
        Queue<Integer> q = createBlockingQueue();
        assertEquals(null, q.dequeue());
    }

    @Test
    public void testEmptyBlockingQueueShouldBlock_whenDequeueIsCalled2() {
        Queue<Integer> q = createBlockingQueue();
        Executors.newSingleThreadScheduledExecutor().schedule(() -> q.enqueue(2), 2, TimeUnit.SECONDS);
        assertEquals(2, (int) q.dequeue());
    }

    @Test
    public void testEmptyQueueShouldReturnNull() {
        Queue<Integer> q = createNonBlockingQueue();
        assertEquals(null, q.dequeue());
    }

    @Test
    public void testAddAndThenRemove_shouldReturnAllAddedElements_InOrderOfAdding() throws Exception {
        Queue<Integer> q = createQueue();
        int iterations = 10_000;
        for (int i = 0; i < iterations; i++) {
            q.enqueue(i);
        }
        for (int i = 0; i < iterations; i++) {
            assertEquals(i, (int) q.dequeue());
        }
    }

    @Test
    public void testSingleElement() throws Exception {
        Queue<Integer> q = createQueue();
        int x = 2;
        q.enqueue(x);
        int y = q.dequeue();
        assertEquals(x, y);
    }

    @Test
    public void testMixedAddAndThenRemove_shouldReturnAllAddedElements_InOrderOfAdding() throws Exception {
        Queue<Integer> q = createQueue();
        int iterations = 10_000;
        for (int i = 0; i < iterations; i++) {
            q.enqueue(i);
            assertEquals(i, (int) q.dequeue());
        }
    }

    @Test
    public void testMixedAddAndThenRemove_shouldReturnAllAddedElements_InOrderOfAdding2() throws Exception {
        Queue<Integer> q = createQueue();
        int iterations = 10_000;
        for (int i = 1; i < iterations; i++) {
            q.enqueue(i);
            if (i % 2 == 0) {
                assertEquals(i - 1, (int) q.dequeue());
                assertEquals(i, (int) q.dequeue());
            }
        }
    }

    @Test
    public void concurrentSingleProducerSingleConsumer() throws ExecutionException, InterruptedException, TimeoutException {
        Queue<Integer> queue = createQueue();
        runConcurrent(1, 1, queue);
    }

    @Test
    public void concurrentUniformLoadTest() throws ExecutionException, InterruptedException, TimeoutException {
        for (int i = 1; i < 10; i++) {
            Queue<Integer> queue = createQueue();
            runConcurrent(i * 10, i * 10, queue);
        }
    }

    @Test
    public void concurrentHighReadLoadTest() throws ExecutionException, InterruptedException, TimeoutException {
        for (int i = 1; i < 10; i++) {
            Queue<Integer> queue = createNonBlockingQueue();
            runConcurrent(i, i * 10, queue);
        }
    }

    @Test
    public void concurrentHighWrightLoadTest() throws ExecutionException, InterruptedException, TimeoutException {
        for (int i = 1; i < 10; i++) {
            Queue<Integer> queue = createQueue();
            runConcurrent(i * 10, i, queue);
        }
    }

    private static void runConcurrent(int writers, int readers, Queue<Integer> queue) throws ExecutionException, InterruptedException, TimeoutException {
        ExecutorService readersExecutor = Executors.newFixedThreadPool(readers);
        ExecutorService writersExecutor = Executors.newFixedThreadPool(writers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch stop = new CountDownLatch(0);
        prepareAdders(writers, writersExecutor, start, stop, queue);
        List<Future<List<Integer>>> res = prepareRemovers(readers, readersExecutor, start, queue);
        start.countDown();
        stop.await();
        List<Integer> results = new ArrayList<>();
        for (Future<List<Integer>> future : res) {
            results.addAll(future.get(2, TimeUnit.SECONDS));
        }
        Integer q;
        while (!queue.isEmpty()) {
            q = queue.dequeue();
            if (q == null) {
                break;
            }
            results.add(q);
        }
//        assertEquals("writers: " + writers + " readers: " + readers, 1000 * writers, results.size());
        for (int i = 0; i < 1000; i++) {
            final int j = i;
            assertEquals("writers: " + writers + " readers: " + readers,
                    writers, results.stream().filter((e) -> e != null && e.equals(j)).count());
        }
        readersExecutor.shutdownNow();
    }

    private static void prepareAdders(int addersNo, ExecutorService executor,
                                      CountDownLatch stop, CountDownLatch start,
                                      Queue<Integer> queue) {
        for (int i = 0; i < addersNo; i++) {
            executor.submit(new Adder(start, stop, queue, 1));
        }
    }

    private static List<Future<List<Integer>>> prepareRemovers(int removersNo, ExecutorService executor,
                                                               CountDownLatch start, Queue<Integer> queue) {
        List<Future<List<Integer>>> ret = new ArrayList<>();
        for (int i = 0; i < removersNo; i++) {
            ret.add(executor.submit(new Remover(start, queue)));
        }
        return ret;
    }

    private static class Adder implements Callable<Void> {
        private final CountDownLatch start;
        private final CountDownLatch stop;
        private final Queue<Integer> queue;
        private final int multiplier;

        private Adder(CountDownLatch l, CountDownLatch stop, Queue<Integer> q, int multiplier) {
            this.start = l;
            this.stop = stop;
            this.queue = q;
            this.multiplier = multiplier;
        }

        @Override
        public Void call() throws Exception {
            start.await();
            for (int i = 0; i < 1000; i++) {
                queue.enqueue(i);
            }
            stop.countDown();
            return null;
        }
    }

    private static class Remover implements Callable<List<Integer>> {
        private final CountDownLatch start;
        private final Queue<Integer> queue;
        private final List<Integer> removed = new ArrayList<>();

        private Remover(CountDownLatch l, Queue<Integer> q) {
            this.start = l;
            this.queue = q;
        }

        @Override
        public List<Integer> call() throws Exception {
            start.await();
            for (int i = 0; i < 1000; i++) {
                removed.add(queue.dequeue());
            }
            return removed;
        }
    }
}
