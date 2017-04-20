package ru.mipt.dpqe.queue;


import java.sql.Time;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class MSQueue<T> implements Queue<T> {
    private final AtomicReference<Node<T>> head;
    private final AtomicReference<Node<T>> tail;

    public MSQueue() {
        Node<T> dummy = new Node<>(null);
        head = new AtomicReference<>(dummy);
        tail = new AtomicReference<>(dummy);
    }

    public boolean enqueue(T e) {
        Node<T> node = new Node<>(e);
        Node<T> t;
        while (true) {
            t = tail.get();
            Node<T> tNext = t.next.get();
            if (tNext != null) {
                tail.compareAndSet(t, tNext);
                continue;
            }

            if (t.next.compareAndSet(null, node)) {
                break;
            }
            backoff();
        }
        tail.compareAndSet(t, node);
        return true;
    }

    public T dequeue() {
        Node<T> next;
        while (true) {
            Node<T> h = head.get();
            next = h.next.get();
            if (next == null) {
                return null;
            }

            Node<T> t = tail.get();
            if (h == t) {
                tail.compareAndSet(t, next);
                continue;
            }

            if (head.compareAndSet(h, next)) {
                break;
            }
            backoff();
        }
        T retValue = next.value;
        next.value = null;
        return retValue;
    }

    private void backoff() {
        Thread.yield();
    }

    @Override
    public boolean isEmpty() {
        return head.get().next.get() == null;
    }

    private static class Node<T> {
        final AtomicReference<Node<T>> next = new AtomicReference<>(null);
        T value;

        private Node(T value) {
            this.value = value;
        }
    }
}
