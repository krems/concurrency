package ru.mipt.dpqe.queue;


import java.util.concurrent.TimeUnit;

public class MyDisruptorQueue<T> implements Queue<T> {
    private final Object[] buffer;
    private final int capacity;
    private volatile long head = 0;
    private volatile long tail = 0;

    public MyDisruptorQueue(int capacity) {
        this.buffer = new Object[capacity];
        this.capacity = capacity;
    }

    @Override
    public boolean enqueue(T e) {
        int index = index(tail);
        while (tail == head + capacity) {
            backoff();
        }
        buffer[index] = e;
        tail++;
        return true;
    }

    @Override
    public T dequeue() {
        while (head == tail) {
            backoff();
        }
        int index = index(head);
        T e = (T) buffer[index];
        buffer[index] = null;
        head++;
        return e;
    }

    @Override
    public boolean isEmpty() {
        return head == tail;
    }

    private int index(long pointer) {
        return (int) pointer % capacity;
    }

    private void backoff() {
        try {
            TimeUnit.MILLISECONDS.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
//        Thread.yield();
    }
}
