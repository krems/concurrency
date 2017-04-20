package ru.mipt.dpqe.queue;

public interface Queue<T> {
    boolean enqueue(T e);
    T dequeue();
    boolean isEmpty();
}
