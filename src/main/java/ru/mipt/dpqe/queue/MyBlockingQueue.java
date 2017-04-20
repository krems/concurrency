package ru.mipt.dpqe.queue;


public class MyBlockingQueue<T> implements Queue<T> {
    private final Object monitor = new Object();
    private Node<T> head;
    private Node<T> tail;

    public boolean enqueue(T e) {
        Node<T> node = new Node<>(e);
        synchronized (monitor) {
            if (tail == null) {
                tail = node;
                head = node;
                monitor.notifyAll();
                return true;
            }
            tail.next = node;
            tail = node;
        }
        return true;
    }

    public T dequeue() {
        Node<T> headNode;
        synchronized (monitor) {
            if (blockIfEmpty()) {
                return null;
            }
            headNode = head;
            if (head == tail) {
                tail = null;
            }
            head = head.next;
        }
        return headNode.value;
    }

    @Override
    public boolean isEmpty() {
        synchronized (monitor) {
            return head == null;
        }
    }

    private boolean blockIfEmpty() {
        while (head == null) {
            try {
                monitor.wait();
            } catch (InterruptedException e) {
                return true;
            }
        }
        return false;
    }

    private static class Node<T> {
        Node<T> next;
        final T value;

        private Node(T value) {
            this.value = value;
        }
    }
}
