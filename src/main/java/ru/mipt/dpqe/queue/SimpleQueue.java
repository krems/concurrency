package ru.mipt.dpqe.queue;

public class SimpleQueue<T> implements Queue<T> {
    private Node<T> head;
    private Node<T> tail;

    public boolean enqueue(T e) {
        Node<T> node = new Node<>(e);
        if (tail == null) {
            tail = node;
            head = node;
            return true;
        }
        tail.next = node;
        tail = node;
        return true;
    }

    public T dequeue() {
        if (head == null) {
            return null;
        }
        Node<T> headNode = this.head;
        if (head == tail) {
            tail = null;
        }
        head = head.next;
        return headNode.value;
    }

    @Override
    public boolean isEmpty() {
        return head == null;
    }

    private static class Node<T> {
        Node<T> next;
        final T value;

        private Node(T value) {
            this.value = value;
        }
    }
}
