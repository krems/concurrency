package ru.mipt.dpqe.list;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;


public class LockFreeOrderedList<K extends Comparable<K>, V> {
    private static final Object DUMMY_VALUE = new Object();
    private final Node<K> top = new Node<>(null, null);

    public V get(K key) {
        Objects.requireNonNull(key);
        Node<K> b = findFirstNodeBefore(key);
        Node<K> n = b.next.get();
        while (n != null && n.key.compareTo(key) < 0) {
            b = n;
            n = n.next.get();
        }
        if (n == null || n.key.compareTo(key) != 0) {
            return null; // no such key
        }
        Object v = n.value.get();
        if (v == null || v == n) {
            n.helpDelete(b, n.next.get());
            return null; // deleted
        }
        return (V) v;
    }

    public void put(K key) {
        doPut(key, DUMMY_VALUE);
    }

    public void put(K key, V value) {
        doPut(key, value);
    }

    private void doPut(K key, Object value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        Node<K> n = new Node<>(key, value);
        while (true) {
            Node<K> b = findFirstNodeBefore(key);
            Node<K> f = b.next.get();
            if (f != null) {
                if (f.value.get() == null) {
                    f.helpDelete(b, f.next.get());
                    continue; // f deleted
                }
                Object bv = b.value.get();
                if (bv == null || bv == b) {
                    continue; // b deleted
                }
                n.next.set(f);
            }
            if (b.next.compareAndSet(f, n)) {
                return; // success
            }
        }
    }

    public boolean remove(K key) {
        Objects.requireNonNull(key);
        while (true) {
            Node<K> b = findFirstNodeBefore(key);
            if (b.key == null) {
                return false; // empty list
            }
            Node<K> n = b.next.get();
            if (n == null) {
                return false; // already deleted
            }
            if (n.key.compareTo(key) > 0) {
                return false; // no such key
            }
            if (n.key.compareTo(key) != 0) {
                continue; // late read
            }
            Object bv = b.value.get();
            if (bv == null || bv == b) {
                continue; // b deleted
            }

            Object v = n.value.get();
            if (v == null) {
                n.helpDelete(b, n.next.get());
                continue; // n deleted, should delete another node with such key
            }
            if (!n.value.compareAndSet(v, null)) {
                continue; // retry
            }
            Node<K> f = n.next.get();
            if (!n.mark(f) || !b.next.compareAndSet(n, f)) {
                get(key); // gem! retry
            }
            return true; // success
        }
    }

    private Node<K> findFirstNodeBefore(K key) {
        Node<K> node = top.next.get();
        if (node == null) {
            return top;
        }
        Node<K> prev = this.top;
        while (node != null && key.compareTo(node.key) < 0) {
            prev = node;
            node = node.next.get();
        }
        return prev;
    }

    private static class Node<K> {
        final AtomicReference<Node<K>> next = new AtomicReference<>(null);
        K key;
        final AtomicReference<Object> value;

        private Node(K key, Object value) {
            this.key = key;
            this.value = new AtomicReference<>(value);
        }

        public Node(K key) {
            this.key = key;
            this.value = new AtomicReference<>(this);
        }

        public void helpDelete(Node<K> b, Node<K> f) {
            if (b.next.get() == this && next.get() == f) {
                if (f == null || f.value.get() == f) {
                    mark(f);
                    return;
                }
                b.next.compareAndSet(this, f.next.get());
            }
        }

        public boolean mark(Node<K> f) {
            Node<K> marker = new Node<>(key);
            marker.next.set(f);
            return next.compareAndSet(f, marker);
        }
    }
}
