package com.twq.local;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public class BoundedPriorityQueue<A> implements Iterable<A>, Serializable {

    //基于PriorityQueue来实现我们的单机版的topN
    private PriorityQueue<A> underlying = null;
    private int maxSize; //topN中的N
    private Comparator<? super A> comparator; //元素比较器

    public BoundedPriorityQueue(int maxSize, Comparator<? super A> comparator) {
        underlying = new PriorityQueue<A>(maxSize, comparator);
        this.maxSize = maxSize;
        this.comparator = comparator;
    }

    private int size() {
        return underlying.size();
    }

    public void addAll(Iterable<A> iterable) {
        Iterator<A> iterator = iterable.iterator();
        while (iterator.hasNext()) {
            add(iterator.next());
        }
    }

    public BoundedPriorityQueue<A> add(A elem) {
        if (size() < maxSize) {
            underlying.offer(elem);
        } else {
            maybeReplaceLowest(elem);
        }
        return this;
    }

    private void maybeReplaceLowest(A elem) {
        A head = underlying.peek();
        if (head != null && comparator.compare(head, elem) < 0) {
            underlying.poll();
            underlying.offer(elem);
        }
    }

    @Override
    public String toString() {
        return underlying.toString();
    }


    @Override
    public Iterator<A> iterator() {
        return underlying.iterator();
    }
}
