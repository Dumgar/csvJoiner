package org.dumgar.csvJoiner.joinerSortJoin;

import java.io.*;
import java.util.*;

public class ExternalSort<T extends Comparable<T>> implements Iterable<T> {
    private int bufferSize = 10000;
    private List<SortStorage> partFiles = new LinkedList<>();
    private Iterator<T> source;
    private List<T> part = new LinkedList<>();

    public ExternalSort() {
    }
    public ExternalSort(Iterator<T> newSource) {
        setSource(newSource);
    }
    public ExternalSort(Iterator<T> newSource, Integer newSize) {
        this(newSource);
        setBufferSize(newSize);
    }
    public void setBufferSize(int newSize) {
        bufferSize = newSize;
    }
    public void setSource(Iterator<T> newSource) {
        source = newSource;
        try {
            sortParts();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Collections.sort(part);
    }
    public Iterator<T> iterator() {
        if (partFiles.size() == 0) {
            // маленькая оптимизация, если всё уместилось в память
            return part.iterator();
        }
        return new Iterator<T>() {
            Long t = 0L;
            List<T> items = new ArrayList<T>();
            List<Iterator<T>> iterators = new ArrayList<Iterator<T>>();
            Integer minIdx = null;
            {
                iterators.add(part.iterator());
                for (SortStorage f : partFiles) {
                    iterators.add(f.iterator());
                }
                for (Iterator<T> item : iterators) {
                    if (item.hasNext()) {
                        items.add(item.next());
                    } else {
                        throw new RuntimeException("failed to get first for iterator");
                    }
                }
            }

            public boolean hasNext() {
                if (minIdx == null) {
                    for (int i = 0; i < items.size(); i++) {
                        if (items.get(i) != null &&
                                (minIdx == null ||
                                        items.get(i).compareTo(items.get(minIdx)) < 0)) {
                            minIdx = i;
                        }
                    }
                }
                return minIdx != null;
            }
            public T next() {
                T res = null;
                if (hasNext()) {
                    res = items.get(minIdx);
                    if (iterators.get(minIdx).hasNext()) {
                        items.set(minIdx, iterators.get(minIdx).next());
                    } else {
                        items.set(minIdx, null);
                    }
                }
                minIdx = null;
                return res;
            }
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }
    void sortParts() throws IOException {
        while (source.hasNext()) {
            part.add((T)source.next());
            if (part.size() >= bufferSize && source.hasNext()) {
                Collections.sort(part);
                partFiles.add(new SortStorage(part));
                part.clear();
            }
        }
    }
}