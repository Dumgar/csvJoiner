package org.dumgar.csvJoiner.joinerSortJoin;

import java.io.*;
import java.util.*;

public class SortStorage<T> implements SortStorageT<T>{
    private final File file;

    public SortStorage(List<T> objects) throws IOException {
        file = File.createTempFile("FileSort", "dat");
        setObjects(objects);
    }
    public void setObjects(List<T> objects) throws IOException {
        ObjectOutputStream wr = new ObjectOutputStream(new FileOutputStream(file));
        for (T item : objects) {
            wr.writeObject(item);
        }
        wr.close();
    }

    public Iterator<T> iterator() {
        try {
            return new Iterator<T>() {
                private ObjectInputStream fr =
                        new ObjectInputStream(new FileInputStream(file));
                T obj;

                public boolean hasNext() {
                    if (obj == null) {
                        try {
                            obj = (T) fr.readObject();
                        } catch (ClassNotFoundException e) {
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            obj = null;
                        }
                    }
                    return obj != null;
                }

                public T next() {
                    hasNext();
                    T res = obj;
                    obj = null;
                    return res;
                }

                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void finalize() {
        file.delete();
    }
}