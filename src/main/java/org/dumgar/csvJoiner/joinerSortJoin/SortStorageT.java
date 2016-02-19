package org.dumgar.csvJoiner.joinerSortJoin;

import java.util.List;
import java.io.IOException;

public interface SortStorageT<T> extends Iterable<T> {
    void setObjects(List<T> objects) throws IOException;
}