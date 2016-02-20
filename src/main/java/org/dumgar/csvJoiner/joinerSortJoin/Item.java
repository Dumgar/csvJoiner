package org.dumgar.csvJoiner.joinerSortJoin;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by romandmitriev on 20.02.16.
 */
public class Item {

    Integer key;
    String value;

    public Item(String elem) {
        this.key = Integer.parseInt(elem.split(",")[0]);
        this.value = elem.split(",")[1];
    }


}
