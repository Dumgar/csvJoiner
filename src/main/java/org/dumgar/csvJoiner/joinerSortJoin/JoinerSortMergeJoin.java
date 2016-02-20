package org.dumgar.csvJoiner.joinerSortJoin;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.dumgar.csvJoiner.Joiner;

import java.io.*;
import java.util.Iterator;

public class JoinerSortMergeJoin implements Joiner {


    @Override
    public void innerJoinOnId(String filename1, String filename2, String result) {


        try {
            File file1 = new File(filename1);
            File file2 = new File(filename2);
            LineIterator iter1 = FileUtils.lineIterator(file1, "UTF-8");
            LineIterator iter2 = FileUtils.lineIterator(file2, "UTF-8");


            ExternalSort<String> sort1 = new ExternalSort<>(iter1);
            ExternalSort<String> sort2 = new ExternalSort<>(iter2);


            PrintWriter out = new PrintWriter(new File(result), "UTF-8");

            Iterator<String> iterator1 = sort1.iterator();
            Iterator<String> iterator2 = sort2.iterator();


            Item elem1 = new Item(iterator1.next());
            Item elem2 = new Item(iterator2.next());

            while (iterator1.hasNext() || iterator2.hasNext()) {
                if (elem1.key < elem2.key && iterator1.hasNext()) {
                    elem1 = new Item(iterator1.next());
                }
                if (elem1.key == elem2.key) {
                    out.write(String.format("%09d,%s,%s\n", elem1.key, elem1.value, elem2.value));
                    if (iterator2.hasNext()){
                        elem2 = new Item(iterator2.next());
                    } else break;
                }
                if (elem1.key > elem2.key && iterator2.hasNext()) {
                    elem2 = new Item(iterator2.next());
                }
                if (elem1.key == elem2.key) {
                    out.write(String.format("%09d,%s,%s\n", elem1.key, elem1.value, elem2.value));
                    if (iterator2.hasNext()){
                        elem2 = new Item(iterator2.next());
                    } else break;
                }

            }


            out.close();


        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private class Item {

        Integer key;
        String value;

        public Item(String elem) {
            this.key = Integer.parseInt(elem.split(",")[0]);
            this.value = elem.split(",")[1];
        }


    }
}
