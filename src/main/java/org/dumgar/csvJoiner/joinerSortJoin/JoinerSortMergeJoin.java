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


            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(result), "UTF-8"));

            Iterator<String> iterator1 = sort1.iterator();
            Iterator<String> iterator2 = sort2.iterator();

            String[] elem1 = iterator1.next().split(",");
            String[] elem2 = iterator2.next().split(",");

            x:
            while (iterator1.hasNext() || iterator2.hasNext()) {
                while (Integer.parseInt(elem1[0]) < Integer.parseInt(elem2[0])) {
                    elem1 = iterator1.next().split(",");
                }

                while (Integer.parseInt(elem1[0]) == Integer.parseInt(elem2[0])) {
                    out.write(String.format("%09d", Integer.parseInt(elem1[0])) + "," + elem1[1] + "," + elem2[1] + "\n");
                    if (iterator2.hasNext()) {
                        elem2 = iterator2.next().split(",");
                    } else break x;
                    if (Integer.parseInt(elem1[0]) == Integer.parseInt(elem2[0]) && !iterator2.hasNext()) break;

                }
                while (Integer.parseInt(elem1[0]) > Integer.parseInt(elem2[0])) {
                    if (iterator2.hasNext()) {
                        elem2 = iterator2.next().split(",");
                    } else break;
                }
                if (!iterator1.hasNext() && !iterator2.hasNext()) {
                    if (Integer.parseInt(elem1[0]) == Integer.parseInt(elem2[0])) out.write(String.format("%09d", Integer.parseInt(elem1[0])) + "," + elem1[1] + "," + elem2[1] + "\n");
                    break;
                }
            }


            out.close();


        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
