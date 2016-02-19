package org.dumgar.csvJoiner;

import java.io.*;

/**
 * Created by romandmitriev on 19.02.16.
 */
public class JoinerFor implements Joiner{

    @Override
    public void innerJoinOnId(String filename1, String filename2, String result) {

        BufferedReader br1;
        BufferedReader br2;
        String line1;
        String line2;
        String cvsSplitBy = ",";
        int key1;
        String value1;
        int key2;
        String value2;


        try {
            br1 = new BufferedReader(new FileReader(filename1));
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(result), "UTF-8"));
            while ((line1 = br1.readLine()) != null) {

                String[] both1 = line1.split(cvsSplitBy);
                key1 = Integer.parseInt(both1[0]);
                value1 = both1[1];

                br2 = new BufferedReader(new FileReader(filename2));
                while ((line2 = br2.readLine()) != null) {

                    String[] both2 = line2.split(cvsSplitBy);
                    key2 = Integer.parseInt(both2[0]);
                    value2 = both2[1];

                    if (key1 == key2 ) {

                        out.write(key1 + "," + value1 + "," + value2 + "\n");

                    }

                }

            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
