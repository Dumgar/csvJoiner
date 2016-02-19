package org.dumgar.csvJoiner;

import java.io.*;

public class JoinerFor implements Joiner{

    @Override
    public void innerJoinOnId(String filename1, String filename2, String result) {

        BufferedReader br1;
        BufferedReader br2;
        String line1;
        String line2;
        String cvsSplitBy = ",";
        String temp;

        try {
            br1 = new BufferedReader(new FileReader(filename1));
            BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(result), "UTF-8"));
            while ((line1 = br1.readLine()) != null) {

                String[] both1 = line1.split(cvsSplitBy);

                br2 = new BufferedReader(new FileReader(filename2));
                while ((line2 = br2.readLine()) != null) {

                    String[] both2 = line2.split(cvsSplitBy);

                    if (Integer.parseInt(both1[0]) == Integer.parseInt(both2[0]) ) {

                        temp = String.format("%09d", Integer.parseInt(both1[0]));

                        out.write(temp + "," + both1[1] + "," + both2[1] + "\n");

                    }

                }

            }
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
