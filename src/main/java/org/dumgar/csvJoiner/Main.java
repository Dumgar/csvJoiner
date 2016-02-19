package org.dumgar.csvJoiner;
public class Main {

    public static void main(String[] args) {
//        Joiner joiner = new JoinerH2(System.getProperty("user.dir"));
//        joiner.innerJoinOnId("input_A.csv", "input_B.csv", "result.csv");

        Joiner joiner = new JoinerFor();
        joiner.innerJoinOnId("input_A.csv", "input_B.csv", "result.csv");
    }


}
