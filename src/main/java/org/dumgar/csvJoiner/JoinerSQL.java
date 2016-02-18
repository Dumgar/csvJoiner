package org.dumgar.csvJoiner;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

abstract class JoinerSQL implements Joiner {
    private static final String JOIN_QUERY = "SELECT %s.ID, %s.NAME AS AName, %s.NAME AS Bname \n" +
            "  FROM %s \n" +
            "    INNER JOIN %s ON %s.ID = %s.ID;";
    protected final String url;

    public JoinerSQL(String url) {
        this.url = url;
    }

    public void innerJoinOnId(String filename1, String filename2, String result) {

        try (Connection connection = this.createConnection(url)) {
            String table1 = this.readTableFromFile(filename1);
            String table2 = this.readTableFromFile(filename2);

            String query = String.format(JOIN_QUERY, table1, table1, table2, table1, table2, table1, table2);
            try (PreparedStatement st = connection.prepareStatement(query);
            ResultSet res = st.executeQuery()) {
                this.writeTableToFile(result, res);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        this.clear();

    }

    abstract protected Connection createConnection(String url) throws SQLException;

    abstract protected String readTableFromFile(String filename);

    abstract protected void writeTableToFile(String filename, ResultSet res);

    protected void clear() {

    }
}