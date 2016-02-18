package org.dumgar.csvJoiner;
import org.h2.tools.Csv;
import org.h2.tools.DeleteDbFiles;
import java.sql.*;

class JoinerH2 extends JoinerSQL{
    private static int tableCount = 0;
    private static final String CREATE_QUERY =
            "CREATE TABLE %s(ID CHAR(9) , NAME CHAR(15)) AS SELECT * FROM CSVREAD('%s', '1, 2');";
    private Connection connection = null;

    public JoinerH2(String url) {
        super(url);
    }

    @Override
    protected Connection createConnection(String url) throws SQLException {
        if (connection == null) {
            try {
                Class.forName("org.h2.Driver").newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            this.connection = DriverManager.getConnection("jdbc:h2:" + url + "/h2", "", "");
        }
        return this.connection;
    }

    @Override
    protected String readTableFromFile(String filename) {

        try (PreparedStatement st = connection.prepareStatement(String.format(CREATE_QUERY, "table" + tableCount, filename))) {
            st.execute();
            return "table" + tableCount++;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected void writeTableToFile(String filename, ResultSet res) {

        Csv file = new Csv();
        file.setFieldDelimiter((char) 00);
        file.setWriteColumnHeader(false);
        try {
            file.write(filename, res, "UTF-8");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Override
    protected void clear() {
        DeleteDbFiles.execute(url, null, true);
    }
}