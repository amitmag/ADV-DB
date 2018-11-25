import oracle.jdbc.OracleTypes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Assignment {
    Connection conn;
    final String username;
    final String password;
    final String connectionURL;
    private final String driver = "oracle.jdbc.driver.OracleDriver";

    //1.2.1a
    public Assignment(String connectionURL, String username, String password) {
        conn = null;
        this.connectionURL = connectionURL;
        this.username = username;
        this.password = password;
        this.Connect();
    }

    //1.2.1b
    public void Connect() {
        try {
            Class.forName(this.driver);
            conn = DriverManager.getConnection(this.connectionURL, this.username, this.password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //1.2.2
    public void fileToDataBase(String file_path) {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(file_path));
            String newLine;
            while ((newLine = br.readLine()) != null) {
                String[] lineValues = newLine.split(",");
                String title = lineValues[0];
                String prodYear = lineValues[1];
                insertToDataBase(title, prodYear);
            }
            br.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private void insertToDataBase(String title, String prodYear) {
        if (conn == null)
            this.Connect();
        String query = "INSERT INTO MEDIAITEMS (TITLE,PROD_YEAR) VALUES(?,?)";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setString(1, title);
            pstmt.setString(2, prodYear);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    //1.2.3
    public void calculateSimilarity() {
        int maxDis;
        maxDis = findMaxDis();
        List<Integer> MIDList = createIndexList();
        for (int i = 0; i < MIDList.size() - 1; i++) {
            for (int j = i + 1; j < MIDList.size(); j++) {
                float sim = calcSim(MIDList.get(i), MIDList.get(j), maxDis);
                addToSimTable(MIDList.get(i), MIDList.get(j), sim);
            }
        }
    }

    private void addToSimTable(Integer mid1, Integer mid2, float sim) {
        if (conn == null)
            this.Connect();
        String query = "INSERT INTO Similarity (MID1,MID2,SIMILARITY ) VALUES(?,?,?)";
        try (PreparedStatement pstmt = conn.prepareStatement(query)) {
            pstmt.setInt(1, mid1);
            pstmt.setInt(2, mid2);
            pstmt.setFloat(3, sim);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private float calcSim(Integer mid1, Integer mid2, int maxDis) {
        if (conn == null)
            this.Connect();
        float sim = 0;
        try (CallableStatement cstmt = conn.prepareCall("{? =call SimCalculation (?,?,?)}")) {
            cstmt.registerOutParameter(1, OracleTypes.FLOAT);
            cstmt.setInt(2, mid1);
            cstmt.setInt(3, mid2);
            cstmt.setInt(4, maxDis);
            cstmt.executeUpdate();
            sim = cstmt.getFloat(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return sim;
    }


    private List<Integer> createIndexList() {
        List<Integer> ans = new ArrayList<>();
        if (this.conn == null) {
            Connect();
        }
        String query = "SELECT * FROM MEDIAITEMS ORDER BY MID ASC"; //query
        try (PreparedStatement ps = conn.prepareStatement(query)) {//compiling query in the DB
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                ans.add(rs.getInt("MID"));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ans;
    }

    private int findMaxDis() {
        if (conn == null)
            this.Connect();
        int maxDis = 0;
        try (CallableStatement cstmt = conn.prepareCall("{? = call MaximalDistance ()}")) {
            cstmt.registerOutParameter(1, OracleTypes.NUMBER);
            cstmt.executeUpdate();
            maxDis = cstmt.getInt(1);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return maxDis;
    }

    //1.2.4
    public void printSimilarItems(long MID) {
        if (this.conn == null) {
            Connect();
        }
        String query = " SELECT TITLE,SIMILARITY FROM (SELECT TITLE,SIMILARITY FROM SIMILARITY JOIN MediaItems ON SIMILARITY.MID2 = MediaItems.MID" +
                " WHERE (MID1 = ?) AND SIMILARITY > 0.3)UNION  (SELECT TITLE,SIMILARITY FROM SIMILARITY JOIN MediaItems ON SIMILARITY.MID1 = MediaItems.MID" +
                " WHERE (MID2 = ?) AND SIMILARITY > 0.3) ORDER BY SIMILARITY ASC"; //query
        try (PreparedStatement ps = conn.prepareStatement(query)) {//compiling query in the DB
            ps.setLong(1, MID);
            ps.setLong(2, MID);
            ResultSet rs = ps.executeQuery();
            System.out.println(String.format("the similar titles for %d are:", MID));
            while (rs.next()) {
                System.out.println(String.format("Title: %s. \t sim: %f", rs.getString("TITLE"), rs.getFloat("SIMILARITY")));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void Disconnect() {
        try {
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
