package com.dw.movie.Search;

import com.dw.movie.Entity.Movie;

import java.sql.*;
import java.util.ArrayList;

public class Search {

    private static String dbDriverName = "com.mysql.jdbc.Driver";
    private static String dbConn = "jdbc:mysql://127.0.0.1:3306/sys?user=root&password=123456";
    private static String dwDriverName = "com.cloudera.hive.jdbc4.HS2Driver";
    private static String dwConn = "jdbc:hive2://192.168.44.134:10000/default";

    public static Result searchByTime(String Year, String Month, String Day,String Season,String Week){

        ArrayList<Movie> movie = new ArrayList<Movie>();
        long dbstart = 0, dbend = 0, dwstart = 0, dwend = 0;
        int count = 0;

        // search in db
        try{
            Class.forName(dbDriverName).newInstance();

            Connection conn = DriverManager.getConnection(dbConn);

            if(conn!=null) {
                String attribute="movie_id,movie_name,mpaa_rating,description,runtime, average_rating,studio,rank,year,month,day";
                String table="Movie";
                String where="";

                if (Season.equals("")&&Week.equals("")) {
                    if (!Year.equals("")) {
                        if (where.equals("")) where = "year=?";
                        else where = where + " AND year=?";
                    }
                    if (!Month.equals("")) {
                        if (where.equals("")) where = "month=?";
                        else where = where + " AND month=?";
                    }
                    if (!Day.equals("")) {
                        if (where.equals("")) where = "day=?";
                        else where = where + " AND day=?";
                    }
                    String sql="SELECT * FROM Movie WHERE "+where;
                    PreparedStatement stmt = conn.prepareStatement(sql);
                    int num=0;

                    if (!Year.equals("")){
                        ++num;
                        stmt.setString(num,Year);
                    }
                    if (!Month.equals("")){
                        ++num;
                        stmt.setString(num,Month);
                    }
                    if (!Day.equals("")){
                        ++num;
                        stmt.setString(num,Day);
                    }
                    // execute the query & calculate the time
                    dbstart = System.nanoTime();
                    ResultSet rs = stmt.executeQuery();
                    dbend = System.nanoTime();

                    while(rs.next()) {
                        Movie m=new Movie();
                        m.setMovie_id(rs.getString("movie_id"));
                        m.setYear(rs.getString("year"));
                        m.setMonth(rs.getString("month"));
                        m.setDay(rs.getString("day"));
                        m.setMovie_name(rs.getString("movie_name"));
                        m.setMpaa_rating(rs.getString("mpaa_rating"));
                        m.setDescription(rs.getString("description"));
                        m.setRuntime(rs.getString("runtime"));
                        m.setAverage_rating(rs.getString("average_rating"));
                        m.setStudio(rs.getString("studio"));
                        m.setRank(rs.getString("rank"));
                        movie.add(m);
                    }

                    rs.close();
                    stmt.close();
                }
                else {
                    if (!Year.equals("")) {
                        if (where.equals("")) where = "year=?";
                        else where = where + " AND year=?";
                    }
                    if (!Month.equals("")) {
                        if (where.equals("")) where = "month=?";
                        else where = where + " AND month=?";
                    }
                    if (!Day.equals("")) {
                        if (where.equals("")) where = "day=?";
                        else where = where + " AND day=?";
                    }
                    if (!Season.equals("")) {
                        if (where.equals("")) where = "season=?";
                        else where = where + " AND season=?";
                    }
                    if (!Week.equals("")) {
                        if (where.equals("")) where = "week=?";
                        else where = where + " AND week=?";
                    }
                    String sql="SELECT year,month,day FROM Time WHERE "+where;
                    int num=0;
                    PreparedStatement stmt = conn.prepareStatement(sql);
                    if (!Year.equals("")){
                        ++num;
                        stmt.setString(num,Year);
                    }
                    if (!Month.equals("")){
                        ++num;
                        stmt.setString(num,Month);
                    }
                    if (!Day.equals("")){
                        ++num;
                        stmt.setString(num,Day);
                    }
                    if (!Season.equals("")){
                        ++num;
                        stmt.setString(num,Season);
                    }
                    if (!Week.equals("")){
                        ++num;
                        stmt.setString(num,Week);
                    }
                    // execute the query & calculate the time
                    dbstart = System.nanoTime();
                    ResultSet rs = stmt.executeQuery();
                    while (rs.next()) {
                        String sql2 = "SELECT * FROM Movie WHERE year = \'" + rs.getString("year") + "\' AND month = \'" + rs.getString("month") + "\' AND day = \'" + rs.getString("day") + "\'";
                        Statement stmt2 = conn.createStatement();
                        ResultSet rs2 = stmt2.executeQuery(sql2);
                        while (rs2.next()) {
                            Movie m = new Movie();
                            m.setMovie_id(rs.getString("movie_id"));
                            m.setYear(rs.getString("year"));
                            m.setMonth(rs.getString("month"));
                            m.setDay(rs.getString("day"));
                            m.setMovie_name(rs.getString("movie_name"));
                            m.setMpaa_rating(rs.getString("mpaa_rating"));
                            m.setDescription(rs.getString("description"));
                            m.setRuntime(rs.getString("runtime"));
                            m.setAverage_rating(rs.getString("average_rating"));
                            m.setStudio(rs.getString("studio"));
                            m.setRank(rs.getString("rank"));
                            movie.add(m);
                        }
                        rs2.close();
                        stmt2.close();
                    }
                    dbend = System.nanoTime();
                    rs.close();
                    stmt.close();
                }


                conn.close();
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e){
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //search in dw
        try {
            Class.forName(dwDriverName);
            Connection conn = DriverManager.getConnection(dwConn);

            if(conn != null){
                String attribute="movie_id,movie_name,mpaa_rating,description,runtime, average_rating,studio,rank,year,month,day";
                String table="Movie";
                String where="";

                if (Season.equals("")&&Week.equals("")) {
                    if (!Year.equals("")) {
                        if (where.equals("")) where = "year=?";
                        else where = where + " AND year=?";
                    }
                    if (!Month.equals("")) {
                        if (where.equals("")) where = "month=?";
                        else where = where + " AND month=?";
                    }
                    if (!Day.equals("")) {
                        if (where.equals("")) where = "day=?";
                        else where = where + " AND day=?";
                    }
                    String sql="SELECT * FROM Movie WHERE "+where;
                    PreparedStatement stmt = conn.prepareStatement(sql);
                    int num=0;

                    if (!Year.equals("")){
                        ++num;
                        stmt.setString(num,Year);
                    }
                    if (!Month.equals("")){
                        ++num;
                        stmt.setString(num,Month);
                    }
                    if (!Day.equals("")){
                        ++num;
                        stmt.setString(num,Day);
                    }
                    // execute the query & calculate the time
                    dbstart = System.nanoTime();
                    ResultSet rs = stmt.executeQuery();
                    dbend = System.nanoTime();

                    rs.close();
                    stmt.close();
                }
                else {
                    if (!Year.equals("")) {
                        if (where.equals("")) where = "year=?";
                        else where = where + " AND year=?";
                    }
                    if (!Month.equals("")) {
                        if (where.equals("")) where = "month=?";
                        else where = where + " AND month=?";
                    }
                    if (!Day.equals("")) {
                        if (where.equals("")) where = "day=?";
                        else where = where + " AND day=?";
                    }
                    if (!Season.equals("")) {
                        if (where.equals("")) where = "season=?";
                        else where = where + " AND season=?";
                    }
                    if (!Week.equals("")) {
                        if (where.equals("")) where = "week=?";
                        else where = where + " AND week=?";
                    }
                    String sql="SELECT year,month,day FROM Time WHERE "+where;
                    int num=0;
                    PreparedStatement stmt = conn.prepareStatement(sql);
                    if (!Year.equals("")){
                        ++num;
                        stmt.setString(num,Year);
                    }
                    if (!Month.equals("")){
                        ++num;
                        stmt.setString(num,Month);
                    }
                    if (!Day.equals("")){
                        ++num;
                        stmt.setString(num,Day);
                    }
                    if (!Season.equals("")){
                        ++num;
                        stmt.setString(num,Season);
                    }
                    if (!Week.equals("")){
                        ++num;
                        stmt.setString(num,Week);
                    }
                    // execute the query & calculate the time
                    dbstart = System.nanoTime();
                    ResultSet rs = stmt.executeQuery();
                    while (rs.next()) {
                        String sql2 = "SELECT * FROM Movie WHERE year = \'" + rs.getString("year") + "\' AND month = \'" + rs.getString("month") + "\' AND day = \'" + rs.getString("day") + "\'";
                        Statement stmt2 = conn.createStatement();
                        ResultSet rs2 = stmt2.executeQuery(sql2);
                        rs2.close();
                        stmt2.close();
                    }
                    dbend = System.nanoTime();
                    rs.close();
                    stmt.close();
                }
                conn.close();
            }
        } catch (ClassNotFoundException | SQLException e){
            e.printStackTrace();
        }

        return new Result(dbend-dbstart,dwend-dwstart,count,movie);
    }

    public static Result searchByName(String MovieName) {

        ArrayList<Movie> movie = new ArrayList<Movie>();
        long dbstart = 0, dbend = 0, dwstart = 0, dwend = 0;
        int count = 0;

        // search in db
        try{
            Class.forName(dbDriverName).newInstance();

            Connection conn = DriverManager.getConnection(dbConn);

            if(conn!=null) {
                String sql = "SELECT * FROM Movie WHERE movie_name like ? ";
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1,"%"+MovieName+"%");


                // execute the query & calculate the time
                dbstart = System.nanoTime();
                ResultSet rs = stmt.executeQuery();
                dbend = System.nanoTime();

                while(rs.next()) {
//                    count += rs.getInt("Count");
//                    Statement stmt2 = conn.createStatement();
//                    String sql2 = "SELECT NameProductId FROM name_movie_list WHERE NameId = \'" + rs.getString("NameId") + "\'";
//                    ResultSet rs2 = stmt2.executeQuery(sql2);
//                    while(rs2.next()){
                    Movie m=new Movie();
                    m.setMovie_id(rs.getString("movie_id"));
                    m.setYear(rs.getString("year"));
                    m.setMonth(rs.getString("month"));
                    m.setDay(rs.getString("day"));
                    m.setMovie_name(rs.getString("movie_name"));
                    m.setMpaa_rating(rs.getString("mpaa_rating"));
                    m.setDescription(rs.getString("description"));
                    m.setRuntime(rs.getString("runtime"));
                    m.setAverage_rating(rs.getString("average_rating"));
                    m.setStudio(rs.getString("studio"));
                    m.setRank(rs.getString("rank"));
                    movie.add(m);
//                        movie.add(rs2.getString("NameProductId"));
//                    }
//                    rs2.close();
                }

                rs.close();
                stmt.close();
                conn.close();
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e){
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //search in dw
        try {
            Class.forName(dwDriverName);
            Connection conn = DriverManager.getConnection(dwConn);

            if(conn != null){
                String sql = "SELECT * FROM Movie WHERE movie_name like ?";
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1,"%"+MovieName+"%");

                // execute the query & calculate the time
                dwstart = System.nanoTime();
                ResultSet rs = stmt.executeQuery(sql);
                dwend = System.nanoTime();

                rs.close();
                stmt.close();
                conn.close();
            }
        } catch (ClassNotFoundException | SQLException e){
            e.printStackTrace();
        }

        return new Result(dbend-dbstart,dwend-dwstart,count,movie);

    }

    public static Result searchByDirector(String DirectorName) {

        ArrayList<Movie> movie = new ArrayList<Movie>();
        long dbstart = 0, dbend = 0, dwstart = 0, dwend = 0;
        int count = 0;

        // search in db
        try{
            Class.forName(dbDriverName).newInstance();

            Connection conn = DriverManager.getConnection(dbConn);

            if(conn!=null) {
                String sql = "SELECT director_id FROM Director WHERE director_name like ?";
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1,"%"+DirectorName+"%");

                // execute the query & calculate the time
                dbstart = System.nanoTime();
                ResultSet rs = stmt.executeQuery();

                while(rs.next()) {
//                    count += rs.getInt("Count");
                    Statement stmt2 = conn.createStatement();
                    String sql2 = "SELECT movie_id FROM Direct WHERE director_id = \'" + rs.getString("director_id") + "\'";
                    ResultSet rs2 = stmt2.executeQuery(sql2);
                    while(rs2.next()){
                        Statement stmt3 = conn.createStatement();
                        String sql3 = "SELECT * FROM Movie WHERE movie_id = \'" + rs2.getString("movie_id") + "\'";
                        ResultSet rs3 = stmt3.executeQuery(sql3);
                        Movie m=new Movie();
                        m.setMovie_id(rs.getString("movie_id"));
                        m.setYear(rs.getString("year"));
                        m.setMonth(rs.getString("month"));
                        m.setDay(rs.getString("day"));
                        m.setMovie_name(rs.getString("movie_name"));
                        m.setMpaa_rating(rs.getString("mpaa_rating"));
                        m.setDescription(rs.getString("description"));
                        m.setRuntime(rs.getString("runtime"));
                        m.setAverage_rating(rs.getString("average_rating"));
                        m.setStudio(rs.getString("studio"));
                        m.setRank(rs.getString("rank"));
                        movie.add(m);
                        rs3.close();
//                        movie.add(rs3.getString("DirectorProductId"));
                    }
                    rs2.close();
                }
                dbend = System.nanoTime();
                rs.close();
                stmt.close();
                conn.close();
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e){
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //search in dw
        try {
            Class.forName(dwDriverName);
            Connection conn = DriverManager.getConnection(dwConn);

            if(conn != null){
                String sql = "SELECT * FROM Movie WHERE director_name like ?";
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1,"%"+DirectorName+"%");

                // execute the query & calculate the time
                dwstart = System.nanoTime();
                ResultSet rs = stmt.executeQuery();
                dwend = System.nanoTime();
                rs.close();
                stmt.close();
                conn.close();
            }
        } catch (ClassNotFoundException | SQLException e){
            e.printStackTrace();
        }

        return new Result(dbend-dbstart,dwend-dwstart,count,movie);

    }

    public static Result searchByActor(String ActorName) {

        ArrayList<Movie> movie = new ArrayList<Movie>();
        long dbstart = 0, dbend = 0, dwstart = 0, dwend = 0;
        int count = 0;

        // search in db
        try{
            Class.forName(dbDriverName).newInstance();

            Connection conn = DriverManager.getConnection(dbConn);

            if(conn!=null) {
                String sql = "SELECT actor_id FROM Actor WHERE actor_name like ?";
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1,"%"+ActorName+"%");

                // execute the query & calculate the time
                dbstart = System.nanoTime();
                ResultSet rs = stmt.executeQuery();

                while(rs.next()) {
//                    count += rs.getInt(count_para);
                    Statement stmt2 = conn.createStatement();
                    String sql2 = "SELECT movie_id FROM Act WHERE actor_id = \'" + rs.getString("actor_id") + "\'";
                    ResultSet rs2 = stmt2.executeQuery(sql2);
                    while(rs2.next()){
                        Statement stmt3 = conn.createStatement();
                        String sql3 = "SELECT * FROM Movie WHERE movie_id = \'" + rs.getString("movie_id") + "\'";
                        ResultSet rs3 = stmt3.executeQuery(sql3);
                        while (rs3.next()) {
                            Movie m = new Movie();
                            m.setMovie_id(rs.getString("movie_id"));
                            m.setYear(rs.getString("year"));
                            m.setMonth(rs.getString("month"));
                            m.setDay(rs.getString("day"));
                            m.setMovie_name(rs.getString("movie_name"));
                            m.setMpaa_rating(rs.getString("mpaa_rating"));
                            m.setDescription(rs.getString("description"));
                            m.setRuntime(rs.getString("runtime"));
                            m.setAverage_rating(rs.getString("average_rating"));
                            m.setStudio(rs.getString("studio"));
                            m.setRank(rs.getString("rank"));
                            movie.add(m);
                        }
                        rs3.close();
                    }
                    rs2.close();
                }
                dbend = System.nanoTime();
                rs.close();
                stmt.close();
                conn.close();
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e){
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //search in dw
        try {
            Class.forName(dwDriverName);
            Connection conn = DriverManager.getConnection(dwConn);

            if(conn != null){
                String sql = "SELECT * FROM Movie WHERE actor_name like ?";
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1,"%"+ActorName+"%");

                // execute the query & calculate the time
                dwstart = System.nanoTime();
                ResultSet rs = stmt.executeQuery();
                dwend = System.nanoTime();

                rs.close();
                stmt.close();
                conn.close();
            }
        } catch (ClassNotFoundException | SQLException e){
            e.printStackTrace();
        }

        return new Result(dbend-dbstart,dwend-dwstart,count,movie);

    }

    public static Result searchByGenre(String GenreName) {

        ArrayList<Movie> movie = new ArrayList<Movie>();
        long dbstart = 0, dbend = 0, dwstart = 0, dwend = 0;
        int count = 0;

        // search in db
        try{
            Class.forName(dbDriverName).newInstance();

            Connection conn = DriverManager.getConnection(dbConn);

            if(conn!=null) {
                String sql = "SELECT movie_id FROM Movie_Genre WHERE genres like ?";
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1,"%"+GenreName+"%");

                // execute the query & calculate the time
                dbstart = System.nanoTime();
                ResultSet rs = stmt.executeQuery();

                while(rs.next()) {
//                    count += rs.getInt("Count");
                    Statement stmt2 = conn.createStatement();
                    String sql2 = "SELECT * FROM Movie WHERE movie_id = \'" + rs.getString("movie_id") + "\'";
                    ResultSet rs2 = stmt2.executeQuery(sql2);
                    while (rs2.next()) {
                        Movie m = new Movie();
                        m.setMovie_id(rs.getString("movie_id"));
                        m.setYear(rs.getString("year"));
                        m.setMonth(rs.getString("month"));
                        m.setDay(rs.getString("day"));
                        m.setMovie_name(rs.getString("movie_name"));
                        m.setMpaa_rating(rs.getString("mpaa_rating"));
                        m.setDescription(rs.getString("description"));
                        m.setRuntime(rs.getString("runtime"));
                        m.setAverage_rating(rs.getString("average_rating"));
                        m.setStudio(rs.getString("studio"));
                        m.setRank(rs.getString("rank"));
                        m.setLanguage_name(GenreName);
                        movie.add(m);
                    }
                    rs2.close();
//                    count += 1;
//                    MovieId.add(rs.getString("CategoryProductId"));
                }
                dbend = System.nanoTime();
                rs.close();
                stmt.close();
                conn.close();
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e){
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //search in dw
        try {
            Class.forName(dwDriverName);
            Connection conn = DriverManager.getConnection(dwConn);

            if(conn != null){

                String sql = "SELECT * FROM Movie WHERE genres like ?";
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1,"%"+GenreName+"%");

                // execute the query & calculate the time
                dwstart = System.nanoTime();
                ResultSet rs = stmt.executeQuery();
                dwend = System.nanoTime();

                rs.close();
                stmt.close();
                conn.close();
            }
        } catch (ClassNotFoundException | SQLException e){
            e.printStackTrace();
        }

        return new Result(dbend-dbstart,dwend-dwstart,count,movie);

    }

    public static Result searchByLanguage(String LanguageName) {

        ArrayList<Movie> movie = new ArrayList<Movie>();
        long dbstart = 0, dbend = 0, dwstart = 0, dwend = 0;
        int count = 0;

        // search in db
        try{
            Class.forName(dbDriverName).newInstance();

            Connection conn = DriverManager.getConnection(dbConn);

            if(conn!=null) {
                String sql = "SELECT movie_id FROM Movie_Language WHERE language_name like ?";
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1,"%"+LanguageName+"%");

                // execute the query & calculate the time
                dbstart = System.nanoTime();
                ResultSet rs = stmt.executeQuery();

                while(rs.next()) {
//                    count += rs.getInt("Count");
                    Statement stmt2 = conn.createStatement();
                    String sql2 = "SELECT * FROM Movie WHERE movie_id = \'" + rs.getString("movie_id") + "\'";
                    ResultSet rs2 = stmt2.executeQuery(sql2);
                    while (rs2.next()) {
                        Movie m = new Movie();
                        m.setMovie_id(rs.getString("movie_id"));
                        m.setYear(rs.getString("year"));
                        m.setMonth(rs.getString("month"));
                        m.setDay(rs.getString("day"));
                        m.setMovie_name(rs.getString("movie_name"));
                        m.setMpaa_rating(rs.getString("mpaa_rating"));
                        m.setDescription(rs.getString("description"));
                        m.setRuntime(rs.getString("runtime"));
                        m.setAverage_rating(rs.getString("average_rating"));
                        m.setStudio(rs.getString("studio"));
                        m.setRank(rs.getString("rank"));
                        m.setLanguage_name(LanguageName);
                        movie.add(m);
                    }
                    rs2.close();
//                    count += 1;
//                    MovieId.add(rs.getString("CategoryProductId"));
                }
                dbend = System.nanoTime();
                rs.close();
                stmt.close();
                conn.close();
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e){
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //search in dw
        try {
            Class.forName(dwDriverName);
            Connection conn = DriverManager.getConnection(dwConn);

            if(conn != null){

                String sql = "SELECT * FROM Movie WHERE genres like ?";
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.setString(1,"%"+LanguageName+"%");

                // execute the query & calculate the time
                dwstart = System.nanoTime();
                ResultSet rs = stmt.executeQuery();
                dwend = System.nanoTime();

                rs.close();
                stmt.close();
                conn.close();
            }
        } catch (ClassNotFoundException | SQLException e){
            e.printStackTrace();
        }

        return new Result(dbend-dbstart,dwend-dwstart,count,movie);

    }
}

