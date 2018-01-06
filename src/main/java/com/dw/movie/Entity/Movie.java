package com.dw.movie.Entity;

import java.sql.*;

public class Movie {

    private static String dbDriverName = "com.mysql.jdbc.Driver";
    private static String dbConn = "jdbc:mysql://127.0.0.1:3306/sys?user=root&password=root";

    private String movie_id;
    private String movie_name;
    private String genre_name;
    private String language_name;
    private String mpaa_rating;
    private String description;
    private String runtime;
    private String average_rating;
    private String studio;
    private String rank;
    private String isMainActor;

    private String director_name;
    private String actor_name;
    private String review_id;
    private String year;
    private String month;
    private String day;
    private String season;

    public String getMovie_id() {
        return movie_id;
    }

    public void setMovie_id(String movie_id) {
        this.movie_id = movie_id;
    }

    public String getMovie_name() {
        return movie_name;
    }

    public void setMovie_name(String movie_name) {
        this.movie_name = movie_name;
    }

    public String getGenre_name() {
        return genre_name;
    }

    public void setGenre_name(String genre_name) {
        this.genre_name = genre_name;
    }

    public String getLanguage_name() {
        return language_name;
    }

    public void setLanguage_name(String language_name) {
        this.language_name = language_name;
    }

    public String getMpaa_rating() {
        return mpaa_rating;
    }

    public void setMpaa_rating(String mpaa_rating) {
        this.mpaa_rating = mpaa_rating;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getRuntime() {
        return runtime;
    }

    public void setRuntime(String runtime) {
        this.runtime = runtime;
    }

    public String getAverage_rating() {
        return average_rating;
    }

    public void setAverage_rating(String average_rating) {
        this.average_rating = average_rating;
    }

    public String getStudio() {
        return studio;
    }

    public void setStudio(String studio) {
        this.studio = studio;
    }

    public String getRank() {
        return rank;
    }

    public void setRank(String rank) {
        this.rank = rank;
    }

    public String getIsMainActor() {
        return isMainActor;
    }

    public void setIsMainActor(String isMainActor) {
        this.isMainActor = isMainActor;
    }

    public static String getDbDriverName() {
        return dbDriverName;
    }

    public static void setDbDriverName(String dbDriverName) {
        Movie.dbDriverName = dbDriverName;
    }

    public static String getDbConn() {
        return dbConn;
    }

    public static void setDbConn(String dbConn) {
        Movie.dbConn = dbConn;
    }

    public String getDirector_name() {
        return director_name;
    }

    public void setDirector_name(String director_name) {
        this.director_name = director_name;
    }

    public String getActor_name() {
        return actor_name;
    }

    public void setActor_name(String actor_name) {
        this.actor_name = actor_name;
    }

    public String getReview_id() {
        return review_id;
    }

    public void setReview_id(String review_id) {
        this.review_id = review_id;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getSeason() {
        return season;
    }

    public void setSeason(String season) {
        this.season = season;
    }

    public void getMoviebyId(String Id){
        try{
            Class.forName(dbDriverName).newInstance();

            Connection conn = DriverManager.getConnection(dbConn);

            if(conn!=null) {
                Statement stmt = conn.createStatement();
                String sql = "SELECT * FROM movie WHERE movie_id = \'" + Id+ "\'";

                ResultSet rs = stmt.executeQuery(sql);
                while(rs.next()) {
                    this.movie_id = rs.getString("movie_id");
                    this.movie_name = rs.getString("movie_name");
                    this.genre_name=rs.getString("genre_name");
                    this.language_name=rs.getString("language_name");
                    this.mpaa_rating=rs.getString("mpaa_rating");
                    this.description=rs.getString("description");
                    this.runtime=rs.getString("runtime");
                    this.average_rating=rs.getString("average_rating");
                    this.studio=rs.getString("studio");
                    this.rank=rs.getString("rank");
                    this.isMainActor=rs.getString("isMainActor");

                    this.director_name=rs.getString("director_name");
                    this.actor_name=rs.getString("actor_name");
                    this.review_id=rs.getString("review_id");
                    this.year=rs.getString("year");
                    this.month=rs.getString("month");
                    this.day=rs.getString("day");
                    this.season=rs.getString("season");
                }

                rs.close();
                stmt.close();
                conn.close();
            }
        } catch (SQLException | InstantiationException | IllegalAccessException | ClassNotFoundException e){
            e.printStackTrace();
        }
    }
}
