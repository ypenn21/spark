package com.taboola.api;

import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import java.util.HashMap;
import java.util.Map;

@Component
public class DAO {

    public Map getEvents(Timestamp datetime, Long eventid){
        return getEventFromDb(datetime, eventid);
    }

    public Map getEvents(Timestamp datetime){
        return getEventFromDb(datetime, null);
    }

    private Map getEventFromDb(Timestamp datetime, Long eventid){
        HashMap<Integer, Integer> myMap = new HashMap();
        Connection connection=null;
        PreparedStatement stmt=null;

        try {
            String sql ="select * from events where TIME_BUCKET=?";
            if(eventid!=null){
                sql+=" and EVENT_ID=?";
            }
            connection = DataSource.getConnection();
            stmt = connection.prepareStatement(sql);
            stmt.setTimestamp(1, datetime);
            if(eventid!=null) {
                stmt.setLong(2, eventid);
            }
            ResultSet set = stmt.executeQuery();
            if (set.next()) {
                myMap.put(set.getInt(2), set.getInt(4));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally{
            try {

                if (connection != null) {
                    stmt.close();
                }
                if (stmt != null)
                    connection.close();

            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        return myMap;
    }


}
