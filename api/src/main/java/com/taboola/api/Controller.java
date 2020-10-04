package com.taboola.api;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import java.sql.Timestamp;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController()
@RequestMapping("/api")
public class Controller {

    @Autowired
    DAO dao;

    @GetMapping("/currentTime")
    public long time() {
        return Instant.now().toEpochMilli();
    }


    //http://localhost:8080/api//counters/time/1601779060000/eventId/74
    //http://localhost:8080/api//counters/time/1601779065000/eventId/38
    @GetMapping("/counters/time/{datetime}")
    public Map getCountersWithDate(@PathVariable String datetime) {
        Map<Integer, Integer> resource = new HashMap();
        Timestamp time = new Timestamp(Long.parseLong(datetime));
        resource = dao.getEvents(time);
        return resource;
    }

    @GetMapping("/counters/time/{datetime}/eventId/{eventid}")
    public Map getCountersWithDateAndEventId(@PathVariable String datetime, @PathVariable String eventid) {
        Map<Integer, Integer> resource = new HashMap();
        Timestamp time = new Timestamp(Long.parseLong(datetime));
        resource = dao.getEvents(time, Long.parseLong(eventid));
        return resource;
    }

}
