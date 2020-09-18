package com.twq.controller;

import com.twq.hbase.storage.Event;
import com.twq.service.OmneoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.ArrayList;
import java.util.List;

@Controller
public class OmneoController {

    @Autowired
    private OmneoService omneoService;

    @RequestMapping("/omneo/events")
    public String getEvents(Model model, String eventType, String partName) {

        System.out.println("eventType = " + eventType);

        System.out.println("partName = " + partName);

        List<Event> events = omneoService.getEvents(eventType, partName);

        model.addAttribute("events", events);

        model.addAttribute("eventType", eventType);

        model.addAttribute("partName", partName);

        return "events";
    }
}
