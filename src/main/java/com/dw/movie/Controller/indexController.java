package com.dw.movie.Controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping(value = "/")
public class indexController {

    @RequestMapping(value = "")
    public String index() {
        return "index";
    }

    @RequestMapping(value = "actor")
    public String actor() {
        return "actor";
    }

    @RequestMapping(value = "combine")
    public String combine() {
        return "combine";
    }

    @RequestMapping(value = "director")
    public String director() {
        return "director";
    }

    @RequestMapping(value = "name")
    public String name() {
        return "name";
    }

    @RequestMapping(value = "search")
    public String search() {
        return "search";
    }

    @RequestMapping(value = "time")
    public String time() {
        return "time";
    }

    @RequestMapping(value = "type")
    public String type() {
        return "type";
    }
}
