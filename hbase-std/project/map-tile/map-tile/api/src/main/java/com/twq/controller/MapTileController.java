package com.twq.controller;

import com.twq.model.Feature;
import com.twq.model.Tile;
import com.twq.service.MapTileService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
public class MapTileController {

    private static Logger LOG = LoggerFactory.getLogger(MapTileController.class);

    @Autowired
    private MapTileService mapTileService;

    @RequestMapping(value = "/tiles", method = RequestMethod.POST)
    @ResponseBody
    public List<Tile> requestTilesByTileName(@RequestBody RequestInfo requestInfo) {
        LOG.info("request : " + requestInfo);
        // 1、接受并解析参数
        int level = requestInfo.getLevel();
        List<String> tileNames = requestInfo.getTiles();

        //2、调用Service服务拿到想要的数据(就是List<Tile>)
        List<Tile> tiles = mapTileService.findTilesByTileName(level, tileNames);

        //3、返回给前端
        return tiles;
    }

    @RequestMapping(value = "/tiles/{level}", method = RequestMethod.GET)
    @ResponseBody
    public List<Tile> requestTilesByLevel(@PathVariable int level) {
        LOG.info("level : " + level);
        List<Tile> tiles = mapTileService.findTilesByLevel(level);
        return tiles;
    }
}
