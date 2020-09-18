package com.twq.geometry;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class GeomTest {
    public static void main(String[] args) throws ParseException {
        String polygonA = "POLYGON((515694.86 3347690.67,515693 3347740.81,515692.19 3347754.87,515744.91 3347751.7,515786.22 3347752.19,515828.91 3347752.46,515847.8 3347752.24,515844.98 3347711.79,515844.95 3347706.2,515832.02 3347707.77,515830.58 3347700.11,515812.63 3347702.55,515811.53 3347691.42,515799.44 3347691.42,515761.7 3347691.13,515762.11 3347704.96,515744.73 3347705.21,515744.73 3347691,515720.45 3347690.81,515694.86 3347690.67))";

        WKTReader reader = new WKTReader();

        Geometry geomA = reader.read(polygonA);

        System.out.println(geomA);

        // 拿到多边形的最大最小的X和Y值
        Envelope env = geomA.getEnvelopeInternal();
        env.getMinX();
        env.getMaxX();
        env.getMinY();
        env.getMaxY();

        // 拿到所有的多边形中的所有组成的点
        Coordinate[] coordinates = geomA.getCoordinates();
        for (Coordinate c : coordinates) {
            System.out.println(c);
        }

        //判断两个区域是否相交
        String polygonB = "POLYGON((512038.92 3345986.92,512032.47 3345994.99,512032.45 3346014.16,512042.32 3346022.17,512047.86 3346015.34,512040.32 3346009.22,512050.68 3345996.47,512038.92 3345986.92))";
        Geometry geomB = new WKTReader().read(polygonB); // 将多边形解析成一个Geometry对象
        System.out.println(geomB.intersects(geomA));

    }
}
