package com.twq.geometry;

import no.ecc.vectortile.VectorTileDecoder;
import no.ecc.vectortile.VectorTileEncoder;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class VectorTileTest {
    public static void main(String[] args) throws ParseException, IOException {
        VectorTileEncoder encoder = new VectorTileEncoder();
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("a1", "test");

        String polygonA = "POLYGON((515694.86 3347690.67,515693 3347740.81,515692.19 3347754.87,515744.91 3347751.7,515786.22 3347752.19,515828.91 3347752.46,515847.8 3347752.24,515844.98 3347711.79,515844.95 3347706.2,515832.02 3347707.77,515830.58 3347700.11,515812.63 3347702.55,515811.53 3347691.42,515799.44 3347691.42,515761.7 3347691.13,515762.11 3347704.96,515744.73 3347705.21,515744.73 3347691,515720.45 3347690.81,515694.86 3347690.67))";

        WKTReader reader = new WKTReader();

        Geometry geomA = reader.read(polygonA);

        encoder.addFeature("hz_building", attributes, geomA);

        byte[] encodedTile = encoder.encode(); // protobuf编码成的字节数组


        VectorTileDecoder decoder = new VectorTileDecoder();
        VectorTileDecoder.FeatureIterable iterable = decoder.decode(encodedTile);
        for (VectorTileDecoder.Feature feature : iterable) {
            System.out.println(feature.getGeometry());
        }

    }
}
