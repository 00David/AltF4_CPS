package fr.sorbonne_u.meteo.data.tests;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.AlertType;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.AlertTypeI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.Level;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.LevelI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.meteo.data.MeteoAlert;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.RegionData;
import fr.sorbonne_u.meteo.data.WindData;

/**
 * Unit tests for data classes of the meteorological application :
 * 
 * <p>
 * <ul>
 * 	<li>{@link PositionData} class.</li>
 * 	<li>{@link RegionData} class.</li>
 * 	<li>{@link PositionData} class.</li>
 * 	<li>{@link MeteoAlert} class.</li>
 * </ul>
 * </p
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
class MeteoDataTest {

    // ----------------------------
    // Test PositionData
    // ----------------------------
    @Test
    void testPositionData() {
        PositionData pos = new PositionData(10.5, 20.5);
        assertEquals(10.5, pos.getPosX(), "X component should be 10.5");
        assertEquals(20.5, pos.getPosY(), "Y component should be 20.5");

        PositionData pos2 = new PositionData(10.5, 20.5);
        PositionData pos3 = new PositionData(5, 5);

        assertTrue(pos.equals(pos2), "Positions with same coordinates should be equal");
        assertFalse(pos.equals(pos3), "Positions with different coordinates should not be equal");
    }

    // ----------------------------
    // Tests RegionData
    // ----------------------------
    @Test
    void testInvalidRegionData() {
        assertThrows(IllegalArgumentException.class, () -> { new RegionData(10, 0, 0, 5); }, 
        		"xMin > xMax should throw an exception");
        assertThrows(IllegalArgumentException.class, () -> { new RegionData(0, 5, 10, 0); }, 
        		"yMin > yMax should throw an exception");
    }
    
    @Test
    void testRegionData() {
        RegionData region = new RegionData(0, 10, 0, 20);
        double[] limits = region.getLimits();
        assertArrayEquals(new double[]{0,10,0,20}, limits, "Region limits should match");

        PositionData inside = new PositionData(5,10);
        PositionData outside = new PositionData(15,25);

        assertTrue(region.in(inside), "Position inside region should return true");
        assertFalse(region.in(outside), "Position outside region should return false");
        assertFalse(region.in(null), "Position null should return false, not inside the region");
    }

    // ----------------------------
    // Tests WindData
    // ----------------------------
    @Test
    void testInvalidWindData() {
        assertThrows(IllegalArgumentException.class, () -> { new WindData(null, 1.0, 1.0); }, 
        		"Null position should throw an exception");
    }
    
    @Test
    void testWindData() {
        PositionData pos = new PositionData(0,0);
        WindData wind = new WindData(pos, 3.0, 4.0);

        assertEquals(pos, wind.getPosition(), "Wind position should match");
        assertEquals(3.0, wind.xComponent(), "Wind X component should match");
        assertEquals(4.0, wind.yComponent(), "Wind Y component should match");
        assertEquals(5.0, wind.force(), "Wind force should be sqrt(3^2 + 4^2) = 5.0");
    }

    // ----------------------------
    // Tests MeteoAlert
    // ----------------------------
    @Test
    void testInvalidMeteoAlert() {
        AlertTypeI type = AlertType.FLOODING;
        LevelI level = Level.RED;

        ArrayList<RegionI> regions = new ArrayList<>();
        regions.add(new RegionData(0,10,0,10));
        
        ArrayList<RegionI> emptyRegions = new ArrayList<>();

        Instant start = Instant.now();
        Duration dur = Duration.ofHours(2);

        assertThrows(IllegalArgumentException.class, () -> { new MeteoAlert(null, level, regions, start, dur); }, 
        		"Null alertType should throw an exception");
        assertThrows(IllegalArgumentException.class, () -> { new MeteoAlert(type, null, regions, start, dur); }, 
        		"Null alertLevel should throw an exception");
        assertThrows(IllegalArgumentException.class, () -> { new MeteoAlert(type, level, null, start, dur); }, ""
        		+ "Null regions should throw an exception");
        assertThrows(IllegalArgumentException.class, () -> { new MeteoAlert(type, level, emptyRegions, start, dur); }, 
        		"Empty regions should throw an exception");
        assertThrows(IllegalArgumentException.class, () -> { new MeteoAlert(type, level, regions, null, dur); }, ""
        		+ "Null start time should throw an exception");
        assertThrows(IllegalArgumentException.class, () -> { new MeteoAlert(type, level, regions, start, null); }, ""
        		+ "Null duration should throw an exception");
    }
    
    @Test
    void testMeteoAlert() {
        AlertTypeI type = AlertType.FLOODING;
        LevelI level = Level.RED;

        ArrayList<RegionI> regions = new ArrayList<>();
        RegionI r = new RegionData(0,10,0,10); regions.add(r);

        Instant start = Instant.now();
        Duration dur = Duration.ofHours(2);

        MeteoAlert alert = new MeteoAlert(type, level, regions, start, dur);

        assertEquals(type, alert.getAlertType(), "Alert type should match");
        assertEquals(level, alert.getLevel(), "Alert level should match");
        assertEquals(1, alert.getRegions().length, "There should be 1 region");
        assertTrue(alert.getRegions()[0] == r, "The only region should match");
        assertEquals(start, alert.getStartTime(), "Start time should match");
        assertEquals(dur, alert.getDuration(), "Duration should match");
    }
}