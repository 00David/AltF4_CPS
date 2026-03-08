package fr.sorbonne_u.filters.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.junit.jupiter.api.Test;

import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.filters.tests.utils.IMC;
import fr.sorbonne_u.filters.value.IdenticalValueFilter;
import fr.sorbonne_u.filters.value.JokerValueFilter;
import fr.sorbonne_u.filters.value.meteo.PositionInRegionsValueFilter;
import fr.sorbonne_u.filters.value.meteo.StrongWindMultiValuesFilter;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.RegionData;
import fr.sorbonne_u.filters.value.AbstractMultiValuesFilter;

/**
 * Test class for simple and multi-value filters, including:
 * 
 * <p>
 * <ul>
 * 	<li> {@link IdenticalValueFilter} </li>
 * 	<li> {@link JokerValueFilter} </li>
 * 	<li> {@link PositionInRegionsValueFilter} </li>
 * 	<li> {@link StrongWindMultiValuesFilter} </li>
 * 	<li> {@link IMC}, an example extending {@link AbstractMultiValuesFilter} </li>
 * </ul>
 * </p>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ValueFiltersTest {

    // --------------------------------------------------
    // IdenticalValueFilter tests
    // --------------------------------------------------
    @Test
    void testValueFilterConstructor() {
        // normal case
        IdenticalValueFilter filter = new IdenticalValueFilter("hello");
        assertNotNull(filter);

        // null parameter: throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new IdenticalValueFilter(null));
    }

    @Test
    void testValueFilterMatch1() {
        IdenticalValueFilter stringFilter = new IdenticalValueFilter("hello");

        // test matching with different parameters
        assertTrue(stringFilter.match("hello"), "should pass");
        assertFalse(stringFilter.match("world"), "should fail");
        assertFalse(stringFilter.match(10), "should fail");

        // null value: should fail
        assertFalse(stringFilter.match(null));
    }

    @Test
    void testValueFilterMatch2() {
        IdenticalValueFilter intFilter = new IdenticalValueFilter(42);

        // test matching with different parameters
        assertTrue(intFilter.match(42), "should pass");
        assertFalse(intFilter.match(43), "should fail");
        assertFalse(intFilter.match("AHOUUUU"), "should fail");
    }

    // --------------------------------------------------
    // JokerValueFilter tests
    // --------------------------------------------------
    @Test
    void testJokerValueFilterMatch() {
        JokerValueFilter filter = new JokerValueFilter();
        assertNotNull(filter);

        // JokerValueFilter accepts any value, including null
        assertTrue(filter.match("anything"));
        assertTrue(filter.match(42));
        assertTrue(filter.match(null));
        assertTrue(filter.match(Calendar.getInstance()));
    }
    
    // --------------------------------------------------
    // PositionInRegionsValueFilter tests
    // --------------------------------------------------
    @Test
    void testPositionInRegionsValueFilterConstructor() {
        // normal case
    	PositionInRegionsValueFilter filter = new PositionInRegionsValueFilter(new PositionData(2.0,2.0));
        assertNotNull(filter);

        // null parameter: throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new IdenticalValueFilter(null));
    }
    
    @Test
    void testPositionInRegionsValueFilterMatch() {
    	// X position = 2, Y position = 3
    	PositionInRegionsValueFilter filter = new PositionInRegionsValueFilter(new PositionData(2,3));

    	
    	// test matching with different regions
    	RegionData region1 = new RegionData(0, 4, 0, 6);
    	RegionData region2 = new RegionData(0, 1, 0, 6);
    	
    	List<RegionI> regions1 = new ArrayList<>();
    	regions1.add(region1);
    	assertTrue(filter.match((Serializable)regions1), "should pass"); // Both X position inside [0, 4] and Y position inside [0, 6]
    	
    	List<RegionI> regions2 = new ArrayList<>();
    	regions2.add(region2);
    	assertFalse(filter.match((Serializable)regions2), "should fail"); // X position outside [0, 1]
        
    	List<RegionI> regions3 = new ArrayList<>();
    	regions3.add(region1);
    	regions3.add(region2);
    	assertTrue(filter.match((Serializable)regions3), "should pass"); // X and Y positions inside region1, its enough
    	
    	List<RegionI> regions4 = new ArrayList<>();
    	assertFalse(filter.match((Serializable)regions4), "should fail"); // No region in the list

        // null value: should fail
        assertFalse(filter.match(null));
    }
    
    // --------------------------------------------------
    // StrongWindMultiValuesFilter tests
    // --------------------------------------------------
    @Test
    void testStrongWindMultiValuesFilterConstructor() {
        PositionData position = new PositionData(0.0, 0.0);

        // normal case
        StrongWindMultiValuesFilter filter = new StrongWindMultiValuesFilter(position, 10.0, 5.0);
        assertNotNull(filter);

        // null position
        assertThrows(IllegalArgumentException.class, () -> new StrongWindMultiValuesFilter(null, 10.0, 5.0));

        // negative distance threshold
        assertThrows(IllegalArgumentException.class, () -> new StrongWindMultiValuesFilter(position, -1.0, 5.0));

        // negative force threshold
        assertThrows(IllegalArgumentException.class, () -> new StrongWindMultiValuesFilter(position, 10.0, -1.0));
    }
    
    @Test
    void testStrongWindMultiValuesFilterMatch() {
        PositionData turbinePosition = new PositionData(0.0, 0.0);

        StrongWindMultiValuesFilter filter = new StrongWindMultiValuesFilter(turbinePosition, 10.0, 5.0);

        // Valid case: distance OK and force OK
        PositionData windSource1 = new PositionData(3.0, 4.0);
        assertTrue(filter.match(windSource1, 2.0, 1.0), "distance <= threshold and force <= threshold should pass");

        // Distance too large
        PositionData windSource2 = new PositionData(20.0, 20.0);
        assertFalse(filter.match(windSource2, 2.0, 1.0), "distance > threshold should fail");

        // Force too large (force = sqrt(4² + 4²) > 5)
        PositionData windSource3 = new PositionData(1.0, 1.0);
        assertFalse(filter.match(windSource3, 4.0, 4.0), "force > threshold should fail");

        // Invalid types
        assertFalse(filter.match("notPosition", 2.0, 1.0), "invalid position type should fail");
        assertFalse(filter.match(windSource1, "2.0", 1.0), "invalid xWind type should fail");
        assertFalse(filter.match(windSource1, 2.0, "1.0"), "invalid yWind type should fail");

        // Null values
        assertFalse(filter.match(null, 2.0, 1.0), "null position should fail");
    }
    
    
    // --------------------------------------------------
    // IMC tests (concrete case of AbstractMultiValuesFilter)
    // --------------------------------------------------
    @Test
    void testIMCTestConstructor() {
        IMC imcFilter = new IMC();
        assertNotNull(imcFilter);

        // check property names
        String[] names = imcFilter.getNames();
        assertArrayEquals(new String[] {"weight", "height"}, names, "property names must match");
    }

    @Test
    void testIMCTestMatch() {
        IMC imcFilter = new IMC();

        // Person with obesity (IMC (BMI) >= 30)
        assertTrue(imcFilter.match(90.0, 1.7), "BMI >= 30 should pass");

        // Normal person (IMC (BMI) < 30)
        assertFalse(imcFilter.match(60.0, 1.7), "BMI < 30 should fail");

        // Check exceptions
        assertThrows(IllegalArgumentException.class, () -> imcFilter.match((Serializable[])null));
        assertThrows(IllegalArgumentException.class, () -> imcFilter.match(70.0)); // less than 2 values
        
        // Incompatible value types: should fail
        assertFalse(imcFilter.match("90", 1.7), "weight as String should fail");
        assertFalse(imcFilter.match(90.0, "1.7"), "height as String should fail");
    }
}