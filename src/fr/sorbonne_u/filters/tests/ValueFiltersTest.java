package fr.sorbonne_u.filters.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Serializable;
import java.util.Calendar;

import org.junit.jupiter.api.Test;

import fr.sorbonne_u.filters.tests.utils.IMC;
import fr.sorbonne_u.filters.value.IdenticalValueFilter;
import fr.sorbonne_u.filters.value.JokerValueFilter;

/**
 * Test class for simple and multi-value filters, including:
 * <li> {@link IdenticalValueFilter} </li>
 * <li> {@link IMC}, extending {@link MultiValuesFilter} </li>
 * <li> {@link JokerValueFilter} </li>
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
    // IMC tests (MultiValuesFilter)
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