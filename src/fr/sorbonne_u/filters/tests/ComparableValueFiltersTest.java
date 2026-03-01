package fr.sorbonne_u.filters.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import fr.sorbonne_u.filters.value.comparable.EqualToValueFilter;
import fr.sorbonne_u.filters.value.comparable.GreaterThanValueFilter;
import fr.sorbonne_u.filters.value.comparable.LessThanValueFilter;

/**
 * Unit tests for filters on comparable values:
 * <li> {@link EqualToValueFilter} </li>
 * <li> {@link GreaterThanValueFilter} </li>
 * <li> {@link LessThanValueFilter} </li>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ComparableValueFiltersTest {

    // --------------------------------------------------
    // EqualToValueFilter Tests
    // --------------------------------------------------
    @Test
    void testEqualToValueFilterConstructor() {
    	// normal case
        EqualToValueFilter<Integer> filter = new EqualToValueFilter<>(10);
        assertNotNull(filter);

        // null value as parameter: throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new EqualToValueFilter<Integer>(null));
    }

    @Test
    void testEqualToValueFilterMatch() {
        EqualToValueFilter<Integer> filter = new EqualToValueFilter<>(10);

        // test if value == 10
        assertTrue(filter.match(10));
        assertFalse(filter.match(5));
        assertFalse(filter.match(15));
        assertFalse(filter.match(null));

        // value (String) implementing Comparable, incompatible: throws ClassCastException
        assertThrows(ClassCastException.class, () -> filter.match("str"));
        
        // value (List<Integer>) not implementing Comparable, incompatible: throws ClassCastException
        assertThrows(ClassCastException.class, () -> filter.match(new ArrayList<Integer>()));
    }

    // --------------------------------------------------
    // GreaterThanValueFilter Tests
    // --------------------------------------------------
    @Test
    void testGreaterThanValueFilterConstructor() {
    	// normal case
        GreaterThanValueFilter<String> filter = new GreaterThanValueFilter<>("c");
        assertNotNull(filter);

        // null value as parameter: throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new GreaterThanValueFilter<Integer>(null));
    }

    @Test
    void testGreaterThanValueFilterMatch() {
        GreaterThanValueFilter<Integer> filter = new GreaterThanValueFilter<>(10);

        // test if value > 10
        assertTrue(filter.match(11));
        assertFalse(filter.match(10));
        assertFalse(filter.match(5));
        assertFalse(filter.match(null));

        // value (String) implementing Comparable, incompatible: throws ClassCastException
        assertThrows(ClassCastException.class, () -> filter.match("str"));
        
        // value (List<Integer>) not implementing Comparable, incompatible: throws ClassCastException
        assertThrows(ClassCastException.class, () -> filter.match(new ArrayList<Integer>()));
    }

    // --------------------------------------------------
    // LessThanValueFilter Tests
    // --------------------------------------------------
    @Test
    void testLessThanValueFilterConstructor() {
    	// normal case
    	LessThanValueFilter<Integer> filter = new LessThanValueFilter<>(10);
        assertNotNull(filter);
        
        // null value as parameter: throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new LessThanValueFilter<Integer>(null));
    }

    @Test
    void testLessThanValueFilterMatch() {
        LessThanValueFilter<Integer> filter = new LessThanValueFilter<>(10);
        
        // test if value < 10
        assertTrue(filter.match(5));
        assertFalse(filter.match(10));
        assertFalse(filter.match(15));
        assertFalse(filter.match(null));

        // value (String) implementing Comparable, incompatible: throws ClassCastException
        assertThrows(ClassCastException.class, () -> filter.match("str"));
        
        // value (List<Integer>) not implementing Comparable, incompatible: throws ClassCastException
        assertThrows(ClassCastException.class, () -> filter.match(new ArrayList<Integer>()));
    }
}