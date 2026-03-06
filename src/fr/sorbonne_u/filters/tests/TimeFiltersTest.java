package fr.sorbonne_u.filters.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import fr.sorbonne_u.filters.time.GreaterOrEqualTimeFilter;
import fr.sorbonne_u.filters.time.IntervalTimeFilter;
import fr.sorbonne_u.filters.time.JokerTimeFilter;
import fr.sorbonne_u.filters.time.LessOrEqualTimeFilter;

/**
 * Unit tests for temporal value filters:
 * 
 * <p>
 * <ul>
 * 	<li> {@link GreaterOrEqualTimeFilter} </li>
 * 	<li> {@link LessOrEqualTimeFilter} </li>
 * 	<li> {@link IntervalTimeFilter} </li>
 * 	<li> {@link JokerTimeFilter} </li>
 * </ul>
 * </p>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class TimeFiltersTest {

    private Instant t0;
    private Instant t1;
    private Instant t2;

    @BeforeEach
    void setup() {
        t0 = Instant.parse("2026-01-01T00:00:00Z");
        t1 = Instant.parse("2026-01-01T12:00:00Z");
        t2 = Instant.parse("2026-01-02T00:00:00Z");
        // t0 < t1 < t2
    }

    // --------------------------------------------------
    // GreaterOrEqualTimeFilter tests
    // --------------------------------------------------
    @Test
    void testGreaterOrEqualTimeFilterConstructor() {
        GreaterOrEqualTimeFilter filter = new GreaterOrEqualTimeFilter(t1);
        assertNotNull(filter);
        
        // null timestamp parameter: throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new GreaterOrEqualTimeFilter(null));
    }
    
    @Test
    void testGreaterOrEqualTimeFilterMatch() {
        GreaterOrEqualTimeFilter filter = new GreaterOrEqualTimeFilter(t1); 

        // accepts timestamps >= t1
        assertTrue(filter.match(t1), "timestamp == filter should pass");
        assertTrue(filter.match(t2), "timestamp > filter should pass");
        assertFalse(filter.match(t0), "timestamp < filter should fail");
        assertFalse(filter.match(null), "null timestamp should fail");
    }

    // --------------------------------------------------
    // LessOrEqualTimeFilter tests
    // --------------------------------------------------
    @Test
    void testLessOrEqualTimeFilterConstructor() {
        LessOrEqualTimeFilter filter = new LessOrEqualTimeFilter(t1);
        assertNotNull(filter);
        
        // null timestamp parameter: throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new LessOrEqualTimeFilter(null));
    }
    
    @Test
    void testLessOrEqualTimeFilterMatch() {
        LessOrEqualTimeFilter filter = new LessOrEqualTimeFilter(t1); 
        
        // accepts timestamps <= t1
        assertTrue(filter.match(t1), "timestamp == filter should pass");
        assertTrue(filter.match(t0), "timestamp < filter should pass");
        assertFalse(filter.match(t2), "timestamp > filter should fail");
        assertFalse(filter.match(null), "null timestamp should fail");
    }

    // --------------------------------------------------
    // IntervalTimeFilter tests
    // --------------------------------------------------
    @Test
    void testIntervalTimeFilterConstructor() {
        IntervalTimeFilter filter = new IntervalTimeFilter(t0, t2);
        assertNotNull(filter);
        
        // null bounds: throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new IntervalTimeFilter(null, t2));
        assertThrows(IllegalArgumentException.class, () -> new IntervalTimeFilter(t0, null));
        
        // maxBound < minBound: throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new IntervalTimeFilter(t2, t0));
    }
    
    @Test
    void testIntervalTimeFilterMatch1() {
        IntervalTimeFilter filter = new IntervalTimeFilter(t0, t2);

        // accepts timestamps within [minBound, maxBound]
        assertTrue(filter.match(t0), "timestamp == minBound should pass");
        assertTrue(filter.match(t1), "timestamp inside interval should pass");
        assertTrue(filter.match(t2), "timestamp == maxBound should pass");
        assertFalse(filter.match(null), "null timestamp should fail");
    }
    
    @Test
    void testIntervalTimeFilterMatch2() {
        IntervalTimeFilter filter = new IntervalTimeFilter(t0, t2);
        
        Instant before = t0.minusSeconds(1); // just before the interval
        Instant after = t2.plusSeconds(1);   // just after the interval

        // before and after outside interval: should fail
        assertFalse(filter.match(before), "timestamp < minBound should fail");
        assertFalse(filter.match(after), "timestamp > maxBound should fail");
    }

    // --------------------------------------------------
    // JokerTimeFilter tests
    // --------------------------------------------------
    @Test
    void testJokerTimeFilterConstructor() {
        JokerTimeFilter filter = new JokerTimeFilter();
        assertNotNull(filter);
    }
    
    @Test
    void testJokerTimeFilterMatch() {
        JokerTimeFilter filter = new JokerTimeFilter();

        // accepts any timestamp, including null
        assertTrue(filter.match(t0));
        assertTrue(filter.match(t1));
        assertTrue(filter.match(t2));
        assertTrue(filter.match(null));
    }
}