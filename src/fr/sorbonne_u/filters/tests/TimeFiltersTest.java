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
 * Tests unitaires pour les filtres sur valeurs temporelles :<br>
 * <li> {@link GreaterOrEqualTimeFilter} </li>
 * <li> {@link LessOrEqualTimeFilter} </li>
 * <li> {@link IntervalTimeFilter} </li>
 * <li> {@link JokerTimeFilter} </li>
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
    // Tests GreaterOrEqualTimeFilter
    // --------------------------------------------------
    @Test
    void testGreaterOrEqualTimeFilterConstructor() {
    	// cas normal
        GreaterOrEqualTimeFilter filter = new GreaterOrEqualTimeFilter(t1);
        assertNotNull(filter);
        
        // timestamp nul en paramètre : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new GreaterOrEqualTimeFilter(null));
    }
    
    @Test
    void testGreaterOrEqualTimeFilterMatch() {
        GreaterOrEqualTimeFilter filter = new GreaterOrEqualTimeFilter(t1); 

        // accepte les match avec timestamp en paramètre >= t1 
        assertTrue(filter.match(t1), "timestamp == filter should pass");
        assertTrue(filter.match(t2), "timestamp > filter should pass");
        assertFalse(filter.match(t0), "timestamp < filter should fail");

        // timestamp nul en paramètre : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> filter.match(null));
    }

    // --------------------------------------------------
    // Tests LessOrEqualTimeFilter
    // --------------------------------------------------
    @Test
    void testLessOrEqualTimeFilterConstructor() {
    	// cas normal
        LessOrEqualTimeFilter filter = new LessOrEqualTimeFilter(t1);
        assertNotNull(filter);
        
        // timestamp nul en paramètre : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new LessOrEqualTimeFilter(null));
    }
    
    @Test
    void testLessOrEqualTimeFilterMatch() {
        LessOrEqualTimeFilter filter = new LessOrEqualTimeFilter(t1); 
        
        // accepte les match avec timestamp en paramètre <= t1 
        assertTrue(filter.match(t1), "timestamp == filter should pass");
        assertTrue(filter.match(t0), "timestamp < filter should pass");
        assertFalse(filter.match(t2), "timestamp > filter should fail");
        
        // timestamp nul en paramètre : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> filter.match(null));
    }

    // --------------------------------------------------
    // Tests IntervalTimeFilter
    // --------------------------------------------------
    @Test
    void testIntervalTimeFilterConstructor() {
    	// cas normal
        IntervalTimeFilter filter = new IntervalTimeFilter(t0, t2);
        assertNotNull(filter);
        
        // timestamp nuls en paramètres : renvoient une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new IntervalTimeFilter(null, t2));
        assertThrows(IllegalArgumentException.class, () -> new IntervalTimeFilter(t0, null));
        
        // max < min : : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new IntervalTimeFilter(t2, t0));
    }
    
    @Test
    void testIntervalTimeFilterMatch1() {
        IntervalTimeFilter filter = new IntervalTimeFilter(t0, t2);

        // accepte les match avec t0 <= timestamp en paramètre <= t2
        assertTrue(filter.match(t0), "timestamp == minBound should pass");
        assertTrue(filter.match(t1), "timestamp inside interval should pass");
        assertTrue(filter.match(t2), "timestamp == maxBound should pass");
        
        // timestamp nul en paramètre : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> filter.match(null));
    }
    
    @Test
    void testIntervalTimeFilterMatch2() {
        IntervalTimeFilter filter = new IntervalTimeFilter(t0, t2);
        
        Instant before = t0.minusSeconds(1); // juste avant l'intervalle
        Instant after = t2.plusSeconds(1); // juste après l'intervalle

        // before et after en dehors de l'intervalle, pas accepté par match
        assertFalse(filter.match(before), "timestamp < minBound should fail");
        assertFalse(filter.match(after), "timestamp > maxBound should fail");
    }

    // --------------------------------------------------
    // Tests JokerTimeFilter
    // --------------------------------------------------
    @Test
    void testJokerTimeFilterConstructor() {
    	// cas normal
        JokerTimeFilter filter = new JokerTimeFilter();
        assertNotNull(filter);
        
        // timestamp nul en paramètre : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> filter.match(null));
    }
    
    @Test
    void testJokerTimeFilterMatch() {
        JokerTimeFilter filter = new JokerTimeFilter();

        // match accepte n'importe quel timestamp
        assertTrue(filter.match(t0));
        assertTrue(filter.match(t1));
        assertTrue(filter.match(t2));
        
        // timestamp nul en paramètre : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> filter.match(null));
    }
}

