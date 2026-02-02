package fr.sorbonne_u.filters.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Serializable;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;

import fr.sorbonne_u.filters.value.comparable.EqualToValueFilter;
import fr.sorbonne_u.filters.value.comparable.GreaterThanValueFilter;
import fr.sorbonne_u.filters.value.comparable.LessThanValueFilter;

/**
 * Tests unitaires pour les filtres sur valeurs comparables :
 * <li> {@link EqualToValueFilter} </li>
 * <li> {@link GreaterThanValueFilter} </li>
 * <li> {@link LessThanValueFilter} </li>
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ComparableValueFiltersTest {

    // --------------------------------------------------
    // Tests EqualToValueFilter
    // --------------------------------------------------
    @Test
    void testEqualToValueFilterConstructor() {
    	// cas normal
        EqualToValueFilter<Integer> filter = new EqualToValueFilter<>(10);
        assertNotNull(filter);

        // valeur nulle en paramètre : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new EqualToValueFilter<Integer>(null));
    }

    @Test
    void testEqualToValueFilterMatch() {
        EqualToValueFilter<Integer> filter = new EqualToValueFilter<>(10);

        // on teste si la valeur == 10
        assertTrue(filter.match(10));
        assertFalse(filter.match(5));
        assertFalse(filter.match(15));

        // valeur (String) implantant Comparable, incompatible : renvoit une ClassCastException
        assertThrows(ClassCastException.class, () -> filter.match("chaine"));
        
        // valeur (List<Integer>) n'implantant pas Comparable, incompatible : renvoit une ClassCastException
        assertThrows(ClassCastException.class, () -> filter.match(new ArrayList<Integer>()));

        // valeur nulle en paramètre : renvoit une NullPointerException
        assertThrows(NullPointerException.class, () -> filter.match((Serializable) null));
    }

    // --------------------------------------------------
    // Tests GreaterThanValueFilter
    // --------------------------------------------------
    @Test
    void testGreaterThanValueFilterConstructor() {
    	// cas normal
        GreaterThanValueFilter<String> filter = new GreaterThanValueFilter<>("c");
        assertNotNull(filter);

        // valeur nulle en paramètre : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new GreaterThanValueFilter<Integer>(null));
    }

    @Test
    void testGreaterThanValueFilterMatch() {
        GreaterThanValueFilter<Integer> filter = new GreaterThanValueFilter<>(10);

        // on teste si la valeur > 10
        assertTrue(filter.match(11));
        assertFalse(filter.match(10));
        assertFalse(filter.match(5));

        // valeur (String) implantant Comparable, incompatible : renvoit une ClassCastException
        assertThrows(ClassCastException.class, () -> filter.match("chaine"));
        
        // valeur (List<Integer>) n'implantant pas Comparable, incompatible : renvoit une ClassCastException
        assertThrows(ClassCastException.class, () -> filter.match(new ArrayList<Integer>()));
        
        // valeur nulle en paramètre : renvoit une NullPointerException
        assertThrows(NullPointerException.class, () -> filter.match((Serializable) null));
    }

    // --------------------------------------------------
    // Tests LessThanValueFilter
    // --------------------------------------------------
    @Test
    void testLessThanValueFilterConstructor() {
    	// cas normal
    	LessThanValueFilter<Integer> filter = new LessThanValueFilter<>(10);
        assertNotNull(filter);
        
        // valeur nulle en paramètre : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new LessThanValueFilter<Integer>(null));
    }

    @Test
    void testLessThanValueFilterMatch() {
        LessThanValueFilter<Integer> filter = new LessThanValueFilter<>(10);
        
        // on teste si la valeur < 10
        assertTrue(filter.match(5));
        assertFalse(filter.match(10));
        assertFalse(filter.match(15));

        // valeur (String) implantant Comparable, incompatible : renvoit une ClassCastException
        assertThrows(ClassCastException.class, () -> filter.match("chaine"));
        
        // valeur (List<Integer>) n'implantant pas Comparable, incompatible : renvoit une ClassCastException
        assertThrows(ClassCastException.class, () -> filter.match(new ArrayList<Integer>()));
        
        // valeur nulle en paramètre : renvoit une NullPointerException
        assertThrows(NullPointerException.class, () -> filter.match((Serializable) null));
    }
}
