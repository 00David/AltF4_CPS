package fr.sorbonne_u.filters.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Serializable;
import java.util.Calendar;

import org.junit.jupiter.api.Test;

import fr.sorbonne_u.filters.tests.utils.IMC;
import fr.sorbonne_u.filters.value.JokerValueFilter;
import fr.sorbonne_u.filters.value.ValueFilter;

/**
 * Classe de tests pour les filtres simples et multi-valeurs, comprenant les classes :
 * <li> {@link ValueFilter} </li>
 * <li> {@link IMC}, étendant {@link MultiValuesFilter} </li>
 * <li> {@link JokerValueFilter} </li>
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ValueFiltersTest {

    // --------------------------------------------------
    // Tests ValueFilter
    // --------------------------------------------------
    @Test
    void testValueFilterConstructor() {
    	// cas normal
        ValueFilter filter = new ValueFilter("hello");
        assertNotNull(filter);

        // valeur nulle en paramètre : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new ValueFilter(null));
    }

    @Test
    void testValueFilterMatch1() {
        ValueFilter stringFilter = new ValueFilter("hello");

        // on essaye de match avec différents paramètres
        assertTrue(stringFilter.match("hello"), "should pass");
        assertFalse(stringFilter.match("world"), "should fail");
        assertFalse(stringFilter.match(10), "should fail");

        // valeur nulle en paramètre : renvoit une IllegalArgumentException
        assertFalse(stringFilter.match(null));
    }
    
    @Test
    void testValueFilterMatch2() {
    	ValueFilter intFilter = new ValueFilter(42);

    	// on essaye de match avec différents paramètres
        assertTrue(intFilter.match(42), "should pass");
        assertFalse(intFilter.match(43), "should fail");
        assertFalse(intFilter.match("AHOUUUU"), "should fail");
    }

    // --------------------------------------------------
    // Tests JokerValueFilter
    // --------------------------------------------------
    @Test
    void testJokerValueFilterMatch() {
        JokerValueFilter filter = new JokerValueFilter();
        assertNotNull(filter);

        // JokerValueFilter accepte tout (tant que c'est Serializable ou null)
        assertTrue(filter.match("anything"));
        assertTrue(filter.match(42));
        assertTrue(filter.match(null));
        assertTrue(filter.match(Calendar.getInstance()));
    }

    // --------------------------------------------------
    // Tests IMCTest (MultiValuesFilter)
    // --------------------------------------------------
    @Test
    void testIMCTestConstructor() {
        IMC imcFilter = new IMC();
        assertNotNull(imcFilter);

        // méthode getNames()
        String[] names = imcFilter.getNames();
        assertArrayEquals(new String[] {"poids", "taille"}, names, "property names must be the same");
    }

    @Test
    void testIMCTestMatch() {
    	IMC imcFilter = new IMC();

        // Personne en obésité (IMC >= 30)
        assertTrue(imcFilter.match(90.0, 1.7), "IMC >= 30 doit passer");

        // Personne normale (IMC < 30)
        assertFalse(imcFilter.match(60.0, 1.7), "IMC < 30 doit échouer");

        // Vérification des exceptions
        assertThrows(IllegalArgumentException.class, () -> imcFilter.match((Serializable[])null));
        assertThrows(IllegalArgumentException.class, () -> imcFilter.match(70.0)); // moins de 2 valeurs
        assertThrows(IllegalArgumentException.class, () -> imcFilter.match("90", 1.7)); // poids pas double
        assertThrows(IllegalArgumentException.class, () -> imcFilter.match(90.0, "1.7")); // taille pas double
    }
}
