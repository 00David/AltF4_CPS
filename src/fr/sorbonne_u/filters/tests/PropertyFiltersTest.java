package fr.sorbonne_u.filters.tests;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.MultiValuesFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;
import fr.sorbonne_u.filters.property.PropertiesFilter;
import fr.sorbonne_u.filters.property.PropertyFilter;
import fr.sorbonne_u.filters.tests.utils.IMC;
import fr.sorbonne_u.filters.value.IdenticalValueFilter;
import fr.sorbonne_u.messages.Property;

/**
 * Classe de tests pour les filtres sur une ou plusieurs propriétés, comprenant :
 * <li> {@link PropertyFilter} </li>
 * <li> {@link PropertiesFilter} avec un {@link MultiValuesFilterI}, par ex. {@link IMC} </li>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class PropertyFiltersTest {

    // --------------------------
    // Tests PropertyFilter
    // --------------------------
    @Test
    void testPropertyFilterConstructor() {
        ValueFilterI vf = new IdenticalValueFilter(42);
        PropertyFilter filter = new PropertyFilter("age", vf);
        assertNotNull(filter);
        
        // méthodes getName() et getValueFilter()
        assertEquals("age", filter.getName());
        assertEquals(vf, filter.getValueFilter());

        // constructeur avec arguments null : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new PropertyFilter(null, vf));
        assertThrows(IllegalArgumentException.class, () -> new PropertyFilter("age", null));
    }
    
    @Test
    void testPropertyFilterMatch() {
        ValueFilterI vf = new IdenticalValueFilter(42);
        PropertyFilter filter = new PropertyFilter("age", vf);

        Property correct = new Property("age", 42);
        Property wrongName = new Property("poids", 42);
        Property wrongValue = new Property("age", 43);

        // on essaye différentes PropertyI pour le match
        assertTrue(filter.match(correct));
        assertFalse(filter.match(wrongName));
        assertFalse(filter.match(wrongValue));
    }

    // --------------------------
    // Tests PropertiesFilter avec IMC
    // --------------------------
    @Test
    void testPropertiesFilterConstructor() {
    	// cas normal
        MultiValuesFilterI imcFilter = new IMC();
        PropertiesFilter pf = new PropertiesFilter(imcFilter);
        assertNotNull(pf);
    	
    	// constructeur avec argument null : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new PropertiesFilter(null));
    }
    
    @Test
    void testPropertiesFilterMatch1() {
        // IMC Filter : poids / (taille*taille) >= 30
        MultiValuesFilterI imcFilter = new IMC();
        PropertiesFilter pf = new PropertiesFilter(imcFilter);

        Property poids = new Property("poids", 90.0);
        Property taille = new Property("taille", 1.7);
        Property tailleTropGrande = new Property("taille", 2.0);

        // Match vrai : IMC =~ 31,14 >= 30.0
        assertTrue(pf.match(poids, taille));

        // Match faux : IMC = 22,5 < 30.0
        assertFalse(pf.match(poids, tailleTropGrande));
    }
    
    @Test
    void testPropertiesFilterMatch2() {
        MultiValuesFilterI imcFilter = new IMC();
        PropertiesFilter pf = new PropertiesFilter(imcFilter);

        Property poids = new Property("poids", 90.0);
        Property taille = new Property("taille", 1.7);

        // Propriétés null : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> pf.match((PropertyI[])null));
        
        // Nombre de propriétés < 2 : renvoit une IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> pf.match(poids));
        
        // Propriété manquante (taille) : renvoit une IllegalArgumentException
        Property nbDoigts = new Property("nbDoigts", 10);
        Property couleurYeux = new Property("couleurYeux", "vert");
        assertThrows(IllegalArgumentException.class, () -> pf.match(poids, nbDoigts, couleurYeux));

        // IMC Filter avec type incorrect : renvoit une IllegalArgumentException
        Property poidsString = new Property("poids", "90");
        assertThrows(IllegalArgumentException.class, () -> pf.match(poidsString, taille));
    }
}