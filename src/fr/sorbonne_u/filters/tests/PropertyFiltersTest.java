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
 * Test class for filters on one or multiple properties, including:
 * 
 * <ul>
 * 	<li> {@link PropertyFilter} </li>
 * 	<li> {@link PropertiesFilter} with a {@link MultiValuesFilterI}, e.g. {@link IMC} </li>
 * </ul>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class PropertyFiltersTest {

    // --------------------------
    // PropertyFilter tests
    // --------------------------
    @Test
    void testPropertyFilterConstructor() {
        ValueFilterI vf = new IdenticalValueFilter(42);
        PropertyFilter filter = new PropertyFilter("age", vf);
        assertNotNull(filter);
        
        // getName() and getValueFilter() methods
        assertEquals("age", filter.getName());
        assertEquals(vf, filter.getValueFilter());

        // constructor with null arguments: throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new PropertyFilter(null, vf));
        assertThrows(IllegalArgumentException.class, () -> new PropertyFilter("age", null));
    }
    
    @Test
    void testPropertyFilterMatch() {
        ValueFilterI vf = new IdenticalValueFilter(42);
        PropertyFilter filter = new PropertyFilter("age", vf);

        Property correct = new Property("age", 42);
        Property wrongName = new Property("weight", 42);
        Property wrongValue = new Property("age", 43);

        // test different PropertyI for match
        assertTrue(filter.match(correct));
        assertFalse(filter.match(wrongName));
        assertFalse(filter.match(wrongValue));
    }

    // --------------------------
    // PropertiesFilter tests with IMC
    // --------------------------
    @Test
    void testPropertiesFilterConstructor() {
        // normal case
        MultiValuesFilterI imcFilter = new IMC();
        PropertiesFilter pf = new PropertiesFilter(imcFilter);
        assertNotNull(pf);
        
        // constructor with null argument: throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> new PropertiesFilter(null));
    }
    
    @Test
    void testPropertiesFilterMatch1() {
        // IMC Filter: weight / (height*height) >= 30
        MultiValuesFilterI imcFilter = new IMC();
        PropertiesFilter pf = new PropertiesFilter(imcFilter);

        Property weight = new Property("weight", 90.0);
        Property height = new Property("height", 1.7);
        Property heightTooHigh = new Property("height", 2.0);

        // true match: IMC =~ 31.14 >= 30.0
        assertTrue(pf.match(weight, height));

        // false match: IMC = 22.5 < 30.0
        assertFalse(pf.match(weight, heightTooHigh));
    }
    
    @Test
    void testPropertiesFilterMatch2() {
        MultiValuesFilterI imcFilter = new IMC();
        PropertiesFilter pf = new PropertiesFilter(imcFilter);

        Property weight = new Property("weight", 90.0);
        Property height = new Property("height", 1.7);

        // null properties: throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> pf.match((PropertyI[])null));
        
        // fewer than 2 properties: throws IllegalArgumentException
        assertThrows(IllegalArgumentException.class, () -> pf.match(weight));
        
        // missing property (height): throws IllegalArgumentException
        Property fingerCount = new Property("fingerCount", 10);
        Property eyeColor = new Property("eyeColor", "green");
        assertThrows(IllegalArgumentException.class, () -> pf.match(weight, fingerCount, eyeColor));

        // IMC Filter with wrong type: returns false
        Property weightString = new Property("weight", "90"); // weight ("90") as String, should be double
        assertFalse(pf.match(weightString, height));
    }
}