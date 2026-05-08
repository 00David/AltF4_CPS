package fr.sorbonne_u.filters.tests;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertiesFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.property.PropertiesFilter;
import fr.sorbonne_u.filters.property.PropertyFilter;
import fr.sorbonne_u.filters.tests.utils.IMC;
import fr.sorbonne_u.filters.tests.utils.Muscle;
import fr.sorbonne_u.filters.time.GreaterOrEqualTimeFilter;
import fr.sorbonne_u.filters.time.JokerTimeFilter;
import fr.sorbonne_u.filters.value.comparable.EqualToValueFilter;
import fr.sorbonne_u.filters.value.comparable.GreaterThanValueFilter;
import fr.sorbonne_u.filters.value.comparable.LessThanValueFilter;
import fr.sorbonne_u.messages.MessageTest;

/**
 * Unit tests for {@link MessageFilter}.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class MessageFilterTest {

    // --------------------------
    // Constructor tests
    // --------------------------
	
	@Test
	public void testConstructor() {
		MessageFilter filter = new MessageFilter(new PropertyFilterI[] {}, new PropertiesFilterI[] {}, new JokerTimeFilter());
		assertNotNull(filter);
		
		// constructor with one null argument: throws IllegalArgumentException
		assertThrows(IllegalArgumentException.class, () -> new MessageFilter(
				(PropertyFilterI[])null, 
				new PropertiesFilterI[] {}, 
				new GreaterOrEqualTimeFilter(Instant.now())
				));
		assertThrows(IllegalArgumentException.class, () -> new MessageFilter(
				new PropertyFilterI[] {}, 
				(PropertiesFilterI[])null, 
				new GreaterOrEqualTimeFilter(Instant.now())
				));
		assertThrows(IllegalArgumentException.class, () -> new MessageFilter(
				new PropertyFilterI[] {}, 
				new PropertiesFilterI[] {}, 
				null
				));
	}
	
    // --------------------------
    // Tests with no filters
    // --------------------------
	
	@Test
	public void testNoFiltersAlwaysTrue() {
		MessageI message = new MessageTest.DummyMessage();
		message.putProperty("type", "sport");

		// no filters
		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] {},
				new PropertiesFilterI[] {},
				new JokerTimeFilter()
		);

		// returns true
		assertTrue(filter.match(message));
	}
	
    // --------------------------
    // Tests with one time filter
    // --------------------------
	
	@Test
	public void testTimeFilterPasses() {
		MessageI message = new MessageTest.DummyMessage();
		message.putProperty("type", "sport");

		// single time filter, initialized with a timestamp before message creation, accepts later timestamps
		TimeFilterI timeFilter = new GreaterOrEqualTimeFilter(Instant.now().minusSeconds(10));

		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] {},
				new PropertiesFilterI[] {},
				timeFilter
		);

		// returns true for the single filter
		assertTrue(filter.match(message));
	}

	@Test
	public void testTimeFilterFails() {
		MessageI message = new MessageTest.DummyMessage();
		message.putProperty("type", "sport");

		// single time filter, initialized with a timestamp after message creation, accepts later timestamps
		TimeFilterI timeFilter = new GreaterOrEqualTimeFilter(Instant.now().plusSeconds(10));

		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] {},
				new PropertiesFilterI[] {},
				timeFilter
		);

		// returns false for the single filter
		assertFalse(filter.match(message));
	}
	
    // --------------------------
    // Tests with multiple PropertyFilters
    // --------------------------
	
	@Test
	public void testMultiplePropertyFilterPasses() {
		MessageI message = new MessageTest.DummyMessage();
		message.putProperty("name", "Pierrot");
		message.putProperty("age", 47);
		message.putProperty("height", 1.75);

		// name == Pierrot -> true
		ValueFilterI valueFilterNom = new EqualToValueFilter<>("Pierrot");
		PropertyFilterI propertyFilterNom = new PropertyFilter("name", valueFilterNom);
		
		// age > 25 -> true
		ValueFilterI valueFilterAge = new GreaterThanValueFilter<>(25);
		PropertyFilterI propertyFilterAge = new PropertyFilter("age", valueFilterAge);
		
		// height < 1.78 -> true
		ValueFilterI valueFilterTaille = new LessThanValueFilter<>(1.78);
		PropertyFilterI propertyFilterTaille = new PropertyFilter("height", valueFilterTaille);

		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] { propertyFilterNom, propertyFilterAge, propertyFilterTaille },
				new PropertiesFilterI[] {},
				new JokerTimeFilter()
		);

		// returns true for all filters matching positively
		assertTrue(filter.match(message));
	}

	@Test
	public void testMultiplePropertyFilterFails() {
		MessageI message = new MessageTest.DummyMessage();
		message.putProperty("name", "Pierrot");
		message.putProperty("age", 47);
		message.putProperty("height", 1.85); // changed

		// name == Pierrot -> true
		ValueFilterI valueFilterNom = new EqualToValueFilter<>("Pierrot");
		PropertyFilterI propertyFilterNom = new PropertyFilter("name", valueFilterNom);
		
		// age > 25 -> true
		ValueFilterI valueFilterAge = new GreaterThanValueFilter<>(25);
		PropertyFilterI propertyFilterAge = new PropertyFilter("age", valueFilterAge);
		
		// height < 1.78 -> false
		ValueFilterI valueFilterTaille = new LessThanValueFilter<>(1.78);
		PropertyFilterI propertyFilterTaille = new PropertyFilter("height", valueFilterTaille);

		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] { propertyFilterNom, propertyFilterAge, propertyFilterTaille },
				new PropertiesFilterI[] {},
				new JokerTimeFilter()
		);

		// returns false because of height filter failing
		assertFalse(filter.match(message));
	}
	
    // --------------------------
    // Tests with multiple PropertiesFilters
    // --------------------------

	@Test
	public void testMultiplePropertiesFilterPasses() {

		MessageI message = new MessageTest.DummyMessage();
		message.putProperty("weight", 90.0);
		message.putProperty("muscle_weight", 50.0);
		message.putProperty("height", 1.70);

		// IMC (BMI) =~ 31.14 >= 30.0 -> true
		PropertiesFilterI propertiesFilterIMC = new PropertiesFilter(new IMC());
		
		// Muscle mass percentage =~ 55.5 >= 40.0 -> true
		PropertiesFilterI propertiesFilterMuscle = new PropertiesFilter(new Muscle());
		
		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] {},
				new PropertiesFilterI[] { propertiesFilterIMC, propertiesFilterMuscle },
				new JokerTimeFilter()
		);

		// returns true for all positive filters
		assertTrue(filter.match(message));
	}
	
	@Test
	public void testMultiplePropertiesFilterFails() {

		MessageI message = new MessageTest.DummyMessage();
		message.putProperty("weight", 90.0);
		message.putProperty("muscle_weight", 30.0); // changed
		message.putProperty("height", 1.70);

		// BMI =~ 31.14 >= 30.0 -> true
		PropertiesFilterI propertiesFilterIMC = new PropertiesFilter(new IMC());
		
		// Muscle mass percentage =~ 33.3 >= 40.0 -> false
		PropertiesFilterI propertiesFilterMuscle = new PropertiesFilter(new Muscle());
		
		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] {},
				new PropertiesFilterI[] { propertiesFilterIMC, propertiesFilterMuscle },
				new JokerTimeFilter()
		);

		// returns false due to muscle mass filter failing
		assertFalse(filter.match(message));
	}
	
    // --------------------------
    // Tests with everything
    // --------------------------
	
	@Test
	public void testMatchAllFiltersPasses() {
		MessageI message = new MessageTest.DummyMessage();
		message.putProperty("weight", 90.0);
		message.putProperty("muscle_weight", 50.0);
		message.putProperty("height", 1.70);
		
		/* TimeFilterI */
		
		TimeFilterI timeFilter = new GreaterOrEqualTimeFilter(Instant.now().minusSeconds(10));

		/* PropertyFilters */
		
		ValueFilterI valueFilterPoidsM = new EqualToValueFilter<>(50.0);
		PropertyFilterI propertyFilterPoidsM = new PropertyFilter("muscle_weight", valueFilterPoidsM);
		
		ValueFilterI valueFilterTaille = new GreaterThanValueFilter<>(1.6);
		PropertyFilterI propertyFilterTaille = new PropertyFilter("height", valueFilterTaille);

		/* PropertiesFilters */
		
		PropertiesFilterI propertiesFilterIMC = new PropertiesFilter(new IMC());
		PropertiesFilterI propertiesFilterMuscle = new PropertiesFilter(new Muscle());
		
		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] { propertyFilterPoidsM, propertyFilterTaille },
				new PropertiesFilterI[] { propertiesFilterIMC, propertiesFilterMuscle},
				timeFilter
		);

		// returns true for all positive filters
		assertTrue(filter.match(message));
	}
	
	@Test
	public void testMatchAllFiltersFails() {
		MessageI message = new MessageTest.DummyMessage();
		message.putProperty("weight", 90.0);
		message.putProperty("muscle_weight", 45.0); // changed
		message.putProperty("height", 1.70);
		
		/* TimeFilterI */
		
		TimeFilterI timeFilter = new GreaterOrEqualTimeFilter(Instant.now().minusSeconds(10));

		/* PropertyFilters */
		
		ValueFilterI valueFilterPoidsM = new EqualToValueFilter<>(50.0);
		PropertyFilterI propertyFilterPoidsM = new PropertyFilter("muscle_weight", valueFilterPoidsM);
		
		ValueFilterI valueFilterTaille = new GreaterThanValueFilter<>(1.6);
		PropertyFilterI propertyFilterTaille = new PropertyFilter("height", valueFilterTaille);

		/* PropertiesFilters */
		
		PropertiesFilterI propertiesFilterIMC = new PropertiesFilter(new IMC());
		PropertiesFilterI propertiesFilterMuscle = new PropertiesFilter(new Muscle());
		
		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] { propertyFilterPoidsM, propertyFilterTaille },
				new PropertiesFilterI[] { propertiesFilterIMC, propertiesFilterMuscle},
				timeFilter
		);

		// returns false because muscle weight filter fails
		assertFalse(filter.match(message));
	}
}