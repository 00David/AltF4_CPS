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
import fr.sorbonne_u.filters.value.comparable.EqualToValueFilter;
import fr.sorbonne_u.filters.value.comparable.GreaterThanValueFilter;
import fr.sorbonne_u.filters.value.comparable.LessThanValueFilter;
import fr.sorbonne_u.messages.Message;

/**
 * Tests unitaires de {@link MessageFilter}.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class MessageFilterTest {

    // --------------------------
    // Test du constructeur
    // --------------------------
	
	@Test
	public void testConstructor() {
		MessageFilter filter = new MessageFilter(new PropertyFilterI[] {}, new PropertiesFilterI[] {}, null);
		assertNotNull(filter);
		
		// constructeur avec 1 des 2 tableaux nuls : renvoit une IllegalArgumentException
		assertThrows(IllegalArgumentException.class, () -> new  MessageFilter((PropertyFilterI[])null, new PropertiesFilterI[] {}, null));
		assertThrows(IllegalArgumentException.class, () -> new  MessageFilter(new PropertyFilterI[] {}, (PropertiesFilterI[])null, null));
	}
	
    // --------------------------
    // Test avec aucun filtre
    // --------------------------
	
	@Test
	public void testNoFiltersAlwaysTrue() {
		MessageI message = new Message();
		message.putProperty("type", "sport");

		// aucun filtre
		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] {},
				new PropertiesFilterI[] {},
				null
		);

		// renvoit true
		assertTrue(filter.match(message));
	}
	
    // --------------------------
    // Tests avec 1 filtre temporel
    // --------------------------
	
	@Test
	public void testTimeFilterPasses() {
		MessageI message = new Message();
		message.putProperty("type", "sport");

		// 1 seul filtre temporel, initialisé avec un timestamp précédant la création du message, accepte les timestamps postérieurs
		TimeFilterI timeFilter = new GreaterOrEqualTimeFilter(Instant.now().minusSeconds(10));

		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] {},
				new PropertiesFilterI[] {},
				timeFilter
		);

		// renvoit true pour le seul filtre
		assertTrue(filter.match(message));
	}

	@Test
	public void testTimeFilterFails() {
		MessageI message = new Message();
		message.putProperty("type", "sport");

		// 1 seul filtre temporel, initialisé avec un timestamp suivant la création du message, accepte les timestamps postérieurs
		TimeFilterI timeFilter = new GreaterOrEqualTimeFilter(Instant.now().plusSeconds(10));

		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] {},
				new PropertiesFilterI[] {},
				timeFilter
		);

		// renvoit false pour le seul filtre
		assertFalse(filter.match(message));
	}
	
    // --------------------------
    // Tests avec plusieurs PropertyFilter
    // --------------------------
	
	@Test
	public void testMultiplePropertyFilterPasses() {
		MessageI message = new Message();
		message.putProperty("nom", "Pierrot");
		message.putProperty("age", 47);
		message.putProperty("taille", 1.75);

		// nom == Pierrot -> true
		ValueFilterI valueFilterNom = new EqualToValueFilter<>("Pierrot");
		PropertyFilterI propertyFilterNom = new PropertyFilter("nom", valueFilterNom);
		
		// age > 25 -> true
		ValueFilterI valueFilterAge = new GreaterThanValueFilter<>(25);
		PropertyFilterI propertyFilterAge = new PropertyFilter("age", valueFilterAge);
		
		// taille < 1.78 -> true
		ValueFilterI valueFilterTaille = new LessThanValueFilter<>(1.78);
		PropertyFilterI propertyFilterTaille = new PropertyFilter("taille", valueFilterTaille);

		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] { propertyFilterNom, propertyFilterAge, propertyFilterTaille },
				new PropertiesFilterI[] {},
				null
		);

		// renvoit true pour le match positif de tous les filtres
		assertTrue(filter.match(message));
	}

	@Test
	public void testMultiplePropertyFilterFails() {
		MessageI message = new Message();
		message.putProperty("nom", "Pierrot");
		message.putProperty("age", 47);
		message.putProperty("taille", 1.85); // changé

		// nom == Pierrot -> true
		ValueFilterI valueFilterNom = new EqualToValueFilter<>("Pierrot");
		PropertyFilterI propertyFilterNom = new PropertyFilter("nom", valueFilterNom);
		
		// age > 25 -> true
		ValueFilterI valueFilterAge = new GreaterThanValueFilter<>(25);
		PropertyFilterI propertyFilterAge = new PropertyFilter("age", valueFilterAge);
		
		// taille < 1.78 -> false
		ValueFilterI valueFilterTaille = new LessThanValueFilter<>(1.78);
		PropertyFilterI propertyFilterTaille = new PropertyFilter("taille", valueFilterTaille);

		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] { propertyFilterNom, propertyFilterAge, propertyFilterTaille },
				new PropertiesFilterI[] {},
				null
		);

		// renvoit false pour le match négatif du filtre sur la taille
		assertFalse(filter.match(message));
	}
	
    // --------------------------
    // Tests avec plusieurs PropertiesFilter
    // --------------------------

	@Test
	public void testMultiplePropertiesFilterPasses() {

		MessageI message = new Message();
		message.putProperty("poids", 90.0);
		message.putProperty("poids_musculaire", 50.0);
		message.putProperty("taille", 1.70);

		// IMC =~ 31,14 >= 30.0 -> true
		PropertiesFilterI propertiesFilterIMC = new PropertiesFilter(new IMC());
		
		// Pourcentage de masse musculaire =~ 55.5 >= 40.0 -> true
		PropertiesFilterI propertiesFilterMuscle = new PropertiesFilter(new Muscle());
		
		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] {},
				new PropertiesFilterI[] { propertiesFilterIMC, propertiesFilterMuscle },
				null
		);

		// renvoit true pour le match positif de tous les filtres
		assertTrue(filter.match(message));
	}
	
	@Test
	public void testMultiplePropertiesFilterFails() {

		MessageI message = new Message();
		message.putProperty("poids", 90.0);
		message.putProperty("poids_musculaire", 30.0); // changé
		message.putProperty("taille", 1.70);

		// IMC =~ 31,14 >= 30.0 -> true
		PropertiesFilterI propertiesFilterIMC = new PropertiesFilter(new IMC());
		
		// Pourcentage de masse musculaire =~ 33.3 >= 40.0 -> false
		PropertiesFilterI propertiesFilterMuscle = new PropertiesFilter(new Muscle());
		
		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] {},
				new PropertiesFilterI[] { propertiesFilterIMC, propertiesFilterMuscle },
				null
		);

		// renvoit false pour le match négatif du filtre sur le pourcentage de masse musculaire
		assertFalse(filter.match(message));
	}
	
    // --------------------------
    // Tests avec tout
    // --------------------------
	
	@Test
	public void testMatchAllFiltersPasses() {
		MessageI message = new Message();
		message.putProperty("poids", 90.0);
		message.putProperty("poids_musculaire", 50.0);
		message.putProperty("taille", 1.70);
		
		/* TimeFilterI */
		
		// 1 filtre temporel, initialisé avec un timestamp précédant la création du message, accepte les timestamps postérieurs
		TimeFilterI timeFilter = new GreaterOrEqualTimeFilter(Instant.now().minusSeconds(10));

		/* PropertyFilter */
		
		// poids_musculaire == 50.0 -> true
		ValueFilterI valueFilterPoidsM = new EqualToValueFilter<>(50.0);
		PropertyFilterI propertyFilterPoidsM = new PropertyFilter("poids_musculaire", valueFilterPoidsM);
		
		// taille > 1.6 -> true
		ValueFilterI valueFilterTaille = new GreaterThanValueFilter<>(1.6);
		PropertyFilterI propertyFilterTaille = new PropertyFilter("taille", valueFilterTaille);

		/* PropertiesFilter */
		
		// IMC =~ 31,14 >= 30.0 -> true
		PropertiesFilterI propertiesFilterIMC = new PropertiesFilter(new IMC());
		
		// Pourcentage de masse musculaire =~ 55.5 >= 40.0 -> true
		PropertiesFilterI propertiesFilterMuscle = new PropertiesFilter(new Muscle());
		
		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] { propertyFilterPoidsM, propertyFilterTaille },
				new PropertiesFilterI[] { propertiesFilterIMC, propertiesFilterMuscle},
				timeFilter
		);

		// renvoit true pour le match positif de tous les filtres
		assertTrue(filter.match(message));
	}
	
	@Test
	public void testMatchAllFiltersFails() {
		MessageI message = new Message();
		message.putProperty("poids", 90.0);
		message.putProperty("poids_musculaire", 45.0); // changé
		message.putProperty("taille", 1.70);
		
		/* TimeFilterI */
		
		// 1 filtre temporel, initialisé avec un timestamp précédant la création du message, accepte les timestamps postérieurs
		TimeFilterI timeFilter = new GreaterOrEqualTimeFilter(Instant.now().minusSeconds(10));

		/* PropertyFilter */
		
		// poids_musculaire == 50.0 -> false
		ValueFilterI valueFilterPoidsM = new EqualToValueFilter<>(50.0);
		PropertyFilterI propertyFilterPoidsM = new PropertyFilter("poids_musculaire", valueFilterPoidsM);
		
		// taille > 1.6 -> true
		ValueFilterI valueFilterTaille = new GreaterThanValueFilter<>(1.6);
		PropertyFilterI propertyFilterTaille = new PropertyFilter("taille", valueFilterTaille);

		/* PropertiesFilter */
		
		// IMC =~ 31,14 >= 30.0 -> true
		PropertiesFilterI propertiesFilterIMC = new PropertiesFilter(new IMC());
		
		// Pourcentage de masse musculaire =~ 50 >= 40.0 -> true
		PropertiesFilterI propertiesFilterMuscle = new PropertiesFilter(new Muscle());
		
		MessageFilter filter = new MessageFilter(
				new PropertyFilterI[] { propertyFilterPoidsM, propertyFilterTaille },
				new PropertiesFilterI[] { propertiesFilterIMC, propertiesFilterMuscle},
				timeFilter
		);

		// renvoit false pour le match négatif du filtre sur le poids musculaire
		assertFalse(filter.match(message));
	}
}