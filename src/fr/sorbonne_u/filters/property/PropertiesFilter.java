package fr.sorbonne_u.filters.property;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.MultiValuesFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertiesFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;

/**
 * Filtre les valeurs de certaines propriétés données.
 * <p>
 * Les propriétés étant données à la méthode {@code match} sous la forme d'une liste de {@link PropertyI}, 
 * tandis que le {@link MultiValuesFilterI} associé est donné en argument au constructeur.
 * </p>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class PropertiesFilter implements PropertiesFilterI {

	private static final long serialVersionUID = 6231862455176712802L;
	private MultiValuesFilterI multiValuesFilter;
	
	/**
	 * Construit un filtre concernant plusieurs propriétés.
	 * @param multiValuesFilter Le filtre s'appliquant aux valeurs des propriétés concernées.
	 */
	public PropertiesFilter(MultiValuesFilterI multiValuesFilter) {
		if (multiValuesFilter == null) throw new IllegalArgumentException("A multi-values filter cannot be null");
		this.multiValuesFilter = multiValuesFilter;
	}

	@Override
	public MultiValuesFilterI getMultiValuesFilter() {
		return multiValuesFilter;
	}

	@Override
	public boolean match(PropertyI... properties) {
		// Verif préconditions
		if (properties == null) throw new IllegalArgumentException("Properties cannot be null");
		if (properties.length < 2) throw new IllegalArgumentException("At least 2 properties, not less");
		
	    String[] propertyNames = multiValuesFilter.getNames();
	    Serializable[] values = new Serializable[propertyNames.length];
		
	    // Pour chaque nom de propriété nécéssaire par le MultiValuesFilterI
	    for (int i = 0; i < propertyNames.length; i++) {
	        boolean found = false;

	        // On cherche la propriété correspondante dans les arguments
	        for (PropertyI p : properties) {
	            if (p.getName().equals(propertyNames[i])) {
	                values[i] = p.getValue();
	                found = true;
	                break;
	            }
	        }

	        if (!found) throw new IllegalArgumentException("Required property by the filter not found"); // propriété requise absente (verif précondition)
	    }

	    return multiValuesFilter.match(values);
	}

}
