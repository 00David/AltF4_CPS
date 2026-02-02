package fr.sorbonne_u.filters;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;

/**
 * Classe contenant l'ensemble des filtres à appliquer aux valeurs des propriétés d'un {@link MessageI}, via la méthode {@code match}.
 */
public class MessageFilter implements MessageFilterI{

	private static final long serialVersionUID = -6067517238603360377L;
	
	private PropertyFilterI[] propertyFilters;
	private PropertiesFilterI[] propertiesFilters;
	private TimeFilterI timeFilter;
	
	/**
	 * Construit un filtre de message.
	 * @param propertyFilters L'ensemble des {@link PropertyFilterI} à appliquer aux valeurs des propriétés des messages.
	 * @param propertiesFilters L'ensemble des {@link PropertiesFilterI} à appliquer aux valeurs des propriétés des messages.
	 * @param timeFilter Un {@link TimeFilterI} à appliquer au timestamp du message. Peut être {@code null}.
	 * @throws IllegalArgumentException si {@code propertyFilters} == {@code null} ou {@code propertiesFilters} == {@code null}.
	 */
	public MessageFilter(PropertyFilterI[] propertyFilters, PropertiesFilterI[] propertiesFilters, TimeFilterI timeFilter) {
		if (propertyFilters == null || propertiesFilters == null) {
			throw new IllegalArgumentException("An array cannot be null");
		}
		this.propertyFilters = propertyFilters;
		this.propertiesFilters = propertiesFilters;
		this.timeFilter = timeFilter;
	}

	@Override
	public PropertyFilterI[] getPropertyFilters() {
		return propertyFilters;
	}

	@Override
	public PropertiesFilterI[] getPropertiesFilters() {
		return propertiesFilters;
	}

	@Override
	public TimeFilterI getTimeFilter() {
		return timeFilter;
	}

	@Override
	public boolean match(MessageI message) {

	    // 1. Filtre temporel
	    if (timeFilter != null && !timeFilter.match(message.getTimeStamp())) {
	        return false;
	    }

	    PropertyI[] properties = message.getProperties();

	    // 2. Filtres sur propriétés individuelles
	    for (PropertyFilterI pf : propertyFilters) {
	        boolean matched = false;

	        for (PropertyI p : properties) {
	            if (pf.match(p)) {
	                matched = true;
	                break;
	            }
	        }

	        // La propriété attendue par le filtre n'a pas matché
	        if (!matched) return false;
	    }

	    // 3. Filtres sur propriétés conjointes
	    for (PropertiesFilterI pf : propertiesFilters) {
	        if (!pf.match(properties)) return false;
	    }

	    return true;
	}

}
