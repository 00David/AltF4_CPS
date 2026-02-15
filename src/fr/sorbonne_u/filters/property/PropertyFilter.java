package fr.sorbonne_u.filters.property;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI.PropertyI;

/**
 * Filtre la valeur d'une propriété. 
 * <p>
 * La propriété étant donnée à la méthode {@code match} sous la forme d'une {@link PropertyI}, 
 * tandis que le nom de cette propriété concernée et le {@link ValueFilterI} associé sont donnés en arguments au constructeur.
 * </p>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class PropertyFilter implements PropertyFilterI {

	private static final long serialVersionUID = -7695552208630589978L;
	
	protected String propertyName;
	protected ValueFilterI valueFilter;
	
	/**
	 * Crée un filtre sur une certaine propriété.
	 * @param propertyName Le nom de la propriété concernée.
	 * @param valueFilter Le filtre sur la valeur de la propriété.
	 * @throws IllegalArgumentException si {@code propertyName} == {@code null} ou {@code valueFilter} == {@code null}.
	 */
	public PropertyFilter(String propertyName, ValueFilterI valueFilter) {
        if (propertyName == null) throw new IllegalArgumentException("A property name cannot be null");
        if (valueFilter == null) throw new IllegalArgumentException("A value filter cannot be null");
		this.propertyName = propertyName;
		this.valueFilter = valueFilter;
	}

	@Override
	public String getName() {
		return propertyName;
	}

	@Override
	public ValueFilterI getValueFilter() {
		return valueFilter;
	}

	@Override
	public boolean match(PropertyI property) {
		return propertyName.equals(property.getName()) && valueFilter.match(property.getValue());
	}

}
