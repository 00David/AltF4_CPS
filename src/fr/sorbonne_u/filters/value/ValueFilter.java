package fr.sorbonne_u.filters.value;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;

/**
 * Filtre ne gardant que les valeurs égales à {@code filterValue} donné dans le constructeur.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class ValueFilter implements ValueFilterI {

	private static final long serialVersionUID = 8418130044029451842L;
	private Serializable filterValue; 
	
	/**
	 * Construit le filtre ne gardant que les valeurs égales à {@code filterValue}.
	 * @param filterValue La seule valeur acceptée du filtre.
	 * @throws IllegalArgumentException si {@code filterValue} == {@code null}.
	 */
	public ValueFilter(Serializable filterValue) {
		if (filterValue == null) throw new IllegalArgumentException("A filter value cannot be null");
		this.filterValue = filterValue;
	}
	
	@Override
	public boolean match(Serializable value){
		return filterValue.equals(value);
	}

}
