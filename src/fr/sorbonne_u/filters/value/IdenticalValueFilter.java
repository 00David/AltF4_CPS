package fr.sorbonne_u.filters.value;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;

/**
 * Filtre ne gardant que les valeurs identiques à {@code filterValue} donné dans le constructeur.
 * 
 * <p>
 * Utilise la méthode {@code match} pour vérifier cela.
 * </p>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class IdenticalValueFilter implements ValueFilterI {

	private static final long serialVersionUID = 8651615691590385567L;
	private Serializable filterValue; 
	
	/**
	 * Construit le filtre ne gardant que les valeurs identiques à {@code filterValue}.
	 * @param filterValue La seule valeur acceptée du filtre.
	 * @throws IllegalArgumentException si {@code filterValue} == {@code null}.
	 */
	public IdenticalValueFilter(Serializable filterValue) {
		if (filterValue == null) throw new IllegalArgumentException("A filter value cannot be null");
		this.filterValue = filterValue;
	}
	
	@Override
	public boolean match(Serializable value){
		return filterValue.equals(value);
	}

}
