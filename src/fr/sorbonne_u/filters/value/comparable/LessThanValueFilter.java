package fr.sorbonne_u.filters.value.comparable;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;

/**
 * Filtre ne gardant que les valeurs inférieures à {@code filterValue} donné dans le constructeur.
 * <p>
 * Les types implantant {@link Comparable}, le code de {@code match} utilise la méthode {@link Comparable#compareTo(Object)}.
 * </p>
 * @param <T> Le type des valeurs comparées du filtre, que ce soit la valeur de référence du filtre ou celle donnée en argument 
 * à {@code match}, implantant {@link Comparable}.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class LessThanValueFilter<T extends Comparable<T>> implements ValueFilterI {

	private static final long serialVersionUID = -7377931202662396565L;
	private T filterValue; 
	
	/**
	 * Construit le filtre gardant les valeurs inférieures à {@code filterValue}, de type implantant {@link Comparable}.
	 * @param filterValue La valeur du filtre.
	 * @throws IllegalArgumentException si {@code filterValue} == {@code null}.
	 */
	public LessThanValueFilter(T filterValue) {
		if (filterValue == null) throw new IllegalArgumentException("A filter value cannot be null");
		this.filterValue = filterValue;
	}
	
	@Override
	public boolean match(Serializable value) {
		@SuppressWarnings("unchecked")
		T v = (T)value; // ClassCastException possible ici
		return v.compareTo(filterValue) < 0; // NullPointerException possible ici si v == null
	}

}
