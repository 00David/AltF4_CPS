package fr.sorbonne_u.filters.tests.utils;

import java.io.Serializable;

import fr.sorbonne_u.filters.value.AbstractMultiValuesFilter;

/**
 * Classe concrète d'exemple pour un filtre multi-valeurs.<br>
 * Considère une personne comme musclée (match renvoit true), pour le poids total et le poids musculaire d'une personne, 
 * si le poids musculaire représente au moins 40% du poids total de la personne.
 */
public class Muscle extends AbstractMultiValuesFilter{

	private static final long serialVersionUID = -5177956127310642977L;

	/**
	 * Construit notre classe de masse musculaire de test, en affectant directement les 2 noms de propriétés concernées.
	 */
	public Muscle() {
		super(new String[]{"poids", "poids_musculaire"});
	}

	/**
	 * @throws IllegalArgumentException si {@code values} == {@code null}.
	 * @throws IllegalArgumentException si moins de 2 valeurs.
	 * @throws IllegalArgumentException si une des 2 valeurs n'est pas flottante (propre à cette classe).
	 */
	@Override
	public boolean match(Serializable... values) {
		// Verif préconditions
		if (values == null) throw new IllegalArgumentException("values cannot be null");
		if (values.length < 2) throw new IllegalArgumentException("A multi-values filter cannot have less than 2 values");
		
		if (!(values[0] instanceof Double)) throw new IllegalArgumentException("'poids' not a double");
		double p = (double)values[0];
		if (!(values[1] instanceof Double)) throw new IllegalArgumentException("'poids_musculaire' not a double");
		double m = (double)values[1];
		return (m/p)*100.0 >= 40.0;
	}

}
