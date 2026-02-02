package fr.sorbonne_u.filters.tests.utils;

import java.io.Serializable;

import fr.sorbonne_u.filters.value.MultiValuesFilter;

/**
 * Classe concrète d'exemple pour un filtre multi-valeurs, reprenant l'example de l'IMC.
 */
public class IMC extends MultiValuesFilter{

	private static final long serialVersionUID = -3520212719164037634L;

	/**
	 * Construit notre classe IMC de test, en affectant directement les 2 noms de propriétés concernées.
	 */
	public IMC() {
		super(new String[] {"poids", "taille"});
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
		if (!(values[1] instanceof Double)) throw new IllegalArgumentException("'taille' not a double");
		double t = (double)values[1];
		return (p/(t*t)) >= 30.0;
	}

}
