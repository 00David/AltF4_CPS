package fr.sorbonne_u.filters.value;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.MultiValuesFilterI;

/**
 * Classe abstraite représentant un filtre prenant plusieurs valeurs dans sa méthode {@code match}.
 * 
 * <p>
 * Pour un filtre concret, il faut alors créer une classe étendant cette classe. Exemple avec : {@link fr.sorbonne_u.filters.tests.utils.IMC}.
 * </p>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public abstract class AbstractMultiValuesFilter implements MultiValuesFilterI {

	private static final long serialVersionUID = 1955588007790725754L;
	protected String[] propertyNames;
	
	/**
	 * Crée un filtre s'appliquant à plusieurs valeurs de propriétés, de noms {@code propertyNames} donnés.
	 * @param propertyNames Les noms des propriétés pour lesquelles les valeurs de {@code match} correspondent.
	 * @throws IllegalArgumentException si il y a moins de 2 propriétés concernées.
	 */
	public AbstractMultiValuesFilter(String[] propertyNames) {
        if (propertyNames == null || propertyNames.length < 2) {
            throw new IllegalArgumentException( "A multi-values filter must target at least two properties");
        }
		this.propertyNames = propertyNames;
	}
	
	@Override
	public String[] getNames() {
		return propertyNames;
	}

	@Override
	public abstract boolean match(Serializable... values);

}
