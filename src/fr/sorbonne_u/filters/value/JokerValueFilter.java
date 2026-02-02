package fr.sorbonne_u.filters.value;

import java.io.Serializable;

import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;

/**
 * Filtre acceptant n'importe quelle {@code value} donnée en paramètre à la méthode {@code match}.
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class JokerValueFilter implements ValueFilterI{

	private static final long serialVersionUID = -7611950551672657716L;

	@Override
	public boolean match(Serializable value) {
		return true;
	}

}
