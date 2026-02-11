package fr.sorbonne_u.filters.value;

import java.io.Serializable;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;
import fr.sorbonne_u.meteo.data.RegionData;

public class PositionFilter extends ValueFilter {

	public PositionFilter(Serializable filterValue) {
		super(filterValue);
		
	}


	private static final long serialVersionUID = 1L;
	private Serializable filterValue; 
	
	
	/**
	 * Verifie que la position est bien dans la region
	 * <p><strong>Contrat</strong></p>
		<pre>
		pre
		post
		</pre>
	 */
	@Override
	public boolean match(Serializable value) {
		if(value instanceof RegionData) {
			return ((RegionData) value).in((PositionI)filterValue);
		}
		return false;
	}

	

}
