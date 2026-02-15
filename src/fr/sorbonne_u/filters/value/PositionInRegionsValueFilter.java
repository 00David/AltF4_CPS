package fr.sorbonne_u.filters.value;

import java.io.Serializable;
import java.util.List;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;

/**
 * Filtre indiquant si une {@link PositionI} (récupérée dans le constructeur)
 * est contenue dans une liste de {@link RegionI} (donnée en argument à {@code match}).
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class PositionInRegionsValueFilter implements ValueFilterI {
	
	private static final long serialVersionUID = 1041120887376161384L;
	private PositionI position; 

	/**
	 * Construit le filtre avec une {@link PositionI}.
	 * @param filterValue La position acceptée par au moins une {@link RegionI} donnée dans la liste en argument à {@code match}.
	 * @throws IllegalArgumentException si {@code position} == {@code null}.
	 */
	public PositionInRegionsValueFilter(PositionI position) {
		if (position == null) throw new IllegalArgumentException("A position cannot be null");
		this.position = position;
	}
	
	@Override
	public boolean match(Serializable value) {
		if(value instanceof List<?>) {
			List<RegionI> regions = (List<RegionI>)value;
			for (RegionI region : regions) {
				if (region.in(position)) return true; // Verifie que la position est dans au moins une région donnée
			}
		}
		return false;
	}

	

}
