package fr.sorbonne_u.filters.value.meteo;

import java.io.Serializable;
import java.util.List;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.ValueFilterI;

/**
 * Filter indicating whether a {@link PositionI} (provided in the constructor)
 * is contained in a list of {@link RegionI} (given as an argument to {@code match}).
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code position != null}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class PositionInRegionsValueFilter implements ValueFilterI {
	
	private static final long serialVersionUID = 1041120887376161384L;
	
	/** 
	 * The filter accepts only regions containing this position.<br>
	 * Only 1 region of the list provided in the {@code match} method containing this position is enough for matching. 
	 */
	private PositionI position; 

	/**
	 * Constructs the filter with a {@link PositionI}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code position != null}
	 * post	class invariant satisfied
	 * </pre>
	 * 
	 * @param position 						The position accepted if it belongs to at least one {@link RegionI} given in the list argument to {@code match}.
	 * @throws IllegalArgumentException		if {@code position} == {@code null}.
	 */
	public PositionInRegionsValueFilter(PositionI position) {
		if (position == null) throw new IllegalArgumentException("A position cannot be null");
		this.position = position;
	}
	
	@Override
	public boolean match(Serializable value) throws ClassCastException {
		if (value == null) return false;
		
		if(value instanceof List<?>) { // Java does not authorize "value instanceof List<RegionI>"
			List<RegionI> regions = (List<RegionI>)value; // Not the cleanest thing, cannot do differently
			
			// Checks if the position is in at least one given region
			boolean posInOneRegion = false; 
			for (int i = 0; !posInOneRegion && i < regions.size(); i++) {
				RegionI region = regions.get(i);
				if (region.in(position)) {
					posInOneRegion = true;
				}
			}
			return posInOneRegion;
					
		} else {
			return false;
		}
	}

}
