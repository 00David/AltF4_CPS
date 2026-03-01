package fr.sorbonne_u.messages.tests;

import java.io.Serializable;

/**
 * Utility class for tests.
 * 
 * <p>Used by {@link MethodsMessageTest} for having a pair of strings : longitude / latitude</p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true} // no more invariant
 * </pre>
 */
public class LocationData implements Serializable {
	
	private static final long serialVersionUID = 6442774187550548688L;
	private String longitude;
	private String latitude;
	
	/**
	 * Creates a pair of strings : longitude / latitude
	 * @param longitude The longitude of the pair.
	 * @param latitude	The latitude of the pair.
	 */
	public LocationData(String longitude,String latitude) {
		this.longitude = longitude;
		this.latitude = latitude;
	}
	
	
	/**
	 * Returns the longitude of the pair.
	 * @return the longitude of the pair.
	 */
	public String getLongitude() {
		return this.longitude;
	}
	
	/**
	 * Returns the latitude of the pair.
	 * @return the latitude of the pair.
	 */
	public String getLatitude() {
		return this.latitude;
	}
}
