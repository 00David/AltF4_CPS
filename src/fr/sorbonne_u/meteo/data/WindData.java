package fr.sorbonne_u.meteo.data;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.meteo.interfaces.WindDataI;

/**
 * A wind described by a vector and a source position.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code position != null}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class WindData implements WindDataI {

	private static final long serialVersionUID = 8536953812168770777L;

	/** Source position of the wind vector */
	private final PositionData position;
	
	/** X component of the wind vector */
	private final double windX;
	/** Y component of the wind vector */
	private final double windY;
	
	/**
	 * Creates a wind described by a vector and a source position.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code position != null}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param position 						wind source position
	 * @param windX							X component of the wind vector
	 * @param windY							Y component of the wind vector
	 * @throws IllegalArgumentException		if the wind source position cannot is {@code null}.
	 */
	public WindData(PositionData position, double windX, double windY) {
		// Precondition check
		if (position == null) throw new IllegalArgumentException("The wind source position cannot be null");
		
		this.position = position;
		this.windX = windX;
		this.windY = windY;
	}
	
	@Override
	public PositionI getPosition() {
		return this.position;
	}

	@Override
	public double xComponent() {
		return windX;
	}

	@Override
	public double yComponent() {
		return windY;
	}

	@Override
	public double force() {
		return Math.sqrt(windX * windX + windY * windY);
	}
	
	@Override
	public String toString() {
		String res = "WindData\n";
		res += "Position : "+position+"\n";
		res += "Wind vector : X="+windX+" Y="+windY;
		return res;
	}

}
