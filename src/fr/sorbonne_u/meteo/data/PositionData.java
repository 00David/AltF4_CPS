package fr.sorbonne_u.meteo.data;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;

/**
 * A (X,Y) coordinate couple.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code true}	// no more invariant
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class PositionData implements PositionI {
	
	private static final long serialVersionUID = -4464305380366957098L;
	
	/** X coordinate */
	private final double posX;
	/** Y coordinate */
	private final double posY;
	
	/**
	 * Creates a (X,Y) coordinate couple.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @param posX	The X coordinate component.
	 * @param posY	The Y coordinate component.
	 */
	public PositionData(double posX,double posY) {
		this.posX = posX;
		this.posY = posY;
	}
	
	/**
	 * Returns the X component of the couple.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @return the X component of the couple.
	 */
	public double getPosX() {
		return this.posX;
	}
	
	/**
	 * Returns the Y component of the couple.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
	 * @return the Y component of the couple.
	 */
	public double getPosY() {
		return this.posY;
	}
	
	/**
	 * Returns true if {@code p} is equals to {@code this}.
	 * 
	 * <ul>
	 * 	<li>If {@code p} is an instance of PositionData, we can test the equality on (X,Y) components.</li>
	 * 	<li>Otherwise, it returns false.</li>
	 * </ul>
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param p		a position to be compared to {@code this}.
	 * @return true	if {@code p} is equals to {@code this}, according to (X,Y) components.
	 */
	@Override
	public boolean equals(PositionI p) {
		if (p instanceof PositionData) {
			PositionData pos = (PositionData) p;
			return (this.posX == pos.posX) && (this.posY == pos.posY);
		} else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return "("+posX+","+posY+")";
	}
}
