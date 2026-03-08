package fr.sorbonne_u.meteo.data;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;

/**
 * A region described by a rectangle.
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code xMin <= xMax}
 * invariant	{@code yMin <= yMax}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class RegionData implements RegionI {

	private static final long serialVersionUID = 4505980791444655950L;
	
	/** X lower bound of the region limit */
	private final double xMin;
	/** X upper bound of the region limit */
	private final double xMax;
	/** Y lower bound of the region limit */
	private final double yMin;
	/** Y upper bound of the region limit */
	private final double yMax;
	
	/**
	 * Creates a region described by a rectangle, by giving the rectangle's limits.
	 *
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code xMin <= xMax}
	 * pre	{@code yMin <= yMax}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param xMin minimum X bound of the rectangle.
	 * @param xMax maximum X bound of the rectangle.
	 * @param yMin minimum Y bound of the rectangle.
	 * @param yMax maximum Y bound of the rectangle.
	 */
	public RegionData(double xMin, double xMax, double yMin, double yMax) {
		// Precondition checks
		if (xMin > xMax) throw new IllegalArgumentException("X min cannot be greater than X max");
		if (yMin > yMax) throw new IllegalArgumentException("Y min cannot be greater than Y max");
		
		this.xMin = xMin;
		this.xMax = xMax;
		this.yMin = yMin;
		this.yMax = yMax;
	}
	
	/**
	 * Returns the limits of the rectangle describing the region.
	 * 
	 * <p>
	 * Limits are given in the order : xMin, xMax, yMin, yMax
	 * </p>
	 * 
	 * <pre>
	 * pre	{@code true} 	// no precondition.
	 * post	{@code return != null && return.length == 4}
	 * post	{@code return[0] == xMin && return[1] == xMax && return[2] == yMin && return[3] == yMax}
	 * </pre>
	 * 
	 * @return the limits of the rectangle describing the region.
	 */
	public double[] getLimits() {
		return new double[]{xMin, xMax, yMin, yMax}; // postconditions verified by construction
	}
	
	/**
	 * Return true if {@code p} is inside this region.<br>
	 * {@code p} must be an instance of {@link PositionData}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code p instanceof PositionData}
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 *
	 * @param p							a position to be compared.
	 * @return true						if {@code p} is inside this region.
	 * @throws IllegalArgumentException	if {@code p} is not an instance of {@link PositionData}.
	 */
	@Override
	public boolean in(PositionI p) {
		// Special case when the position is null, we return false
		if (p == null) return false;
		
		// Precondition check
		if (!(p instanceof PositionData)) throw new IllegalArgumentException("Position must be an instance of PositionData");
		
		PositionData pos = (PositionData) p; // If p is not a PositionData, we cannot access to its coordinates
		
		if(pos.getPosX() <= this.xMax && pos.getPosX() >= this.xMin 
				&& pos.getPosY() <= this.yMax && pos.getPosY() >= this.yMin) {
			return true;
		}
		return false;
	}
}
