package fr.sorbonne_u.filters.value.meteo;

import java.io.Serializable;

import fr.sorbonne_u.filters.value.AbstractMultiValuesFilter;
import fr.sorbonne_u.messages.meteo.WindMessage;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;

/**
 * Filter accepting a given {@link WindData} values, when its informations are NOT above given thresholds.
 * 
 * <p>
 * 	Values received by the {@code match} method define a {@link WindData} :
 * 	<ul>
 * 		<li>The source {@link PositionData} of the wind vector.</li>
 * 		<li>The X component of the wind vector.</li>
 * 		<li>The Y component of the wind vector.</li>
 * 	</ul>
 * 
 * 
 * <p><strong>Invariants</strong>
 * <pre>
 * invariant	{@code position != null}
 * invariant	{@code distanceThreshold >= 0}
 * invariant	{@code forceThreshold >= 0}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class StrongWindMultiValuesFilter extends AbstractMultiValuesFilter{

	private static final long serialVersionUID = -8059319654493847469L;

	/** The initial position from where distances will be computed */
	private final PositionData position;
	
	/** The distance between {@code position} and the received {@link PositionData} value must not be above this threshold */
	private final double distanceThreshold;
	
	/** The force of the X and Y received wind component values must not be above this threshold */
	private final double forceThreshold;
	
	/**
	 * 
	 * Creates a filter accepting {@link WindData} informations, when not exceeding given thresholds.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code position != null}
	 * pre	{@code distanceThreshold >= 0}
	 * pre	{@code forceThreshold >= 0}
	 * post	class invariant satisfied
	 * </pre>
	 * 
	 * @param position				The position from where the distance will be computed.
	 * @param distanceThreshold		The maximum accepted distance between {@code position} and the wind source {@link PositionData}.
	 * @param forceThreshold 		The maximum accepted wind's force.
	 */
	public StrongWindMultiValuesFilter(PositionData position, double distanceThreshold, double forceThreshold) {
		super(new String[] {WindMessage.WIND_POSITION_PROPERTY, WindMessage.WIND_X_PROPERTY, WindMessage.WIND_Y_PROPERTY});
		// Precondition checks
		if (position == null) throw new IllegalArgumentException("A position cannot be null");
		if (distanceThreshold < 0) throw new IllegalArgumentException("distanceThreshold cannot be negative");
		if (forceThreshold < 0) throw new IllegalArgumentException("forceThreshold cannot be negative");
		
		this.position = position;
		this.distanceThreshold = distanceThreshold;
		this.forceThreshold = forceThreshold;
	}

	@Override
	public boolean match(Serializable... values) {
		if (!(values[0] instanceof PositionData) || !(values[1] instanceof Double) || !(values[2] instanceof Double)) {
			return false;
		}
		
		PositionData sourceWindPosition = (PositionData)values[0];
		double xWind = (double)values[1];
		double yWind = (double)values[2];
		WindData tmp = new WindData(sourceWindPosition, xWind, yWind); // WindData tmp gives us directly the 'force()' method
		
		double distX = position.getPosX() - sourceWindPosition.getPosX();
		double distY = position.getPosY() - sourceWindPosition.getPosY();
		
		if (Math.hypot(distX, distY) <= distanceThreshold && tmp.force() <= forceThreshold) {
			return true;
		} else {
			return false;
		}
	}

}
