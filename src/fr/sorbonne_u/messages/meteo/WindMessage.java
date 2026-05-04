package fr.sorbonne_u.messages.meteo;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import fr.sorbonne_u.cps.pubsub.exceptions.UnknownPropertyException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;

/**
 * A message containing a {@link WindData} data, tested in {@link MeteoMessagesTest}.
 * 
 * <p>
 * 	This class offers the possibility of directly getting specific wind data property values stored, through local getters.
 * 	However, it is still possible to first get the payload, cast it to a {@link WindData} instance, and use its getters.
 * </p>
 * 
 * <p>For ensuring a safe and coherent use, the class doesn't allow any modification related to the wind contained data :
 * 	<ul>
 * 		<li>The {@code putProperty} method does not allow to replace the value of a wind property.</li>
 * 		<li>The {@code removeProperty} method does not allow to remove a wind property.</li>
 * 		<li>The {@code setPayload} method usage is strictly forbidden : it would allow to replace the actual contained {@link WindData}.</li>
 * 	</ul>
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code this.getPayload().instanceof WindData}
 * invariant	{@code this.propertyExists(WIND_POSITION_PROPERTY)}
 * invariant	{@code this.propertyExists(WIND_X_PROPERTY)}
 * invariant	{@code this.propertyExists(WIND_Y_PROPERTY)}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class WindMessage extends Message {
	
	private static final long serialVersionUID = 3210811330259675964L;

	/** The name of the property related to a {@link PositionData} value. */
	public static final String WIND_POSITION_PROPERTY = "windPosition";
	
	/** The name of the property related to a double X wind vector value. */
	public static final String WIND_X_PROPERTY = "windX";
	
	/** The name of the property related to a double Y wind vector value. */
	public static final String WIND_Y_PROPERTY = "windY";
	
	/**
	 * Creates and initialized a message containing a {@link WindData}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code wind != null}
	 * pre	{@code wind.getPosition() instanceof PositionData}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param wind 							A wind data to store.
	 * @throws IllegalArgumentException 	If a precondition is not satisfied.
	 */
	public WindMessage(WindData wind) {
		super();
		// Precondition checks
		if (wind == null)
			throw new IllegalArgumentException("wind cannot be null");
		if (!(wind.getPosition() instanceof PositionData))
			throw new IllegalArgumentException("position must be instance of PositionData");
		
		super.setPayload(wind);
		super.putProperty(WIND_POSITION_PROPERTY, (PositionData)wind.getPosition());
		super.putProperty(WIND_X_PROPERTY, wind.xComponent());
		super.putProperty(WIND_Y_PROPERTY, wind.yComponent());
	}
	
	//--------------------------------------------------------------------------
	// RESTRICTED MESSAGE OPERATIONS
	//--------------------------------------------------------------------------
	
	/**
	 * @throws IllegalArgumentException if the property {@code name} is a wind property.
	 */
	@Override
	public void putProperty(String name, Serializable value) {
		ArrayList<String> windProperties = new ArrayList<>(Arrays.asList(
				WIND_POSITION_PROPERTY,
				WIND_X_PROPERTY,
				WIND_Y_PROPERTY
			));
		
		if (!windProperties.contains(name)) {
			super.putProperty(name, value); // delegates precondition verifications to the parent method
		} else {
			throw new IllegalArgumentException("cannot remplace an existing wind message property value");
		}
	}
	
	/**
	 * @throws IllegalArgumentException if the property {@code name} is a wind property.
	 */
	@Override
	public void removeProperty(String name) throws UnknownPropertyException {
		ArrayList<String> windProperties = new ArrayList<>(Arrays.asList(
				WIND_POSITION_PROPERTY,
				WIND_X_PROPERTY,
				WIND_Y_PROPERTY
			));
		
		if (!windProperties.contains(name)) {
			super.removeProperty(name); // delegates precondition verifications to the parent method
		} else {
			throw new IllegalArgumentException("cannot remove an existing wind message property");
		}
	}
	
	/**
	 * @throws IllegalArgumentException always thrown : otherwise it would have allowed to replace the contained {@link WindData}
	 * 			by something else.
	 */
	@Override
	public void setPayload(Serializable payload) {
		throw new IllegalArgumentException("cannot replace an existing wind message payload");
	}
	
	//--------------------------------------------------------------------------
	// WIND DATA GETTERS
	//--------------------------------------------------------------------------
	
	/**
	 * Returns the contained wind position.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	{@code true} // no postcondition
	 * </pre>
	 * 
	 * @return the contained wind position.
	 */
	public PositionData getPosition() {
		try {
			Serializable val = getPropertyValue(WIND_POSITION_PROPERTY);
			if (val instanceof PositionData) {
				return (PositionData)val;
			} else {
				return null;
			}
		} catch (UnknownPropertyException e) {
			return null;
		}
	}

	/**
	 * Returns the contained X wind vector component.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	{@code true} // no postcondition
	 * </pre>
	 * 
	 * @return the contained X wind vector component, or 0 if an error occurs.
	 */
	public double getX() {
		try {
			Serializable val = getPropertyValue(WIND_X_PROPERTY);
			if (val instanceof Double) {
				return (double)val;
			} else {
				return 0;
			}
		} catch (UnknownPropertyException e) {
			return 0;
		}
	}

	/**
	 * Returns the contained Y wind vector component.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	{@code true} // no postcondition
	 * </pre>
	 * 
	 * @return the contained Y wind vector component, or 0 if an error occurs.
	 */
	public double getY() {
		try {
			Serializable val = getPropertyValue(WIND_Y_PROPERTY);
			if (val instanceof Double) {
				return (double)val;
			} else {
				return 0;
			}
		} catch (UnknownPropertyException e) {
			return 0;
		}
	}
	
	//--------------------------------------------------------------------------
	// COPY
	//--------------------------------------------------------------------------
	
	@Override
	public MessageI copy() {
		ArrayList<String> windProperties = new ArrayList<>(Arrays.asList(
				WIND_POSITION_PROPERTY,
				WIND_X_PROPERTY,
				WIND_Y_PROPERTY
			));
		
		Message copiedMessage = new WindMessage((WindData)this.getPayload());
		for(PropertyI p : this.properties.values()) {
			
			if (!windProperties.contains(p.getName())) {
				copiedMessage.putProperty(p.getName(), p.getValue());
			}
		}
		
		assert(copiedMessage != null); // Postcondition check
		return copiedMessage;
	}
}
