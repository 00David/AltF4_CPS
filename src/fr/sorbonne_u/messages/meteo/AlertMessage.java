package fr.sorbonne_u.messages.meteo;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.cps.pubsub.exceptions.UnknownPropertyException;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.AlertType;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.Level;
import fr.sorbonne_u.messages.Message;
import fr.sorbonne_u.meteo.data.MeteoAlert;

/**
 * A message containing a {@link MeteoAlert} data.
 * 
 * <p>
 * 	This class offers the possibility of directly getting specific meteo alert property values stored, through local getters.
 * 	However, it is still possible to first get the payload, cast it to a {@link MeteoAlert} instance, and use its getters.
 * </p>
 * 
 * <p>For ensuring a safe and coherent use, the class doesn't allow any modification related to the meteo alert contained data :
 * 	<ul>
 * 		<li>The {@code putProperty} method does not allow to replace the value of a meteo alert property.</li>
 * 		<li>The {@code removeProperty} method does not allow to remove a meteo alert property.</li>
 * 		<li>The {@code setPayload} method usage is strictly forbidden : it would allow to replace the actual contained {@link MeteoAlert}.</li>
 * 	</ul>
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code this.getPayload().instanceof MeteoAlert}
 * invariant	{@code this.propertyExists(ALERT_TYPE_PROPERTY)}
 * invariant	{@code this.propertyExists(ALERT_LEVEL_PROPERTY)}
 * invariant	{@code this.propertyExists(ALERT_REGIONS_PROPERTY)}
 * invariant	{@code this.propertyExists(ALERT_START_PROPERTY)}
 * invariant	{@code this.propertyExists(ALERT_DURATION_PROPERTY)}
 * </pre>
 * 
 * @author Kevin Tang
 * @author David Vadimon
 */
public class AlertMessage extends Message {

	private static final long serialVersionUID = 2262257839882150943L;
	
	/** The name of the property related to a {@link AlertType} value. */
	public static final String ALERT_TYPE_PROPERTY = "alertType";
	
	/** The name of the property related to a {@link Level} value. */
	public static final String ALERT_LEVEL_PROPERTY = "alertLevel";
	
	/** The name of the property related to a List of {@link RegionI} value. */
	public static final String ALERT_REGIONS_PROPERTY = "alertRegions";
	
	/** The name of the property related to a start {@link Instant} value. */
	public static final String ALERT_START_PROPERTY = "alertStartTime";
	
	/** The name of the property related to a {@link Duration} value. */
	public static final String ALERT_DURATION_PROPERTY = "alertDuration";
	
	/**
	 * Creates and initialized a message containing a {@link MeteoAlert}.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code meteoAlert != null}
	 * pre	{@code meteoAlert.getAlertType() instanceof AlertType}
	 * pre	{@code meteoAlert.getLevel() instanceof Level}
	 * post	class invariants satisfied
	 * </pre>
	 * 
	 * @param meteoAlert 					A meteo alert data to store.
	 * @throws IllegalArgumentException 	If a precondition is not satisfied.
	 */
	public AlertMessage(MeteoAlert meteoAlert) {
		super();
		// Precondition checks
		if (meteoAlert == null)
			throw new IllegalArgumentException("meteoAlert cannot be null");
		if (!(meteoAlert.getAlertType() instanceof AlertType))
			throw new IllegalArgumentException("alert type must be instance of AlertType");
		if (!(meteoAlert.getLevel() instanceof Level))
			throw new IllegalArgumentException("alert level must be instance of Level");
		
		RegionI[] regionsArray = meteoAlert.getRegions();
		ArrayList<RegionI> regionsList = new ArrayList<>();
		for (int i=0; i<regionsArray.length; i++) {
			regionsList.add(regionsArray[i]);
		}
		
		super.setPayload(meteoAlert);
		super.putProperty(ALERT_TYPE_PROPERTY, (AlertType)meteoAlert.getAlertType());
		super.putProperty(ALERT_LEVEL_PROPERTY, (Level)meteoAlert.getLevel());
		super.putProperty(ALERT_REGIONS_PROPERTY, regionsList);
		super.putProperty(ALERT_START_PROPERTY, meteoAlert.getStartTime());
		super.putProperty(ALERT_DURATION_PROPERTY, meteoAlert.getDuration());
	}
	
	//--------------------------------------------------------------------------
	// RESTRICTED MESSAGE OPERATIONS
	//--------------------------------------------------------------------------
	
	/**
	 * @throws IllegalArgumentException if the property {@code name} is a meteo alert property.
	 */
	@Override
	public void putProperty(String name, Serializable value) {
		ArrayList<String> alertProperties = new ArrayList<>(Arrays.asList(
			    ALERT_TYPE_PROPERTY,
			    ALERT_LEVEL_PROPERTY,
			    ALERT_REGIONS_PROPERTY,
			    ALERT_START_PROPERTY,
			    ALERT_DURATION_PROPERTY
			));
		
		if (!alertProperties.contains(name)) {
			super.putProperty(name, value); // delegates precondition verifications to the parent method
		} else {
			throw new IllegalArgumentException("cannot remplace an existing meteo alert message property value");
		}
	}
	
	/**
	 * @throws IllegalArgumentException if the property {@code name} is a meteo alert property.
	 */
	@Override
	public void removeProperty(String name) throws UnknownPropertyException {
		ArrayList<String> alertProperties = new ArrayList<>(Arrays.asList(
			    ALERT_TYPE_PROPERTY,
			    ALERT_LEVEL_PROPERTY,
			    ALERT_REGIONS_PROPERTY,
			    ALERT_START_PROPERTY,
			    ALERT_DURATION_PROPERTY
			));
		
		if (!alertProperties.contains(name)) {
			super.removeProperty(name); // delegates precondition verifications to the parent method
		} else {
			throw new IllegalArgumentException("cannot remove an existing meteo alert message property");
		}
	}
	
	/**
	 * @throws IllegalArgumentException always thrown : otherwise it would have allowed to replace the contained {@link MeteoAlert}
	 * 			by something else.
	 */
	@Override
	public void setPayload(Serializable payload) {
		throw new IllegalArgumentException("cannot replace an existing meteo alert message payload");
	}
	
	//--------------------------------------------------------------------------
	// METEO ALERT DATA GETTERS
	//--------------------------------------------------------------------------
	
	/**
	 * Returns the contained alert type.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	{@code true} // no postcondition
	 * </pre>
	 * 
	 * @return the contained alert type.
	 */
	public AlertType getAlertType() {
		try {
			Serializable val = getPropertyValue(ALERT_TYPE_PROPERTY);
			if (val instanceof AlertType) {
				return (AlertType)val;
			} else {
				return null;
			}
		} catch (UnknownPropertyException e) {
			return null;
		}
	}

	/**
	 * Returns the contained alert level.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	{@code true} // no postcondition
	 * </pre>
	 * 
	 * @return the contained alert level.
	 */
	public Level getLevel() {
		try {
			Serializable val = getPropertyValue(ALERT_LEVEL_PROPERTY);
			if (val instanceof Level) {
				return (Level)val;
			} else {
				return null;
			}
		} catch (UnknownPropertyException e) {
			return null;
		}
	}

	/**
	 * Returns the contained alert regions.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	{@code true} // no postcondition
	 * </pre>
	 * 
	 * @return the contained alert regions.
	 */
	@SuppressWarnings("unchecked") // List created in the constructor, necessarily the case here
	public List<RegionI> getRegions() {
		try {
			Serializable val = getPropertyValue(ALERT_REGIONS_PROPERTY);
			if (val instanceof List<?>) {
				return (List<RegionI>)val;
			} else {
				return null;
			}
		} catch (UnknownPropertyException e) {
			return null;
		}
	}

	/**
	 * Returns the contained alert start instant.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	{@code true} // no postcondition
	 * </pre>
	 * 
	 * @return the contained alert start instant.
	 */
	public Instant getStartTime() {
		try {
			Serializable val = getPropertyValue(ALERT_START_PROPERTY);
			if (val instanceof Instant) {
				return (Instant)val;
			} else {
				return null;
			}
		} catch (UnknownPropertyException e) {
			return null;
		}
	}

	/**
	 * Returns the contained alert duration.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true} // no precondition
	 * post	{@code true} // no postcondition
	 * </pre>
	 * 
	 * @return the contained alert duration.
	 */
	public Duration getDuration() {
		try {
			Serializable val = getPropertyValue(ALERT_DURATION_PROPERTY);
			if (val instanceof Duration) {
				return (Duration)val;
			} else {
				return null;
			}
		} catch (UnknownPropertyException e) {
			return null;
		}
	}
	
	//--------------------------------------------------------------------------
	// COPY
	//--------------------------------------------------------------------------
	
	@Override
	public MessageI copy() {
		ArrayList<String> alertProperties = new ArrayList<>(Arrays.asList(
			    ALERT_TYPE_PROPERTY,
			    ALERT_LEVEL_PROPERTY,
			    ALERT_REGIONS_PROPERTY,
			    ALERT_START_PROPERTY,
			    ALERT_DURATION_PROPERTY
			));
		
		Message copiedMessage = new AlertMessage((MeteoAlert)this.getPayload());
		for(PropertyI p : this.properties.values()) {
			
			if (!alertProperties.contains(p.getName())) {
				copiedMessage.putProperty(p.getName(), p.getValue());
			}
		}
		
		assert(copiedMessage != null); // Postcondition check
		return copiedMessage;
	}
}
