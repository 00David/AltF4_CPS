package fr.sorbonne_u.meteo.components;

import java.time.Instant;
import java.util.ArrayList;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.LevelI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.PreconditionException;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.property.PropertiesFilter;
import fr.sorbonne_u.filters.property.PropertyFilter;
import fr.sorbonne_u.filters.time.GreaterOrEqualTimeFilter;
import fr.sorbonne_u.filters.value.meteo.PositionInRegionsValueFilter;
import fr.sorbonne_u.meteo.data.MeteoAlert;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;
import fr.sorbonne_u.meteo.plugins.WindTurbineReceptionPlugin;
import fr.sorbonne_u.meteo.tests.WindTurbineTest;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;
import fr.sorbonne_u.exceptions.PostconditionException;

/**
 * The WindTurbine component is a {@link Client} of the system.
 * 
 * <p>
 * It receives messages sent by meteorological stations/offices and takes
 * appropriate decisions for its orientation or safety mode.
 * </p>
 * 
 * <p>
 * A classic WindTurbine is necessarily a STANDARD client in this class :
 * this is mainly explained by the necessity to be able to interact with privileged channels, in addition of default channels.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code datas != null}
 * invariant	{@code pos != null}
 * invariant	{@code orientation != null}
 * invariant	{@code maxWorkingLevel != null}
 * invariant	{@code subscriptionPlugin != null && subscriptionPlugin.isInitialised()}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class WindTurbine extends Client {

	/** The component position */
	protected final PositionData windTurbinePos;
	
	/** Stores wind data received from messages */
	protected final ArrayList<WindData> datas;

	/** The orientation of the wind turbine. PositionData is used here as a vector, for the WindTurbine orientation */
	protected PositionData orientation;
	
	/** 
	 * Maximum alert level (inclusive) at which the turbine operates.<br>
	 * If a higher level is received, the turbine enters safety mode.
	 */
	protected final LevelI maxWorkingLevel;

	/** Indicates whether the turbine is in safety mode or not (false by default) */
	protected boolean safeMode;
	
	// SUBSCRIPTION + RECEPTION
	
	/** Subscription + reception plugin */
	protected final WindTurbineReceptionPlugin subscriptionPlugin;
	
	
	/** 
	 * Indicates if the wind turbine must register itself with default filters on channels "channel0" and "channel1".<br>
	 * Test sub-classes can set this boolean at false, otherwise it is set à true by default.
	 */
	protected boolean defaultReceptionOnExec;

	//--------------------------------------------------------------------------
	// Constructors
	//--------------------------------------------------------------------------
	
	/**
     * Creates a wind turbine component for the publish/subscribe system, extending a {@link Client}.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
     * pre  {@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
     * pre  {@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
     * pre  {@code pos != null}
     * pre  {@code maxWorkingLevel != null}
     * post class invariants satisfied
     * </pre>
     * 
     * @param receivingInboundPortURI     URI of the inbound port providing {@link ReceivingCI}.
     * @param registrationOutboundPortURI URI of the outbound port requesting {@link RegistrationCI}.
     * @param publishingOutboundPortURI   URI of the outbound port requesting {@link PublishingCI}.
	 * @param nbThreads					  Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads		  Number of threads to be created in the component schedulable pool.
     * @param windTurbinePos              The position of the wind turbine.
     * @param maxWorkingLevel             Maximum alert level (inclusive) at which the turbine operates.
     * @throws Exception                  If a problem occurs during initialization or port publication.
     */
	protected WindTurbine(String receivingInboundPortURI, String registrationOutboundPortURI, String publishingOutboundPortURI, 
			int nbThreads, int nbSchedulableThreads, PositionData windTurbinePos,LevelI maxWorkingLevel) throws Exception {
		super(receivingInboundPortURI, "registration", registrationOutboundPortURI, RegistrationClass.STANDARD, nbThreads, nbSchedulableThreads);
		
		// Some preconditions are checked inside the superconstructor while others are checked here
		assert (windTurbinePos != null) : new PreconditionException("WindTurbine position cannot be null");	
		assert (maxWorkingLevel != null) : new PreconditionException("max working level cannot be null");	

		this.windTurbinePos = windTurbinePos;
		this.datas = new ArrayList<WindData>();
		this.orientation = new PositionData(0, 0);
		this.safeMode = false;
		this.maxWorkingLevel = maxWorkingLevel;
		this.defaultReceptionOnExec = true;
		
		// Install the subscription + reception plugin (Wind turbine version)
		this.subscriptionPlugin = new WindTurbineReceptionPlugin(receivingInboundPortURI, registrationPlugin.getRegistrationOutboundPort());
		this.subscriptionPlugin.setPluginURI("subscription");
		this.installPlugin(this.subscriptionPlugin);
		
		// Invariant checks
		AbstractComponent.checkImplementationInvariant(this);
		AbstractComponent.checkInvariant(this);
		
		// Postcondition checks
		assert subscriptionPlugin.isInitialised() : 
			new PostconditionException("Subscription (+reception) plugin must be initialized");
		assert	this.isPortExisting(receivingInboundPortURI) : 
			new PostconditionException("The component must have a port with URI " + receivingInboundPortURI);
		assert	this.findPortFromURI(receivingInboundPortURI).getImplementedInterface().equals(ReceivingCI.class) : 
			new PostconditionException("The component must have a port with implemented interface ReceivingCI");
		assert	this.findPortFromURI(receivingInboundPortURI).isPublished() :
			new PostconditionException("The component must have a port published with URI " + receivingInboundPortURI);
		
		this.getTracer().setTitle("WindTurbine");
		this.getTracer().setRelativePosition((int)windTurbinePos.getPosX(), (int)windTurbinePos.getPosY());
	}
	
	/**
     * Creates a wind turbine component for the publish/subscribe system, extending a {@link Client}, by providing 
	 * the {@code reflectionInboundPortURI} of the component.
     * 
     * <p>Constructor used by test sub-class {@link WindTurbineTest}.</p>
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
     * pre  {@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
     * pre  {@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
     * pre  {@code pos != null}
     * pre  {@code maxWorkingLevel != null}
	 * pre	{@code reflectionInboundPortURI != null && !reflectionInboundPortURI.isEmpty()}
     * post class invariants satisfied
     * </pre>
     * 
     * @param receivingInboundPortURI     URI of the inbound port providing {@link ReceivingCI}.
     * @param registrationOutboundPortURI URI of the outbound port requesting {@link RegistrationCI}.
     * @param publishingOutboundPortURI   URI of the outbound port requesting {@link PublishingCI}.
	 * @param nbThreads					  Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads		  Number of threads to be created in the component schedulable pool.
     * @param windTurbinePos              The position of the wind turbine.
     * @param maxWorkingLevel             Maximum alert level (inclusive) at which the turbine operates.
	 * @param reflectionInboundPortURI	  URI of the reflection inbound port to be created in this component.
     * @throws Exception                  If a problem occurs during initialization or port publication.
     */
	protected WindTurbine(String receivingInboundPortURI, String registrationOutboundPortURI, String publishingOutboundPortURI, 
			int nbThreads, int nbSchedulableThreads, PositionData windTurbinePos, LevelI maxWorkingLevel, String reflectionInboundPortURI) throws Exception {
		super(receivingInboundPortURI, "registration", registrationOutboundPortURI, RegistrationClass.STANDARD, nbThreads, nbSchedulableThreads, reflectionInboundPortURI);
		
		// Some preconditions are checked inside the superconstructor while others are checked here
		assert (windTurbinePos != null) : new PreconditionException("WindTurbine position cannot be null");	
		assert (maxWorkingLevel != null) : new PreconditionException("max working level cannot be null");	

		this.windTurbinePos = windTurbinePos;
		this.datas = new ArrayList<WindData>();
		this.orientation = new PositionData(0, 0);
		this.safeMode = false;
		this.maxWorkingLevel = maxWorkingLevel;
		this.defaultReceptionOnExec = true;
		
		// Install the subscription + reception plugin (Wind turbine version)
		this.subscriptionPlugin = new WindTurbineReceptionPlugin(receivingInboundPortURI, registrationPlugin.getRegistrationOutboundPort());
		this.subscriptionPlugin.setPluginURI("subscription");
		this.installPlugin(this.subscriptionPlugin);
		
		// Invariant checks
		AbstractComponent.checkImplementationInvariant(this);
		AbstractComponent.checkInvariant(this);
		
		// Postcondition checks
		assert subscriptionPlugin.isInitialised() : 
			new PostconditionException("Subscription (+reception) plugin must be initialized");
		assert	this.isPortExisting(receivingInboundPortURI) : 
			new PostconditionException("The component must have a port with URI " + receivingInboundPortURI);
		assert	this.findPortFromURI(receivingInboundPortURI).getImplementedInterface().equals(ReceivingCI.class) : 
			new PostconditionException("The component must have a port with implemented interface ReceivingCI");
		assert	this.findPortFromURI(receivingInboundPortURI).isPublished() :
			new PostconditionException("The component must have a port published with URI " + receivingInboundPortURI);
		
		this.getTracer().setTitle("WindTurbine");
		this.getTracer().setRelativePosition((int)windTurbinePos.getPosX(), (int)windTurbinePos.getPosY());
	}

	// --------------------------------------------------------------------------
	// Component life-cycle
	// --------------------------------------------------------------------------

	@Override
	public void execute() throws Exception {
		super.execute();
		this.logMessage("executing WindTurbine component");

		// Register on the Broker
		registrationPlugin.register(registrationClass);
		assert (registrationPlugin.getBrokerPublishingPortURI() != null);
		this.logMessage("WindTurbine registered");

		if (defaultReceptionOnExec) startWindTurbine();
	}

    /**
     * Registers the wind turbine to channels for receiving wind data and alerts.
     * 
     * <p>
     * <li> Only considers messages created after the turbine registration.
     * <li> For alerts, only considers alerts where one of the affected regions contains the turbine's position.
     * </p>
     * 
     * <p>
     * <li> Wind datas are received from "channel0".
     * <li> Meteorological alerts are received from "channel1".
     * </p>
     * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
     * 
     * @throws Exception If a problem occurs while connecting ports.
     */
	protected void startWindTurbine() throws Exception {

        // Create a filter only accepting messages after the turbine subscription
        TimeFilterI timeFilter = new GreaterOrEqualTimeFilter(Instant.now());

        // Wind data subscription
        subscriptionPlugin.subscribe("channel0", new MessageFilter(new PropertyFilter[0], new PropertiesFilter[0], timeFilter));
        this.logMessage("WindTurbine subscribed to channel0");

        // Alert data subscription
        PropertyFilterI[] propFiltersAlert = new PropertyFilterI[1];
        // Create a filter accepting regions containing the wind turbine position
        propFiltersAlert[0] = new PropertyFilter("regions", new PositionInRegionsValueFilter(this.windTurbinePos));

        subscriptionPlugin.subscribe("channel1", new MessageFilter(propFiltersAlert, new PropertiesFilter[0], timeFilter));
        this.logMessage("WindTurbine subscribed to channel1");
    }
	
	// --------------------------------------------------------------------------
	// Component public methods
	// --------------------------------------------------------------------------
	
	/**
	 * Indicates if the wind turbine is in safe mode.
	 * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre  {@code true}	// no precondition.
	 * post	{@code true} 	// no postcondition.
	 * </pre>
	 * 
	 * @return true		if wind turbine is in safe mode, otherwise it operates normally.
	 */
	public boolean inSafeMode() {
		return safeMode;
	}

    /**
     * Calculates the orientation to adopt according to the collected wind data so far + the {@code newWindData} added.
     * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre  {@code true}	// no precondition.
	 * post	{@code datas.contains(newWindData)}
	 * </pre>
     */
	public void considerNewWindData(WindData newWindData) {
		if (newWindData != null) datas.add(newWindData);
		
	    double x = 0;
	    double y = 0;
	    double sumWeights = 0;

	    for (WindData d : datas) {
	        double distance = distance((PositionData) d.getPosition()); // having a WindData implies that its position is a 'PositionData'

	        double weight;
	        if (distance == 0) {
	            weight = 1.0;
	        } else {
	            weight = 1.0 / distance;
	        }

	        x += d.xComponent() * weight;
	        y += d.yComponent() * weight;

	        sumWeights += weight;
	    }

	    if (sumWeights != 0) {
	        x = x / sumWeights;
	        y = y / sumWeights;
	    }

	    // Reverse vector direction
	    x = -x;
	    y = -y;

		this.orientation = new PositionData(x, y);
		this.logMessage("WindTurbine has changed orientation : now = " + this.orientation);
		this.logMessage("------------------------------");
		
		// Postcondition check
		assert(datas.contains(newWindData)) : new PostconditionException("New wind data must be considered");
	}
	
    /**
     * Reacts to the received alert level according to operation threshold.
     * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre  {@code true}	// no precondition.
	 * post	{@code true}	// no postcondition.
	 * </pre>
	 * 
     * @param alert Received alert.
     */
	public void considerNewAlert(MeteoAlert alert) {
		if (alert == null) {
			this.logMessage("------------------------------");
			return;
		}

		MeteoAlertI.Level maxLevel = (MeteoAlertI.Level) this.maxWorkingLevel;
		MeteoAlertI.Level alertLevel = (MeteoAlertI.Level) alert.getLevel();

		if (alertLevel.ordinal() > maxLevel.ordinal() && !safeMode) {
			goInSafeMode();
			this.datas.clear(); // Erase previous wind datas
		} else if (alertLevel.ordinal() <= maxLevel.ordinal() && safeMode) {
			goBacktoNormalMode();
		
		// Don't change anything, if we have :
		// - received an alert above the maximum operating level while already in safe mode
		// - received an alert within the operating range while already in normal operation (safeMode == false)
		} else {
			this.logMessage("------------------------------");
		}
	}
	
	// --------------------------------------------------------------------------
	// Component internal methods
	// --------------------------------------------------------------------------

    /**
     * Computes the distance between the turbine and a meteorological station.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code stationPos != null}
     * post {@code true} // no postcondition.
     * </pre>
     * 
     * @param stationPos	Position of the meteorological station.
     * @return				the distance between the turbine and a {@code stationPos}.
     */
	protected double distance(PositionData stationPos) {
		// Precondition check
		assert(stationPos != null) : new PreconditionException("stationPos cannot be null");	
		
		double dx = stationPos.getPosX() - this.windTurbinePos.getPosX();
		double dy = stationPos.getPosY() - this.windTurbinePos.getPosY();
		return Math.sqrt(dx * dx + dy * dy);
	}
	
	/**
	 * WindTurbine enters safe mode, stops operating.
	 * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre {@code true}	// no precondition.
	 * post {@code this.safemode == true}
	 * </pre>
	 */
	protected void goInSafeMode() {
		if (!this.safeMode) {
			this.logMessage("WindTurbine going into security mode.");
			this.logMessage("------------------------------");
			this.safeMode = true;
		}
		// Postcondition check
		assert(this.safeMode == true) : new PostconditionException("Safe mode must be turned on");
	}

	/**
	 * WindTurbine returns to normal operation.
	 * 
	 * <p><strong>Contrat</strong></p>
	 * <pre>
	 * pre {@code true}	// no precondition.
	 * post {@code this.safemode == false}
	 * </pre>
	 */
	protected void goBacktoNormalMode() {
		if (this.safeMode) {
			this.logMessage("WindTurbine returning to normal operation.");
			this.logMessage("------------------------------");
			this.safeMode = false;
		}
		// Postcondition check
		assert(this.safeMode == false) : new PostconditionException("Safe mode must be turned off");
	}
}
