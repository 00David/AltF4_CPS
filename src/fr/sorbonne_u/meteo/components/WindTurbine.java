package fr.sorbonne_u.meteo.components;

import java.time.Instant;
import java.util.ArrayList;

import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.LevelI;
import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.property.PropertiesFilter;
import fr.sorbonne_u.filters.property.PropertyFilter;
import fr.sorbonne_u.filters.time.GreaterOrEqualTimeFilter;
import fr.sorbonne_u.filters.value.PositionInRegionsValueFilter;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;
import fr.sorbonne_u.meteo.plugins.WindTurbineReceptionPlugin;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;

/**
 * The WindTurbine component is a {@link Client} of the system.
 * 
 * <p>
 * It receives messages sent by meteorological stations/offices and takes
 * appropriate decisions for its orientation or safety mode.
 * </p>
 * 
 * <p><strong>Invariants</strong></p>
 * <pre>
 * invariant	{@code datas != null}
 * invariant	{@code pos != null}
 * invariant	{@code orientation != null}
 * invariant	{@code maxWorkingLevel != null}
 * invariant	{@code subscriptionPlugin != null}
 * </pre>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class WindTurbine extends Client {

	// Stores wind data received from messages
	private ArrayList<WindData> datas;

	// The position of the wind turbine
	private PositionData pos;

	// The orientation of the wind turbine
	private PositionData orientation; // PositionData is used as a vector here, for the WindTurbine orientation

	// Maximum alert level (inclusive) at which the turbine operates.
    // If a higher level is received, the turbine enters safety mode.
	private LevelI maxWorkingLevel;

	// Indicates whether the turbine is in safety mode or not (false by default)
	private boolean safeMode;
	
	// Subscription + reception
	protected final WindTurbineReceptionPlugin subscriptionPlugin;

	//--------------------------------------------------------------------------
	// Constructor
	//--------------------------------------------------------------------------
	
	/**
     * Creates a wind turbine component for the publish/subscribe system, extending a {@link Client}.
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
     * pre  {@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
     * pre  {@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
     * pre  {@code pos != null}
     * pre  {@code maxWorkingLevel != null}
     * post {@code true} // no postcondition.
     * </pre>
     * 
     * @param registrationOutboundPortURI URI of the outbound port requesting {@link RegistrationCI}.
     * @param publishingOutboundPortURI   URI of the outbound port requesting {@link PublishingCI}.
     * @param receivingInboundPortURI     URI of the inbound port providing {@link ReceivingCI}.
     * @param pos                         The position of the wind turbine.
     * @param maxWorkingLevel             Maximum alert level (inclusive) at which the turbine operates.
     * @throws Exception                  If a problem occurs during initialization or port publication.
     */
	protected WindTurbine(String registrationOutboundPortURI, String publishingOutboundPortURI,
			String receivingInboundPortURI, PositionI pos,LevelI maxWorkingLevel) throws Exception {
		super(receivingInboundPortURI, registrationOutboundPortURI, RegistrationClass.FREE);
		assert (pos != null);
		assert (maxWorkingLevel != null);

		this.pos = (PositionData) pos;
		this.datas = new ArrayList<WindData>();
		this.orientation = new PositionData(0, 0);
		this.safeMode = false;
		this.maxWorkingLevel = maxWorkingLevel;
		
		// Install the subscription + reception plugin (Wind turbine version)
		this.subscriptionPlugin = new WindTurbineReceptionPlugin(receivingInboundPortURI, registrationPlugin.getRegistrationOutboundPort());
		this.subscriptionPlugin.setPluginURI("subscription");
		this.installPlugin(this.subscriptionPlugin);
		
		this.getTracer().setTitle("WindTurbine");
		this.getTracer().setRelativePosition(0, 0);

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

		startWindTurbine();
	}

    /**
     * Registers the wind turbine to channels for receiving wind data and alerts.
     * 
     * <p>
     * <li> Only considers messages created after the turbine registration.
     * <li> For alerts, only considers alerts where one of the affected regions contains the turbine's position.
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
	public void startWindTurbine() throws Exception {

        // Create a filter only accepting messages after the turbine subscription
        TimeFilterI timeFilter = new GreaterOrEqualTimeFilter(Instant.now());

        // Wind data subscription
        subscriptionPlugin.subscribe("channel0", new MessageFilter(new PropertyFilter[0], new PropertiesFilter[0], timeFilter));
        this.logMessage("WindTurbine subscribed to channel0");

        // Alert data subscription
        PropertyFilterI[] propFiltersAlert = new PropertyFilterI[1];
        // Create a filter accepting regions containing the wind turbine position
        propFiltersAlert[0] = new PropertyFilter("regions", new PositionInRegionsValueFilter(this.pos));

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
	 * @return true if wind turbine is in safe mode, otherwise it operates normally.
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
	public void calculateNeededDirection(WindData newWindData) {
		if (newWindData != null) datas.add(newWindData);
		
		double x = 0;
		double y = 0;

		for (WindData d : datas) {
			double distance = distance(d.getPosition());
			double weight = 0;
			
			if(distance == 0) {
				weight = 1.0;
			}else {
				weight = 1.0 / distance;
			}
			
			x += d.xComponent() * weight;
			y += d.yComponent() * weight;
		}

		// Reverse vector direction
		x = x * -1;
		y = y * -1;

		this.orientation = new PositionData(x, y);
		this.logMessage("WindTurbine has changed orientation : now = " + this.orientation);
		this.logMessage("------------------------------");
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
     * @param level Received alert level.
     */
	public void react(LevelI level) {
		if (level == null) {
			this.logMessage("------------------------------");
			return;
		}

		MeteoAlertI.Level maxLevel = (MeteoAlertI.Level) this.maxWorkingLevel;
		MeteoAlertI.Level alertLevel = (MeteoAlertI.Level) level;

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
     * pre  {@code stationPos != null && stationPos instanceof PositionData}
     * post {@code true} // no postcondition.
     * </pre>
     * 
     * @param stationPos Position of the meteorological station.
     * @return the distance between the turbine and a {@code stationPos}.
     */
	private double distance(PositionI stationPos) {
		// Precondition checks
		assert(stationPos != null);
		assert(stationPos instanceof PositionData);
		
		PositionData stationP = (PositionData) stationPos;
		double dx = stationP.getPosX() - this.pos.getPosX();
		double dy = stationP.getPosY() - this.pos.getPosY();
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
	private void goInSafeMode() {
		if (!this.safeMode) {
			this.logMessage("WindTurbine going into security mode.");
			this.logMessage("------------------------------");
			this.safeMode = true;
		}
		// Postcondition check
		assert(this.safeMode == true);
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
	private void goBacktoNormalMode() {
		if (this.safeMode) {
			this.logMessage("WindTurbine returning to normal operation.");
			this.logMessage("------------------------------");
			this.safeMode = false;
		}
		// Postcondition check
		assert(this.safeMode == false);
	}
}
