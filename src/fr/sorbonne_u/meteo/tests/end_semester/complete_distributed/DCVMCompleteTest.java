package fr.sorbonne_u.meteo.tests.end_semester.complete_distributed;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.broker.component.Broker;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractDistributedCVM;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.components.utils.tests.TestStep;
import fr.sorbonne_u.components.utils.tests.TestStepI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.property.PropertiesFilter;
import fr.sorbonne_u.filters.property.PropertyFilter;
import fr.sorbonne_u.filters.time.GreaterOrEqualTimeFilter;
import fr.sorbonne_u.filters.time.JokerTimeFilter;
import fr.sorbonne_u.filters.value.meteo.PositionInRegionsValueFilter;
import fr.sorbonne_u.filters.value.meteo.StrongWindMultiValuesFilter;
import fr.sorbonne_u.messages.meteo.AlertMessage;
import fr.sorbonne_u.meteo.components.MeteoBureau;
import fr.sorbonne_u.meteo.components.MeteoStation;
import fr.sorbonne_u.meteo.components.WindTurbine;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.RegionData;
import fr.sorbonne_u.meteo.data.WindData;
import fr.sorbonne_u.meteo.tests.mid_semester.CVMCompleteTest;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * Test distributed version of {@link CVMCompleteTest}.<br>
 * Considering that it was already testing all of our main functionalities, 
 * which are also used by the gossip protocol, we kept most of the original scenario.
 * 
 * <p>
 * In terms of JVM occupations :
 * <ul>
 * 	<li>There are <strong>5 Brokers</strong>, each in its own JVM and connected to others with the gossip protocol.</li>
 * 	<li>In Broker n°1's JVM : WindTurbine n°1 (STANDARD), MeteoStation n°1 (STANDARD) and the clock server.</li>
 * 	<li>In Broker n°2's JVM : MeteoBureau n°1 (PREMIUM).</li>
 * 	<li>In Broker n°3's JVM : nobody...</li>
 * 	<li>In Broker n°4's JVM : MeteoStation n°2 (FREE) and WindTurbine n°2 (STANDARD).</li>
 * 	<li>In Broker n°5's JVM : MeteoBureau n°2 (FREE) and WindTurbine n°3 (FREE).</li>
 * </ul>
 * </p>
 * 
 * <p>
 * 	Wind turbine filters ;
 * </p>
 * <ul>
		<li><strong>WindTurbine n°1</strong> : accepts only wind datas with low force, 
			and alerts targeting its region.</li>
		<li><strong>WindTurbine n°2</strong> : accepts every wind, and only alerts targeting its region.</li>
		<li><strong>WindTurbine n°3</strong> : accepts every wind, alerts targeting its region,
	 		and only messages starting from 80 seconds after the start.</li>
 *     </li>
 * </ul>
 * 
 * The scenario from the start of the DCVM:
 * 
 * <p>
 * 	FIRST PART : focus on FILTER TESTS
 * </p>
 * <ul>
 * 		<li>After 5 seconds, <strong>All 3 WindTurbines</strong> subscribe to default channels "channel0" and "channel1".
 *     	<li>After 20 seconds, <strong>MeteoBureau n°2</strong> sends a wind alert only on <strong>WindTurbine n°1</strong> exact position.</li>
 *     	<li>After 40 seconds, <strong>MeteoStation n°2</strong> sends wind data with high force, 
 *         not being accepted by <strong>WindTurbine n°1</strong>'s filter on wind force
 *         nor <strong>WindTurbine 3</strong>'s filter on time.</li>
 *     	<li>After 60 seconds, <strong>MeteoBureau n°2</strong> sends a dangerous alert exactly to <strong>WindTurbine n°3</strong>'s 
 *         position, not taken into account because its too soon to be accepted by its time filter.</li>
 *     	<li>After 80 seconds, <strong>MeteoBureau n°2</strong> sends again a dangerous alert exactly to 
 *         <strong>WindTurbine n°3's</strong> position, taken into account this time.</li>
 *     	<li>After 100 seconds, <strong>WindTurbine n°3</strong> unregisters itself from the system.</li>
 * </ul>
 *
 * <p>
 * 	SECOND PART : focus on PRIVILEGED SERVICES TESTS
 * </p>
 * <ul>
 *     	<li>After 120 seconds, <strong>MeteoBureau n°1</strong> creates a new channel : "testChannel".</li>
 *     	<li>After 140 seconds, <strong>WindTurbine n°1</strong> unsubscribes from channels "channel0" and "channel1", 
 *         subscribes to channel "testChannel" + <strong>WindTurbine n°2</strong> subscribes to channel "testChannel".<br>
 *         Both with filters accepting everything.</li>
 *     	<li>After 160 seconds, <strong>MeteoStation n°1</strong> sends wind on "channel0".</li>
 *     	<li>After 180 seconds, <strong>MeteoStation n°1</strong> sends wind on "testChannel".</li>
 *     	<li>After 200 seconds, <strong>MeteoBureau n°1</strong> changes 'testChannel' authorized users 
 *         for exactly <strong>WindTurbine n°1</strong> URI.</li>
 *     	<li>After 220 seconds, <strong>MeteoBureau n°1</strong> sends a GREEN alert on the "testChannel", 
 *         received only by <strong>WindTurbine n°1</strong>.</li>
 *     	<li>After 240 seconds, <strong>MeteoBureau n°1</strong> destroys its "testChannel" and verifies that it is has been deleted.</li>
 * </ul>
 * 
 * <p>
 * 	THIRD PART : focus on MULTIPLE CONCURRENT RECEPTIONS
 * </p>
 * <ul>
 *     	<li>After 260 seconds, <strong>MeteoStation n°1</strong> and <strong>MeteoStation n°2</strong> 
 *     		each send 1000 wind datas on "channel0" (100 by 100, then they are grouped by the gossip protocol).</li>
 *     	<li>After 280 seconds, <strong>WindTurbine n°2</strong> verifies that it has received a total of 
 *     		2003 wind datas since the start.</li>
 * </ul>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class DCVMCompleteTest extends AbstractDistributedCVM {
    
	// -------------------------------------------------------------------------
	// Test constants and variables
	// -------------------------------------------------------------------------

	public static String CLOCK_URI = "test-clock";
	public static String START_INSTANT = "2026-01-30T09:00:00.00Z";
	public static String END_INSTANT = "2026-01-30T09:05:10.00Z";

	public static long DELAY_TO_START = 1000L;
	public static long END_SLEEP_DURATION = 10000L;
	public static double ACCELERATION_FACTOR = 2.0;
	public static long EXECUTION_DURATION =
		(long) (DELAY_TO_START + 1000L + ((Instant.parse(END_INSTANT).toEpochMilli() - Instant.parse(START_INSTANT).toEpochMilli())
														/ACCELERATION_FACTOR));
	
	// Delay after the start when steps will be executed, in seconds
	public static long[] STEPS = new long[] {5, 20, 40, 60, 80, 100, 120, 140, 160, 180, 200,
			220, 240, 260, 280};
	public static final int NBSTEPS = STEPS.length;
	
	public static final int NB_END_WINDS_TOTAL = 1000;
	public static final int NB_END_WINDS_PER_SENDING = 100;
	
	// -------------------------------------------------------------------------
	// Component datas
	// -------------------------------------------------------------------------
	
	// Number of threads
	protected final int brokerNbThreads = 4;
	protected final int brokerNbSchedulableThreads = 0;
	protected final int brokerNbFreeLoadersThreads = 2; // Broker's constructor automatically scales at 4 and 8 threads for Standard and Premium senders.
	protected final int brokerNbUnloadersThreads = 4;
	protected final int brokerNbSendersThreads = 4;
	protected final int brokerNbGossipThreads = 4;
	
	protected final int clientNbThreads = 4;
	protected final int clientNbSchedulableThreads = 1;
	
	// Broker 1
	protected final String Broker1GossipInboundPortURI = "gossip-inbound-port-Bro1";
	protected final String Broker1GossipOutboundPortURI = "gossip-outbound-port-Bro1-"; // + "1", +"2", etc..
	protected final PositionData broker1Position = new PositionData(0, 4);
	
	// Broker 2
	protected final String Broker2GossipInboundPortURI = "gossip-inbound-port-Bro2";
	protected final String Broker2GossipOutboundPortURI = "gossip-outbound-port-Bro2-"; // + "1", +"2", etc..
	protected final PositionData broker2Position = new PositionData(1, 4);
	
	// Broker 3
	protected final String Broker3GossipInboundPortURI = "gossip-inbound-port-Bro3";
	protected final String Broker3GossipOutboundPortURI = "gossip-outbound-port-Bro3-"; // + "1", +"2", etc..
	protected final PositionData broker3Position = new PositionData(2, 4);
	
	// Broker 4
	protected final String Broker4GossipInboundPortURI = "gossip-inbound-port-Bro4";
	protected final String Broker4GossipOutboundPortURI = "gossip-outbound-port-Bro4-"; // + "1", +"2", etc..
	protected final PositionData broker4Position = new PositionData(3, 4);
	
	// Broker 5
	protected final String Broker5GossipInboundPortURI = "gossip-inbound-port-Bro5";
	protected final String Broker5GossipOutboundPortURI = "gossip-outbound-port-Bro5-"; // + "1", +"2", etc..
	protected final PositionData broker5Position = new PositionData(4, 4);
	
	// MeteoBureau 1 (with Broker 2)
	protected final String MB1ReceivingInboundPortURI = "inbound-port-client-MB1";
	protected final PositionData bureau1Position = new PositionData(2, 1);
	protected final RegistrationClass MB1rc = RegistrationClass.PREMIUM;
	
	// MeteoBureau 2 (with Broker 5)
	protected final String MB2ReceivingInboundPortURI = "inbound-port-client-MB2";
	protected final PositionData bureau2Position = new PositionData(3, 1);
	protected final RegistrationClass MB2rc = RegistrationClass.FREE;
	
	// MeteoStation 1 (with Broker 1)
	protected final String MS1ReceivingInboundPortURI = "inbound-port-client-MS1";
	protected final static PositionData station1Position = new PositionData(2, 2);
	protected final RegistrationClass MS1rc = RegistrationClass.STANDARD;
	
	// MeteoStation 2 (with Broker 4)
	protected final String MS2ReceivingInboundPortURI = "inbound-port-client-MS2";
	protected final static PositionData station2Position = new PositionData(3, 2);
	protected final RegistrationClass MS2rc = RegistrationClass.FREE;
	
	// Except for Wind turbine 3, this time filter is used : no constraint on time
	protected final static TimeFilterI timeFilterJoker = new JokerTimeFilter();
	
	// WindTurbine 1 (with Broker 5)
	protected final static String WT1ReceivingInboundPortURI = "inbound-port-client-WT1";
	protected final static PositionData windTurbine1Position = new PositionData(1, 0);
	protected final RegistrationClass WT1rc = RegistrationClass.STANDARD;
	protected final MeteoAlertI.Level maxWorkingLevelWT1 = MeteoAlertI.Level.ORANGE; // inclusive
	
	// On winds received, filter keeps only close winds (distance <= 10) && with a force not exceeding 10.
	protected final static PropertiesFilter[] WT1_props =  {new PropertiesFilter(new StrongWindMultiValuesFilter(windTurbine1Position, 10, 10))};
	protected final static MessageFilterI WT1Windfilters =  new MessageFilter(new PropertyFilter[0], WT1_props, timeFilterJoker);
	// On alerts received, filter keeps only alerts with a region containing wind turbine's position
	protected final static PropertyFilterI[] WT1_prop = {new PropertyFilter(AlertMessage.ALERT_REGIONS_PROPERTY, new PositionInRegionsValueFilter(windTurbine1Position))};
    protected final static MessageFilterI WT1Alertfilters =  new MessageFilter(WT1_prop, new PropertiesFilter[0], timeFilterJoker);
	
	// WindTurbine 2 (with Broker 1)
	protected final String WT2ReceivingInboundPortURI = "inbound-port-client-WT2";
	protected final static PositionData windTurbine2Position = new PositionData(2, 0);
	protected final RegistrationClass WT2rc = RegistrationClass.STANDARD;
	protected final MeteoAlertI.Level maxWorkingLevelWT2 = MeteoAlertI.Level.ORANGE; // inclusive
	
	// Accepts every wind data
	protected final static MessageFilterI WT2Windfilters =  new MessageFilter(new PropertyFilter[0], new PropertiesFilter[0], timeFilterJoker);
	// On alerts received, filter keeps only alerts with a region containing wind turbine's position
	protected final static PropertyFilterI[] WT2_prop = {new PropertyFilter(AlertMessage.ALERT_REGIONS_PROPERTY, new PositionInRegionsValueFilter(windTurbine2Position))};
	protected final static MessageFilterI WT2Alertfilters =  new MessageFilter(WT2_prop, new PropertiesFilter[0], timeFilterJoker);
	
	// WindTurbine 3 (with Broker 4)
	protected final String WT3ReceivingInboundPortURI = "inbound-port-client-WT3";
	protected final static PositionData windTurbine3Position = new PositionData(3, 0);
	protected final RegistrationClass WT3rc = RegistrationClass.FREE;
	protected final MeteoAlertI.Level maxWorkingLevelWT3 = MeteoAlertI.Level.ORANGE; // inclusive
	
	// Time constraint, filters accepts messages 80 (=STEP3) seconds after the start of the scenario
	protected final static TimeFilterI WT3timeFilterGE = new GreaterOrEqualTimeFilter(Instant.now().plusSeconds((long)(STEPS[4]/ACCELERATION_FACTOR)));
	// Accepts every wind data
	protected final static MessageFilterI WT3Windfilters =  new MessageFilter(new PropertyFilter[0], new PropertiesFilter[0], WT3timeFilterGE);
	// On alerts received, filter keeps only alerts with a region containing wind turbine's position
	protected final static PropertyFilterI[] WT3_prop = {new PropertyFilter(AlertMessage.ALERT_REGIONS_PROPERTY, new PositionInRegionsValueFilter(windTurbine3Position))};
	protected final static MessageFilterI WT3Alertfilters =  new MessageFilter(WT3_prop, new PropertiesFilter[0], WT3timeFilterGE);
	
	// Broker common parameters
	protected final int nbDefaultChannels = 2;
	protected final int nbMaxChannelsStd = 2;
	protected final int nbMaxChannelsPrem = 4;
	
	// JVM URIs
	protected String JVMbroker1 = "broker1";
	protected String JVMbroker2 = "broker2";
	protected String JVMbroker3 = "broker3";
	protected String JVMbroker4 = "broker4";
	protected String JVMbroker5 = "broker5";
	
	// Component URIs
	protected static String BROKER1_URI = "Broker1-URI";
	protected static String BROKER2_URI = "Broker2-URI";
	protected static String BROKER3_URI = "Broker3-URI";
	protected static String BROKER4_URI = "Broker4-URI";
	protected static String BROKER5_URI = "Broker5-URI";
	protected static String MB1_URI = "MeteoBureau1-URI";
	protected static String MB2_URI = "MeteoBureau2-URI";
	protected static String MS1_URI = "MeteoStation1-URI";
	protected static String MS2_URI = "MeteoStation2-URI";
	protected static String WT1_URI = "WindTurbine1-URI";
	protected static String WT2_URI = "WindTurbine2-URI";
	protected static String WT3_URI = "WindTurbine3-URI";
	
	// -------------------------------------------------------------------------
	// Test scenario steps
	// -------------------------------------------------------------------------
	
	/**
	 * Returns the test scenario described in the class documentation.
	 * 
	 * <p><strong>Contract</strong></p>
	 * <pre>
	 * pre	{@code true}	// no precondition.
	 * post	{@code return != null}
	 * </pre>
	 * 
	 * @return					The test scenario described in the class documentation.
	 */
	public static TestScenario	testScenario() {
		Instant startInstant = Instant.parse(START_INSTANT);
		Instant endInstant = Instant.parse(END_INSTANT);

		// Scenario instants
		Instant[] step_Instants = new Instant[NBSTEPS];
		for (int i=0; i<NBSTEPS; i++) step_Instants[i] = startInstant.plusSeconds(STEPS[i]);

		// MeteoBureau alert regions
		
		ArrayList<RegionI> all_regions = new ArrayList<>();
		all_regions.add(new RegionData(0, 9, 0, 9));
		
		ArrayList<RegionI> WT1_region = new ArrayList<>(); // for targeting only the exact position of Wind Turbine1 
		WT1_region.add(new RegionData(windTurbine1Position.getPosX(), windTurbine1Position.getPosX(), 
				windTurbine1Position.getPosY(), windTurbine1Position.getPosY()));
		
		ArrayList<RegionI> WT3_region = new ArrayList<>(); // for targeting only the exact position of Wind Turbine13
		WT3_region.add(new RegionData(windTurbine3Position.getPosX(), windTurbine3Position.getPosX(), 
				windTurbine3Position.getPosY(), windTurbine3Position.getPosY()));
		
		// Subscription filter on Step 6 subscriptions, accepts everything
        MessageFilterI accept_all_filter = new MessageFilter(new PropertyFilterI[0], new PropertiesFilter[0], timeFilterJoker);
		
		// End wind datas
		List<WindData> ms1_end_winds = new ArrayList<>();
		List<WindData> ms2_end_winds = new ArrayList<>();
		WindData wd1 = new WindData(station1Position, 2, 2);
		WindData wd2 = new WindData(station2Position, 2, 2);
		for (int k=0; k<NB_END_WINDS_PER_SENDING; k++) {
			ms1_end_winds.add(wd1);
			ms2_end_winds.add(wd2);
		}
        
        // The MeteoStation1 component is used for printing logs on the console
		return new TestScenario(
			CLOCK_URI,
			startInstant,
			endInstant,
			new TestStepI[] {
					
				new TestStep(CLOCK_URI, MS1_URI, step_Instants[0],
						owner -> {
							System.out.println("STEP 0/"+(NBSTEPS-1)+" ("+STEPS[0]+"s) : WindTurbines subscribe to default channels 'channel0' and 'channel1'");
						}),
				// WindTurbines subscribe to default channels "channel0" and "channel1"
				new TestStep(CLOCK_URI, WT1_URI, step_Instants[0],
						owner -> {
							try {
								((WindTurbine)owner).subscribeToChannel("channel0", WT1Windfilters);
								((WindTurbine)owner).subscribeToChannel("channel1", WT1Alertfilters);
							} catch (Exception e) {
								System.err.println("Step 0 WT1 subscriptions failed with exception " + e);
							}
						}),
				new TestStep(CLOCK_URI, WT2_URI, step_Instants[0],
						owner -> {
							try {
								((WindTurbine)owner).subscribeToChannel("channel0", WT2Windfilters);
								((WindTurbine)owner).subscribeToChannel("channel1", WT2Alertfilters);
							} catch (Exception e) {
								System.err.println("Step 0 WT2 subscriptions failed with exception " + e);
							}
						}),
				new TestStep(CLOCK_URI, WT3_URI, step_Instants[0],
						owner -> {
							try {
								((WindTurbine)owner).subscribeToChannel("channel0", WT3Windfilters);
								((WindTurbine)owner).subscribeToChannel("channel1", WT3Alertfilters);
							} catch (Exception e) {
								System.err.println("Step 0 WT3 subscriptions failed with exception " + e);
							}
						}),
				
					
				new TestStep(CLOCK_URI, MS1_URI, step_Instants[1],
						owner -> {
							System.out.println("STEP 1/"+(NBSTEPS-1)+" ("+STEPS[1]+"s) : MeteoBureau2 sends a wind alert on WindTubine1 exact position");
						}),
				// MeteoBureau2 sends wind data only on WindTubine1 exact position.
				new TestStep(CLOCK_URI, MB2_URI, step_Instants[1],
						owner -> {
							try {
								((MeteoBureau)owner).publishAlert(MeteoAlertI.AlertType.FLOODING, MeteoAlertI.Level.ORANGE, 
										WT1_region, step_Instants[1], Duration.ofSeconds(5), "channel1");
							} catch (Exception e) {
								System.err.println("Step 1 alert publication failed with exception " + e);
							}
						}),
				
				new TestStep(CLOCK_URI, MS1_URI, step_Instants[2],
						owner -> {
							System.out.println("STEP 2/"+(NBSTEPS-1)+" ("+STEPS[2]+"s) : MeteoStation2 sends wind data with light force, only taken into account by WindTubine2");
						}),
				// MeteoStation2 sends wind data with light force, not being accepted by WindTubine1's filter on wind force.
				new TestStep(CLOCK_URI, MS2_URI, step_Instants[2],
					owner -> {
						try {
							((MeteoStation)owner).publishWindData(10.0, 10.0, "channel0", false);
						} catch (Exception e) {
							System.err.println("Step 2 wind data publication failed with exception " + e);
						}
					}),
				
				new TestStep(CLOCK_URI, MS1_URI, step_Instants[3],
						owner -> {
							System.out.println("STEP 3/"+(NBSTEPS-1)+" ("+STEPS[3]+"s) : MeteoBureau2 sends a dangerous alert exactly to WindTurbine3's position (not taken into account, too early for time filter)");
						}),
				// MeteoBureau2 sends a dangerous alert exactly to WindTurbine3's position, 
				// not taken into account because its too soon to be accepted by its time filter
				new TestStep(CLOCK_URI, MB2_URI, step_Instants[3],
						owner -> {
							try {
								((MeteoBureau)owner).publishAlert(MeteoAlertI.AlertType.EARTHQUAKE, MeteoAlertI.Level.RED, 
										WT3_region, step_Instants[3], Duration.ofSeconds(5), "channel1");
							} catch (Exception e) {
								System.err.println("Step 3 alert publication failed with exception " + e);
							}
						}),
				
				new TestStep(CLOCK_URI, MS1_URI, step_Instants[4],
						owner -> {
							System.out.println("STEP 4/"+(NBSTEPS-1)+" ("+STEPS[4]+"s) : MeteoBureau2 sends again a dangerous alert exactly to WindTurbine3's position (taken into account)");
						}),
				// MeteoBureau2 sends again a dangerous alert exactly to WindTurbine3's position, 
				// taken into account this time
				new TestStep(CLOCK_URI, MB2_URI, step_Instants[4],
						owner -> {
							try {
								((MeteoBureau)owner).publishAlert(MeteoAlertI.AlertType.ICY_STORM, MeteoAlertI.Level.SCARLET, 
										WT3_region, step_Instants[4], Duration.ofSeconds(5), "channel1");
							} catch (Exception e) {
								System.err.println("Step 4 alert publication failed with exception " + e);
							}
						}),
				
				new TestStep(CLOCK_URI, MS1_URI, step_Instants[5],
						owner -> {
							System.out.println("STEP 5/"+(NBSTEPS-1)+" ("+STEPS[5]+"s) : WindTurbine3 unregisters itself from the system");
						}),
				// WindTurbine3 unregisters itself from the system
				new TestStep(CLOCK_URI, WT3_URI, step_Instants[5],
					owner -> {
						try {
							((WindTurbine)owner).unregisterFromSystem();
						} catch (Exception e) {
							System.err.println("Step 5 unregistration failed with exception " + e);
						}
					}),
				
				new TestStep(CLOCK_URI, MS1_URI, step_Instants[6],
						owner -> {
							System.out.println("STEP 6/"+(NBSTEPS-1)+" ("+STEPS[6]+"s) : MeteoBureau1 creates a new channel : 'testChannel'");
						}),
				// MeteoBureau1 creates a new channel : "testChannel"
				new TestStep(CLOCK_URI, MB1_URI, step_Instants[6],
						owner -> {
							try {
								((MeteoBureau)owner).createNewChannel("testChannel", ".*");
							} catch (Exception e) {
								System.err.println("Step 6 new channel creation failed with exception " + e);
							}
						}),
				
				new TestStep(CLOCK_URI, MS1_URI, step_Instants[7],
						owner -> {
							System.out.println("STEP 7/"+(NBSTEPS-1)+" ("+STEPS[7]+"s) : WindTurbine1 unsubscribes from channels 'channel0' and 'channel1',\nWindTurbine1 + WindTurbine2 subscribe to channel 'testChannel'");
						}),
				// WindTurbine1 unsubscribe from channels "channel0" and "channel1", subscribes to channel "testChannel"
				new TestStep(CLOCK_URI, WT1_URI, step_Instants[7],
						owner -> {
							try {
								((WindTurbine)owner).unsubscribeFromChannel("channel0");
								((WindTurbine)owner).unsubscribeFromChannel("channel1");
								((WindTurbine)owner).subscribeToChannel("testChannel", accept_all_filter);
							} catch (Exception e) {
								System.err.println("Step 7 WT1 unsubscriptions + subscription failed with exception " + e);
							}
						}),
				// WindTurbine2 subscribes to channel "testChannel"
				new TestStep(CLOCK_URI, WT2_URI, step_Instants[7],
						owner -> {
							try {
								((WindTurbine)owner).subscribeToChannel("testChannel", accept_all_filter);
							} catch (Exception e) {
								System.err.println("Step 7 WT2 subscription failed with exception " + e);
							}
						}),
				
				// MeteoStation1 sends wind on "channel0"
				new TestStep(CLOCK_URI, MS1_URI, step_Instants[8],
						owner -> {
							try {
								System.out.println("STEP 8/"+(NBSTEPS-1)+" ("+STEPS[8]+"s) : MeteoStation1 sends wind on 'channel0', \nwith only WindTurbine2 still subscribed to it");
								((MeteoStation)owner).publishWindData(1.0, 1.0, "channel0", false);
							} catch (Exception e) {
								System.err.println("Step 8 wind data publication failed with exception " + e);
							}
						}),
				
				// MeteoStation1 sends wind on "testChannel"
				new TestStep(CLOCK_URI, MS1_URI, step_Instants[9],
						owner -> {
							try {
								System.out.println("STEP 9/"+(NBSTEPS-1)+" ("+STEPS[9]+"s) : MeteoStation1 sends wind on 'testChannel', \nwith both WindTurbine 1 and 2 subscribed to it");
								((MeteoStation)owner).publishWindData(2.0, 2.0, "testChannel", false);
							} catch (Exception e) {
								System.err.println("Step 9 wind data publication failed with exception " + e);
							}
						}),
				
				new TestStep(CLOCK_URI, MS1_URI, step_Instants[10],
						owner -> {
							System.out.println("STEP 10/"+(NBSTEPS-1)+" ("+STEPS[10]+"s) : MeteoBureau1 changes 'testChannel' authorized users for exactly WindTurbine1 URI,\nimplicitly unsubscribing WindTurbine2");
						}),
				// MeteoBureau1 changes authorized users for exactly WindTurbine1 URI
				new TestStep(CLOCK_URI, MB1_URI, step_Instants[10],
						owner -> {
							try {
								((MeteoBureau)owner).modifyAuthorizedUsers("testChannel", "^"+WT1ReceivingInboundPortURI+"$"); // inbound receiving port used on the broker
							} catch (Exception e) {
								System.err.println("Step 10 authorized users modification failed with exception " + e);
							}
						}),
				
				new TestStep(CLOCK_URI, MS1_URI, step_Instants[11],
						owner -> {
							System.out.println("STEP 11/"+(NBSTEPS-1)+" ("+STEPS[11]+"s) : MeteoBureau1 sends a GREEN alert on the 'testChannel', received only by WindTurbine1");
						}),
				// MeteoBureau1 sends a GREEN alert on the "testChannel", received only by WindTurbine1
				new TestStep(CLOCK_URI, MB1_URI, step_Instants[11],
						owner -> {
							try {
								((MeteoBureau)owner).publishAlert(MeteoAlertI.AlertType.EARTHQUAKE, MeteoAlertI.Level.GREEN, 
										all_regions, step_Instants[11], Duration.ofSeconds(5), "testChannel");
							} catch (Exception e) {
								System.err.println("Step 11 alert publication failed with exception " + e);
							}
						}),
				
				new TestStep(CLOCK_URI, MS1_URI, step_Instants[12],
						owner -> {
							System.out.println("STEP 12/"+(NBSTEPS-1)+" ("+STEPS[12]+"s) : MeteoBureau1 destroys its 'testChannel' and verifies that it is has been deleted");
						}),
				// MeteoBureau1 destroys its "testChannel" and verifies that it is has been deleted
				new TestStep(CLOCK_URI, MB1_URI, step_Instants[12],
						owner -> {
							try {
								((MeteoBureau)owner).destroyChannelAndVerifies("testChannel");
							} catch (Exception e) {
								System.err.println("Step 12 'testChannel' destruction + verification failed with exception " + e);
							}
						}),
		
				// MeteoStation1 and MeteoStation2 each send 1000 winds on "channel0", 100 by 100
				new TestStep(CLOCK_URI, MS1_URI, step_Instants[13],
						owner -> {
							try {
								System.out.println("STEP 13/"+(NBSTEPS-1)+" ("+STEPS[13]+"s) : MeteoStation1 and MeteoStation2 each "
										+ "send "+NB_END_WINDS_TOTAL+" winds on 'channel0' ("+NB_END_WINDS_PER_SENDING+" by "+NB_END_WINDS_PER_SENDING+")"
												+ ", \nwith only WindTurbine2 still subscribed to it");
								for (int i=0; i<NB_END_WINDS_TOTAL; i+=NB_END_WINDS_PER_SENDING) {
									((MeteoStation)owner).publishWindDatas(ms1_end_winds, "channel0", false);
								}
							} catch (Exception e) {
								System.err.println("Step 13 MS1 multiple wind data publication failed with exception " + e);
							}
						}),
				new TestStep(CLOCK_URI, MS2_URI, step_Instants[13],
						owner -> {
							try {
								for (int i=0; i<NB_END_WINDS_TOTAL; i+=NB_END_WINDS_PER_SENDING) {
									((MeteoStation)owner).publishWindDatas(ms2_end_winds, "channel0", false);
								}
							} catch (Exception e) {
								System.err.println("Step 13 MS2 multiple wind data publication failed with exception " + e);
							}
						}),
				
				new TestStep(CLOCK_URI, MS1_URI, step_Instants[14],
						owner -> {
							System.out.println("STEP 14/"+(NBSTEPS-1)+" ("+STEPS[14]+"s) : WindTurbine2 checks that it has received 2003 wind datas since start");
						}),
				// WindTurbine2 checks that it has received 2003 wind datas since start.
				new TestStep(
						CLOCK_URI, WT2_URI, step_Instants[14],
						owner -> {
							try {
								((WindTurbine)owner).verifyNbWindDatasReceived(2003);
							} catch (Exception e) {
								System.err.println("Step 14 wind datas account check failed with exception " + e);
							}
						}),
			});
	}
	
	public DCVMCompleteTest(String[] args) throws Exception{
		super(args);
	}
	
	@Override
	public void	instantiateAndPublish() throws Exception {
		
		long current = System.currentTimeMillis();
		long unixEpochStartTimeInNanos = TimeUnit.MILLISECONDS.toNanos(current + DELAY_TO_START);
		
		Instant	startInstant = Instant.parse(START_INSTANT);
		TestScenario testScenario = testScenario();
		
		if (thisJVMURI.equals(JVMbroker1)) {
			
			ArrayList<String> broker1OutboundPorts = new ArrayList<>();
			broker1OutboundPorts.add(Broker1GossipOutboundPortURI+"1");
			broker1OutboundPorts.add(Broker1GossipOutboundPortURI+"2");
			
			ArrayList<String> broker1ConnectedTo = new ArrayList<>(); 
			broker1ConnectedTo.add(Broker2GossipInboundPortURI);
			broker1ConnectedTo.add(Broker3GossipInboundPortURI);
			
			// Broker 1 creation
			BROKER1_URI = AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {1, broker1Position, nbDefaultChannels, nbMaxChannelsStd, nbMaxChannelsPrem, brokerNbThreads, 
							brokerNbSchedulableThreads, brokerNbFreeLoadersThreads, brokerNbUnloadersThreads, brokerNbSendersThreads,
							brokerNbGossipThreads, Broker1GossipInboundPortURI, broker1OutboundPorts, broker1ConnectedTo});
			assert this.isDeployedComponent(BROKER1_URI);
			this.toggleTracing(BROKER1_URI);
			
			// MeteoStation 1 creation
			MS1_URI = AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] { 1, MS1_URI, MS1ReceivingInboundPortURI, MS1rc, clientNbThreads, clientNbSchedulableThreads, 
							station1Position, testScenario});
			assert this.isDeployedComponent(MS1_URI);
			this.toggleTracing(MS1_URI);
			
			// WindTurbine 2 creation
			WT2_URI = AbstractComponent.createComponent(WindTurbine.class.getCanonicalName(),
					new Object[] { 2, WT2_URI, WT2ReceivingInboundPortURI, WT2rc, clientNbThreads, clientNbSchedulableThreads, 
							windTurbine2Position, maxWorkingLevelWT2, false, WT2Windfilters, WT2Alertfilters, testScenario});
			assert this.isDeployedComponent(WT2_URI);
			this.toggleTracing(WT2_URI);
			
			// Clock server creation
			String serv = AbstractComponent.createComponent(
					ClocksServer.class.getCanonicalName(),
					new Object[] {
						CLOCK_URI,
						unixEpochStartTimeInNanos,
						startInstant,
						ACCELERATION_FACTOR
					});
			assert this.isDeployedComponent(serv);
			
		} else if (thisJVMURI.equals(JVMbroker2)) {
			
			ArrayList<String> broker2OutboundPorts = new ArrayList<>();
			broker2OutboundPorts.add(Broker2GossipOutboundPortURI+"1");
			broker2OutboundPorts.add(Broker2GossipOutboundPortURI+"2");
			
			ArrayList<String> broker2ConnectedTo = new ArrayList<>(); 
			broker2ConnectedTo.add(Broker1GossipInboundPortURI);
			broker2ConnectedTo.add(Broker4GossipInboundPortURI);
			
			// Broker 2 creation
			BROKER2_URI = AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {2, broker2Position, nbDefaultChannels, nbMaxChannelsStd, nbMaxChannelsPrem, brokerNbThreads, 
							brokerNbSchedulableThreads, brokerNbFreeLoadersThreads, brokerNbUnloadersThreads, brokerNbSendersThreads,
							brokerNbGossipThreads, Broker2GossipInboundPortURI, broker2OutboundPorts, broker2ConnectedTo});
			assert this.isDeployedComponent(BROKER2_URI);
			this.toggleTracing(BROKER2_URI);
			
			// MeteoBureau 1 creation
			MB1_URI = AbstractComponent.createComponent(
					MeteoBureau.class.getCanonicalName(),
					new Object[] {1, MB1_URI, MB1ReceivingInboundPortURI, MB1rc, clientNbThreads, clientNbSchedulableThreads, 
							bureau1Position, testScenario});
			assert this.isDeployedComponent(MB1_URI);
			this.toggleTracing(MB1_URI);
			
		} else if (thisJVMURI.equals(JVMbroker3)) {
			
			ArrayList<String> broker3OutboundPorts = new ArrayList<>();
			broker3OutboundPorts.add(Broker3GossipOutboundPortURI+"1");
			broker3OutboundPorts.add(Broker3GossipOutboundPortURI+"2");
			
			ArrayList<String> broker3ConnectedTo = new ArrayList<>(); 
			broker3ConnectedTo.add(Broker1GossipInboundPortURI);
			broker3ConnectedTo.add(Broker4GossipInboundPortURI);
			
			// Broker 3 creation
			BROKER3_URI = AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {3, broker3Position, nbDefaultChannels, nbMaxChannelsStd, nbMaxChannelsPrem, brokerNbThreads, 
							brokerNbSchedulableThreads, brokerNbFreeLoadersThreads, brokerNbUnloadersThreads, brokerNbSendersThreads,
							brokerNbGossipThreads, Broker3GossipInboundPortURI, broker3OutboundPorts, broker3ConnectedTo});
			assert this.isDeployedComponent(BROKER3_URI);
			this.toggleTracing(BROKER3_URI);
			
			
		} else if (thisJVMURI.equals(JVMbroker4)) {
			
			ArrayList<String> broker4OutboundPorts = new ArrayList<>();
			broker4OutboundPorts.add(Broker4GossipOutboundPortURI+"1");
			broker4OutboundPorts.add(Broker4GossipOutboundPortURI+"2");
			broker4OutboundPorts.add(Broker4GossipOutboundPortURI+"3");
			
			ArrayList<String> broker4ConnectedTo = new ArrayList<>(); 
			broker4ConnectedTo.add(Broker2GossipInboundPortURI);
			broker4ConnectedTo.add(Broker3GossipInboundPortURI);
			broker4ConnectedTo.add(Broker5GossipInboundPortURI);
			
			// Broker 4 creation
			BROKER4_URI = AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {4, broker4Position, nbDefaultChannels, nbMaxChannelsStd, nbMaxChannelsPrem, brokerNbThreads, 
							brokerNbSchedulableThreads, brokerNbFreeLoadersThreads, brokerNbUnloadersThreads, brokerNbSendersThreads,
							brokerNbGossipThreads, Broker4GossipInboundPortURI, broker4OutboundPorts, broker4ConnectedTo});
			assert this.isDeployedComponent(BROKER4_URI);
			this.toggleTracing(BROKER4_URI);
			
			// MeteoStation 2 creation
			MS2_URI = AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] {2, MS2_URI, MS2ReceivingInboundPortURI, MS2rc, clientNbThreads, clientNbSchedulableThreads, 
							station2Position, testScenario});
			assert this.isDeployedComponent(MS2_URI);
			this.toggleTracing(MS2_URI);
			
			// WindTurbine 3 creation
			WT3_URI = AbstractComponent.createComponent(WindTurbine.class.getCanonicalName(),
					new Object[] {3, WT3_URI, WT3ReceivingInboundPortURI, WT3rc, clientNbThreads, clientNbSchedulableThreads, 
							windTurbine3Position, maxWorkingLevelWT3, false, WT3Windfilters, WT3Alertfilters, testScenario});
			assert this.isDeployedComponent(WT3_URI);
			this.toggleTracing(WT3_URI);
			
		} else if (thisJVMURI.equals(JVMbroker5)) {
			
			ArrayList<String> broker5OutboundPorts = new ArrayList<>();
			broker5OutboundPorts.add(Broker5GossipOutboundPortURI+"1");
			
			ArrayList<String> broker5ConnectedTo = new ArrayList<>(); 
			broker5ConnectedTo.add(Broker4GossipInboundPortURI);
			
			// Broker 5 creation
			BROKER5_URI = AbstractComponent.createComponent(
					Broker.class.getCanonicalName(),
					new Object[] {5, broker5Position, nbDefaultChannels, nbMaxChannelsStd, nbMaxChannelsPrem, brokerNbThreads, 
							brokerNbSchedulableThreads, brokerNbFreeLoadersThreads, brokerNbUnloadersThreads, brokerNbSendersThreads,
							brokerNbGossipThreads, Broker5GossipInboundPortURI, broker5OutboundPorts, broker5ConnectedTo});
			assert this.isDeployedComponent(BROKER5_URI);
			this.toggleTracing(BROKER5_URI);
			
			// MeteoBureau 2 creation
			MB2_URI = AbstractComponent.createComponent(
					MeteoBureau.class.getCanonicalName(),
					new Object[] {2, MB2_URI, MB2ReceivingInboundPortURI, MB2rc, clientNbThreads, clientNbSchedulableThreads, 
							bureau2Position, testScenario});
			assert this.isDeployedComponent(MB2_URI);
			this.toggleTracing(MB2_URI);
			
			// WindTurbine 1 creation
			WT1_URI = AbstractComponent.createComponent(WindTurbine.class.getCanonicalName(),
					new Object[] {1, WT1_URI, WT1ReceivingInboundPortURI, WT1rc, clientNbThreads, clientNbSchedulableThreads, 
							windTurbine1Position, maxWorkingLevelWT1, false, WT1Windfilters, WT1Alertfilters, testScenario});
			assert this.isDeployedComponent(WT1_URI);
			this.toggleTracing(WT1_URI);
			
		} else {
			System.err.println("Unknown JVM URI... " + thisJVMURI);
		}

		super.instantiateAndPublish();
	}
	
	public static void main(String[] args) {
		try {
			DCVMCompleteTest dcvm = new DCVMCompleteTest(args) ;
			dcvm.startStandardLifeCycle(EXECUTION_DURATION);
			Thread.sleep(END_SLEEP_DURATION);
			System.exit(0) ;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
