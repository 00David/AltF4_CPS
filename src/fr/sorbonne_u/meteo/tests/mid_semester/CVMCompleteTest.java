package fr.sorbonne_u.meteo.tests.mid_semester;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.components.utils.tests.TestStep;
import fr.sorbonne_u.components.utils.tests.TestStepI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.meteo.interfaces.RegionI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.PropertyFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.VerboseException;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.property.PropertiesFilter;
import fr.sorbonne_u.filters.property.PropertyFilter;
import fr.sorbonne_u.filters.time.GreaterOrEqualTimeFilter;
import fr.sorbonne_u.filters.time.JokerTimeFilter;
import fr.sorbonne_u.filters.value.meteo.PositionInRegionsValueFilter;
import fr.sorbonne_u.filters.value.meteo.StrongWindMultiValuesFilter;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.RegionData;
import fr.sorbonne_u.meteo.tests.MeteoBureauTest;
import fr.sorbonne_u.meteo.tests.MeteoStationTest;
import fr.sorbonne_u.meteo.tests.WindTurbineTest;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * Test class to simulate the following scenario from the start of the CVM.
 * 
 * <p>
 * 	SETUP
 * </p>
 * <ul>
 *     <li>2 {@link MeteoBureauTest} :
 *         <ul>
 *             <li><strong>MeteoBureau n°1</strong> : PREMIUM</li>
 *             <li><strong>MeteoBureau n°2</strong> : FREE</li>
 *         </ul>
 *     </li>
 *     <li>2 {@link MeteoStationTest} :
 *         <ul>
 *             <li><strong>MeteoStation n°1</strong> : STANDARD</li>
 *             <li><strong>MeteoStation n°2</strong> : FREE</li>
 *         </ul>
 *     </li>
 *     <li>3 {@link WindTurbineTest} :
 *         <ul>
 *             <li><strong>WindTurbine n°1</strong> : STANDARD, with filters accepting only wind datas with low force, 
 *                 and alerts targeting its region.</li>
 *             <li><strong>WindTurbine n°2</strong> : STANDARD, with filters accepting every wind, and only alerts targeting its region.</li>
 *             <li><strong>WindTurbine n°3</strong> : FREE, with filters accepting every wind, alerts targeting its region,
 *                 and accepts messages starting from 80 seconds after the start.</li>
 *         </ul>
 *     </li>
 * </ul>
 * 
 * <p>
 * 	FIRST HALF : focus on FILTER TESTS
 * </p>
 * <ul>
 *     <li>After 20 seconds, <strong>MeteoBureau n°2</strong> sends a wind alert only on <strong>WindTurbine n°1</strong> exact position.</li>
 *     <li>After 40 seconds, <strong>MeteoStation n°2</strong> sends wind data with high force, 
 *         not being accepted by <strong>WindTurbine n°1</strong>'s filter on wind force
 *         nor <strong>WindTurbine 3</strong>'s filter on time.</li>
 *     <li>After 60 seconds, <strong>MeteoBureau n°2</strong> sends a dangerous alert exactly to <strong>WindTurbine n°3</strong>'s 
 *         position, not taken into account because its too soon to be accepted by its time filter.</li>
 *     <li>After 80 seconds, <strong>MeteoBureau n°2</strong> sends again a dangerous alert exactly to 
 *         <strong>WindTurbine n°3's</strong> position, taken into account this time.</li>
 *     <li>After 100 seconds, <strong>WindTurbine n°3</strong> unregisters itself from the system.</li>
 * </ul>
 *
 * <p>
 * 	SECOND HALF : focus on PRIVILEGED SERVICES TESTS
 * </p>
 * <ul>
 *     <li>After 120 seconds, <strong>MeteoBureau n°1</strong> creates a new channel : "testChannel".</li>
 *     <li>After 140 seconds, <strong>WindTurbine n°1</strong> unsubscribes from channels "channel0" and "channel1", 
 *         subscribes to channel "testChannel" + <strong>WindTurbine n°2</strong> subscribes to channel "testChannel".<br>
 *         Both with filters accepting everything.</li>
 *     <li>After 160 seconds, <strong>MeteoStation n°1</strong> sends wind on "channel0".</li>
 *     <li>After 180 seconds, <strong>MeteoStation n°1</strong> sends wind on "testChannel".</li>
 *     <li>After 200 seconds, <strong>MeteoBureau n°1</strong> changes 'testChannel' authorized users 
 *         for exactly <strong>WindTurbine n°1</strong> URI.</li>
 *     <li>After 220 seconds, <strong>MeteoBureau n°1</strong> sends a GREEN alert on the "testChannel", 
 *         received only by <strong>WindTurbine n°1</strong>.</li>
 *     <li>After 240 seconds, <strong>MeteoBureau n°1</strong> destroys its "testChannel" and verifies that it is has been deleted.</li>
 * </ul>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class CVMCompleteTest extends AbstractCVM {
    
	// -------------------------------------------------------------------------
	// Test constants and variables
	// -------------------------------------------------------------------------

	public static String CLOCK_URI = "test-clock";
	public static String START_INSTANT = "2026-01-30T09:00:00.00Z";
	public static String END_INSTANT = "2026-01-30T09:04:30.00Z";

	public static long DELAY_TO_START = 1000L;
	public static long END_SLEEP_DURATION = 10000L;
	public static double ACCELERATION_FACTOR = 2.0;
	public static long EXECUTION_DURATION =
		(long) (DELAY_TO_START + 1000L + ((Instant.parse(END_INSTANT).toEpochMilli() - Instant.parse(START_INSTANT).toEpochMilli())
														/ACCELERATION_FACTOR));
	
	// Delay after the start when steps will be executed, in seconds
	public static long[] STEPS = new long[] {20, 40, 60, 80, 100, 120, 140, 160, 180, 200,
			220, 240};
	public static final int NBSTEPS = STEPS.length;
	
	// -------------------------------------------------------------------------
	// Component datas
	// -------------------------------------------------------------------------
	
	// MeteoBureau 1
	protected final String MB1ReceivingInboundPortURI = "inbound-port-client-MB1";
	protected final String MB1RegistrationOutboundPortURI = "registration-outbound-port-MB1";
	protected final String MB1PublishingOutboundPortURI = "publishing-outbound-port-MB1";
	protected final String MB1PrivilegedOutboundPortURI = "privileged-outbound-port-MB1";
	protected static final PositionData bureau1Position = new PositionData(2, 1);
	protected static final RegistrationClass MB1rc = RegistrationClass.PREMIUM;
	
	// MeteoBureau 2
	protected final String MB2ReceivingInboundPortURI = "inbound-port-client-MB2";
	protected final String MB2RegistrationOutboundPortURI = "registration-outbound-port-MB2";
	protected final String MB2PublishingOutboundPortURI = "publishing-outbound-port-MB2";
	protected static final PositionData bureau2Position = new PositionData(3, 1);
	protected static final RegistrationClass MB2rc = RegistrationClass.FREE;
	
	// MeteoStation 1
	protected final String MS1ReceivingInboundPortURI = "inbound-port-client-MS1";
	protected final String MS1RegistrationOutboundPortURI = "registration-outbound-port-MS1";
	protected final String MS1PublishingOutboundPortURI = "publishing-outbound-port-MS1";
	protected final String MS1PrivilegedOutboundPortURI = "privileged-outbound-port-MS1";
	protected static final PositionData station1Position = new PositionData(2, 2);
	protected static final RegistrationClass MS1rc = RegistrationClass.STANDARD;
	
	// MeteoStation 2
	protected final String MS2ReceivingInboundPortURI = "inbound-port-client-MS2";
	protected final String MS2RegistrationOutboundPortURI = "registration-outbound-port-MS2";
	protected final String MS2PublishingOutboundPortURI = "publishing-outbound-port-MS2";
	protected static final PositionData station2Position = new PositionData(3, 2);
	protected static final RegistrationClass MS2rc = RegistrationClass.FREE;
	
	// Except for Wind turbine 3, this time filter is used : no constraint on time
	protected final static TimeFilterI timeFilterJoker = new JokerTimeFilter();
	
	// WindTurbine 1
	protected final static String WT1ReceivingInboundPortURI = "inbound-port-client-WT1";
	protected final String WT1RegistrationOutboundPortURI = "registration-outbound-port-WT1";
	protected final String WT1PublishingOutboundPortURI = "publishing-outbound-port-WT1";
	protected static final PositionData windTurbine1Position = new PositionData(1, 0);
	protected static final RegistrationClass WT1rc = RegistrationClass.STANDARD;
	protected static final MeteoAlertI.Level maxWorkingLevelWT1 = MeteoAlertI.Level.ORANGE; // inclusive
	
	// On winds received, filter keeps only close winds (distance <= 10) && with a force not exceeding 10.
	protected final PropertiesFilter[] WT1_props =  {new PropertiesFilter(new StrongWindMultiValuesFilter(windTurbine1Position, 10, 10))};
	protected final MessageFilterI WT1Windfilters =  new MessageFilter(new PropertyFilter[0], WT1_props, timeFilterJoker);
	// On alerts received, filter keeps only alerts with a region containing wind turbine's position
	protected final PropertyFilterI[] WT1_prop = {new PropertyFilter("regions", new PositionInRegionsValueFilter(windTurbine1Position))};
    protected final MessageFilterI WT1Alertfilters =  new MessageFilter(WT1_prop, new PropertiesFilter[0], timeFilterJoker);
	
	// WindTurbine 2
	protected final String WT2ReceivingInboundPortURI = "inbound-port-client-WT2";
	protected final String WT2RegistrationOutboundPortURI = "registration-outbound-port-WT2";
	protected final String WT2PublishingOutboundPortURI = "publishing-outbound-port-WT2";
	protected static final PositionData windTurbine2Position = new PositionData(2, 0);
	protected static final RegistrationClass WT2rc = RegistrationClass.STANDARD;
	protected static final MeteoAlertI.Level maxWorkingLevelWT2 = MeteoAlertI.Level.ORANGE; // inclusive
	
	// Accepts every wind data
	protected final MessageFilterI WT2Windfilters =  new MessageFilter(new PropertyFilter[0], new PropertiesFilter[0], timeFilterJoker);
	// On alerts received, filter keeps only alerts with a region containing wind turbine's position
	protected final PropertyFilterI[] WT2_prop = {new PropertyFilter("regions", new PositionInRegionsValueFilter(windTurbine2Position))};
	protected final MessageFilterI WT2Alertfilters =  new MessageFilter(WT2_prop, new PropertiesFilter[0], timeFilterJoker);
	
	// WindTurbine 3
	protected final String WT3ReceivingInboundPortURI = "inbound-port-client-WT3";
	protected final String WT3RegistrationOutboundPortURI = "registration-outbound-port-WT3";
	protected final String WT3PublishingOutboundPortURI = "publishing-outbound-port-WT3";
	protected static final PositionData windTurbine3Position = new PositionData(3, 0);
	protected static final RegistrationClass WT3rc = RegistrationClass.FREE;
	protected static final MeteoAlertI.Level maxWorkingLevelWT3 = MeteoAlertI.Level.ORANGE; // inclusive
	
	// Time constraint, filters accepts messages 80 (=STEP3) seconds after the start of the scenario
	protected final static TimeFilterI WT3timeFilterGE = new GreaterOrEqualTimeFilter(Instant.now().plusSeconds((long)(STEPS[3]/ACCELERATION_FACTOR)));
	// Accepts every wind data
	protected final MessageFilterI WT3Windfilters =  new MessageFilter(new PropertyFilter[0], new PropertiesFilter[0], WT3timeFilterGE);
	// On alerts received, filter keeps only alerts with a region containing wind turbine's position
	protected final PropertyFilterI[] WT3_prop = {new PropertyFilter("regions", new PositionInRegionsValueFilter(windTurbine3Position))};
	protected final MessageFilterI WT3Alertfilters =  new MessageFilter(WT3_prop, new PropertiesFilter[0], WT3timeFilterGE);
	
	// Component URIs for tests (without the Broker)
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
		
		// Subscription filter on Step 6 subscription, accepts everything
        MessageFilterI accept_all_filter = new MessageFilter(new PropertyFilterI[0], new PropertiesFilter[0], timeFilterJoker);
		
		return new TestScenario(
			CLOCK_URI,
			startInstant,
			endInstant,
			new TestStepI[] {
				// MeteoBureau2 sends wind data only on WindTubine1 exact position.
				new TestStep(
						CLOCK_URI,
						MB2_URI,
						step_Instants[0],
						owner -> {
							try {
								System.out.println("STEP 0/"+(NBSTEPS-1)+" ("+STEPS[0]+"s) : MeteoBureau2 sends a wind alert on WindTubine1 exact position");
								((MeteoBureauTest)owner).publishAlert(MeteoAlertI.AlertType.FLOODING, MeteoAlertI.Level.ORANGE, 
										WT1_region, step_Instants[0], Duration.ofSeconds(5), "channel1");
							} catch (Exception e) {
								System.err.println("Step 0 alert publication failed with exception " + e);
							}
						}),
				// MeteoStation2 sends wind data with light force, not being accepted by WindTubine1's filter on wind force.
				new TestStep(
					CLOCK_URI,
					MS2_URI,
					step_Instants[1],
					owner -> {
						try {
							System.out.println("STEP 1/"+(NBSTEPS-1)+" ("+STEPS[1]+"s) : MeteoStation2 sends wind data with light force, only taken into account by WindTubine2");
							((MeteoStationTest)owner).publishWindData(10.0, 10.0, "channel0", false);
						} catch (Exception e) {
							System.err.println("Step 1 wind data publication failed with exception " + e);
						}
					}),
				// MeteoBureau2 sends a dangerous alert exactly to WindTurbine3's position, 
				// not taken into account because its too soon to be accepted by its time filter
				new TestStep(
						CLOCK_URI,
						MB2_URI,
						step_Instants[2],
						owner -> {
							try {
								System.out.println("STEP 2/"+(NBSTEPS-1)+" ("+STEPS[2]+"s) : MeteoBureau2 sends a dangerous alert exactly to WindTurbine3's position (not taken into account, too early for time filter)");
								((MeteoBureauTest)owner).publishAlert(MeteoAlertI.AlertType.EARTHQUAKE, MeteoAlertI.Level.RED, 
										WT3_region, step_Instants[2], Duration.ofSeconds(5), "channel1");
							} catch (Exception e) {
								System.err.println("Step 2 alert publication failed with exception " + e);
							}
						}),
				// MeteoBureau2 sends again a dangerous alert exactly to WindTurbine3's position, 
				// taken into account this time
				new TestStep(
						CLOCK_URI,
						MB2_URI,
						step_Instants[3],
						owner -> {
							try {
								System.out.println("STEP 3/"+(NBSTEPS-1)+" ("+STEPS[3]+"s) : MeteoBureau2 sends again a dangerous alert exactly to WindTurbine3's position (taken into account)");
								((MeteoBureauTest)owner).publishAlert(MeteoAlertI.AlertType.ICY_STORM, MeteoAlertI.Level.SCARLET, 
										WT3_region, step_Instants[3], Duration.ofSeconds(5), "channel1");
							} catch (Exception e) {
								System.err.println("Step 3 alert publication failed with exception " + e);
							}
						}),
				// WindTurbine3 unregisters itself from the system
				new TestStep(
					CLOCK_URI,
					WT3_URI,
					step_Instants[4],
					owner -> {
						try {
							System.out.println("STEP 4/"+(NBSTEPS-1)+" ("+STEPS[4]+"s) : WindTurbine3 unregisters itself from the system");
							((WindTurbineTest)owner).unregisterFromSystem();
						} catch (Exception e) {
							System.err.println("Step 4 unregistration failed with exception " + e);
						}
					}),
				// MeteoBureau1 creates a new channel : "testChannel"
				new TestStep(
						CLOCK_URI,
						MB1_URI,
						step_Instants[5],
						owner -> {
							try {
								System.out.println("STEP 5/"+(NBSTEPS-1)+" ("+STEPS[5]+"s) : MeteoBureau1 creates a new channel : 'testChannel'");
								((MeteoBureauTest)owner).createNewChannel("testChannel", ".*");
							} catch (Exception e) {
								System.err.println("Step 5 new channel creation failed with exception " + e);
							}
						}),
				// WindTurbine1 unsubscribe from channels "channel0" and "channel1", subscribes to channel "testChannel"
				new TestStep(
						CLOCK_URI,
						WT1_URI,
						step_Instants[6],
						owner -> {
							try {
								System.out.println("STEP 6/"+(NBSTEPS-1)+" ("+STEPS[6]+"s) : WindTurbine1 unsubscribes from channels 'channel0' and 'channel1',\nWindTurbine1 + WindTurbine2 subscribe to channel 'testChannel'");
								((WindTurbineTest)owner).unsubscibeFromChannel("channel0");
								((WindTurbineTest)owner).unsubscibeFromChannel("channel1");
								((WindTurbineTest)owner).subscibeToChannel("testChannel", accept_all_filter);
							} catch (Exception e) {
								System.err.println("Step 6 WT1 unsubscriptions + subscription failed with exception " + e);
							}
						}),
				// WindTurbine2 subscribes to channel "testChannel"
				new TestStep(
						CLOCK_URI,
						WT2_URI,
						step_Instants[6],
						owner -> {
							try {
								((WindTurbineTest)owner).subscibeToChannel("testChannel", accept_all_filter);
							} catch (Exception e) {
								System.err.println("Step 6 WT2 subscription failed with exception " + e);
							}
						}),
				// MeteoStation1 sends wind on "channel0"
				new TestStep(
						CLOCK_URI,
						MS1_URI,
						step_Instants[7],
						owner -> {
							try {
								System.out.println("STEP 7/"+(NBSTEPS-1)+" ("+STEPS[7]+"s) : MeteoStation1 sends wind on 'channel0', \nwith only WindTurbine2 still subscribed to it");
								((MeteoStationTest)owner).publishWindData(1.0, 1.0, "channel0", false);
							} catch (Exception e) {
								System.err.println("Step 7 wind data publication failed with exception " + e);
							}
						}),
				// MeteoStation1 sends wind on "testChannel"
				new TestStep(
						CLOCK_URI,
						MS1_URI,
						step_Instants[8],
						owner -> {
							try {
								System.out.println("STEP 8/"+(NBSTEPS-1)+" ("+STEPS[8]+"s) : MeteoStation1 sends wind on 'testChannel', \nwith both WindTurbine 1 and 2 subscribed to it");
								((MeteoStationTest)owner).publishWindData(2.0, 2.0, "testChannel", false);
							} catch (Exception e) {
								System.err.println("Step 8 wind data publication failed with exception " + e);
							}
						}),
				// MeteoBureau1 changes authorized users for exactly WindTurbine1 URI
				new TestStep(
						CLOCK_URI,
						MB1_URI,
						step_Instants[9],
						owner -> {
							try {
								System.out.println("STEP 9/"+(NBSTEPS-1)+" ("+STEPS[9]+"s) : MeteoBureau1 changes 'testChannel' authorized users for exactly WindTurbine1 URI,\nimplicitly unsubsrcribing WindTurbine2");
								((MeteoBureauTest)owner).modifyAuthorizedUsers("testChannel", "^"+WT1ReceivingInboundPortURI+"$"); // inbound receiving port used on the broker
							} catch (Exception e) {
								System.err.println("Step 9 authorized users modification failed with exception " + e);
							}
						}),
				// MeteoBureau1 sends a GREEN alert on the "testChannel", received only by WindTurbine1
				new TestStep(
						CLOCK_URI,
						MB1_URI,
						step_Instants[10],
						owner -> {
							try {
								System.out.println("STEP 10/"+(NBSTEPS-1)+" ("+STEPS[10]+"s) : MeteoBureau1 sends a GREEN alert on the 'testChannel', received only by WindTurbine1");
								((MeteoBureauTest)owner).publishAlert(MeteoAlertI.AlertType.EARTHQUAKE, MeteoAlertI.Level.GREEN, 
										all_regions, step_Instants[10], Duration.ofSeconds(5), "testChannel");
							} catch (Exception e) {
								System.err.println("Step 10 alert publication failed with exception " + e);
							}
						}),
				// MeteoBureau11 destroys its "testChannel" and verifies that it is has been deleted
				new TestStep(
						CLOCK_URI,
						MB1_URI,
						step_Instants[11],
						owner -> {
							try {
								System.out.println("STEP 11/"+(NBSTEPS-1)+" ("+STEPS[11]+"s) : MeteoBureau1 destroys its 'testChannel' and verifies that it is has been deleted");
								((MeteoBureauTest)owner).destroyChannelAndVerifies("testChannel");
							} catch (Exception e) {
								System.err.println("Step 11 'testChannel' destruction + verification failed with exception " + e);
							}
						})
				});
	}
	
	public CVMCompleteTest() throws Exception{
		super();
	}
	
	@Override
	public void	deploy() throws Exception{
		assert	!this.deploymentDone() ;
		
		long current = System.currentTimeMillis();
		long unixEpochStartTimeInNanos = TimeUnit.MILLISECONDS.toNanos(current + DELAY_TO_START);
		
		Instant	startInstant = Instant.parse(START_INSTANT);
		TestScenario testScenario = testScenario();
		
		// ---------------------------------------------------------------------
		// Creation phase
		// ---------------------------------------------------------------------
		
		// Broker creation
		int nbMaxChannelsSTD = 2;
		int nbMaxChannelsPREM = 4;
		String broker_URI = AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] {2, nbMaxChannelsSTD, nbMaxChannelsPREM, 1, 0, 1, 1, 1});
		assert this.isDeployedComponent(broker_URI);
		this.toggleTracing(broker_URI);

		
		// Creation of MeteoBureaus
		
		String meteoBureau1_URI_verif = AbstractComponent.createComponent(
				MeteoBureauTest.class.getCanonicalName(),
				new Object[] { MB1ReceivingInboundPortURI, MB1RegistrationOutboundPortURI, MB1PublishingOutboundPortURI, MB1PrivilegedOutboundPortURI,
						MB1rc, 1, 1, bureau1Position, MB1_URI, testScenario, 1});
		assert MB1_URI.equals(meteoBureau1_URI_verif);
		assert this.isDeployedComponent(MB1_URI);
		this.toggleTracing(MB1_URI);
		
		String meteoBureau2_URI_verif = AbstractComponent.createComponent(
				MeteoBureauTest.class.getCanonicalName(),
				new Object[] { MB2ReceivingInboundPortURI, MB2RegistrationOutboundPortURI, MB2PublishingOutboundPortURI, "", // privileged port URI empty
						MB2rc, 1, 1, bureau2Position, MB2_URI, testScenario, 2});
		assert MB2_URI.equals(meteoBureau2_URI_verif);
		assert this.isDeployedComponent(MB2_URI);
		this.toggleTracing(MB2_URI);
		
		// Creation of MeteoStations
		
		String meteoStation1_URI_verif = AbstractComponent.createComponent(
				MeteoStationTest.class.getCanonicalName(),
				new Object[] { MS1ReceivingInboundPortURI, MS1RegistrationOutboundPortURI, MS1PublishingOutboundPortURI, MS1PrivilegedOutboundPortURI,
						MS1rc, 1, 1, station1Position, MS1_URI, testScenario, 1});
		assert MS1_URI.equals(meteoStation1_URI_verif);
		assert this.isDeployedComponent(MS1_URI);
		this.toggleTracing(MS1_URI);
	
		
		String meteoStation2_URI_verif = AbstractComponent.createComponent(
				MeteoStationTest.class.getCanonicalName(),
				new Object[] { MS2ReceivingInboundPortURI, MS2RegistrationOutboundPortURI, MS2PublishingOutboundPortURI, "", // privileged port URI empty
						MS2rc, 1, 1, station2Position, MS2_URI, testScenario, 2});
		assert MS2_URI.equals(meteoStation2_URI_verif);
		assert this.isDeployedComponent(MS2_URI);
		this.toggleTracing(MS2_URI);
		
		// Creation of WindTurbines
		
		String windTurbine1_URI_verif = AbstractComponent.createComponent(WindTurbineTest.class.getCanonicalName(),
				new Object[] { WT1ReceivingInboundPortURI, WT1RegistrationOutboundPortURI, WT1PublishingOutboundPortURI,
						WT1rc, 1, 1, windTurbine1Position, maxWorkingLevelWT1, WT1_URI, testScenario, 1, WT1Windfilters, WT1Alertfilters});
		this.toggleTracing(windTurbine1_URI_verif);
		
		String windTurbine2_URI_verif = AbstractComponent.createComponent(WindTurbineTest.class.getCanonicalName(),
				new Object[] { WT2ReceivingInboundPortURI, WT2RegistrationOutboundPortURI, WT2PublishingOutboundPortURI,
						WT2rc, 1, 1, windTurbine2Position, maxWorkingLevelWT2, WT2_URI, testScenario, 2, WT2Windfilters, WT2Alertfilters});
		this.toggleTracing(windTurbine2_URI_verif);
		
		String windTurbine3_URI_verif = AbstractComponent.createComponent(WindTurbineTest.class.getCanonicalName(),
				new Object[] { WT3ReceivingInboundPortURI, WT3RegistrationOutboundPortURI, WT3PublishingOutboundPortURI,
						WT3rc, 1, 1, windTurbine3Position, maxWorkingLevelWT3, WT3_URI, testScenario, 3, WT3Windfilters, WT3Alertfilters});
		this.toggleTracing(windTurbine3_URI_verif);
		
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

		// --------------------------------------------------------------------
		// Deployment done
		// --------------------------------------------------------------------

		super.deploy();
		assert this.deploymentDone();
	}
	
	public static void main(String[] args) {
		try {
			VerboseException.VERBOSE = true;
			VerboseException.PRINT_STACK_TRACE = true;
			
			CVMCompleteTest cvm = new CVMCompleteTest() ;
			cvm.startStandardLifeCycle(EXECUTION_DURATION);
			Thread.sleep(END_SLEEP_DURATION);
			System.exit(0) ;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
