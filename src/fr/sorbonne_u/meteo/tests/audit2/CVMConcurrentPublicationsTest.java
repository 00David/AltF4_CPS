package fr.sorbonne_u.meteo.tests.audit2;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.broker.component.Broker;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.components.utils.tests.TestStep;
import fr.sorbonne_u.components.utils.tests.TestStepI;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.VerboseException;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.property.PropertiesFilter;
import fr.sorbonne_u.filters.property.PropertyFilter;
import fr.sorbonne_u.filters.time.JokerTimeFilter;
import fr.sorbonne_u.meteo.components.MeteoStation;
import fr.sorbonne_u.meteo.components.WindTurbine;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.meteo.data.WindData;
import fr.sorbonne_u.utils.aclocks.ClocksServer;

/**
 * Test class to simulate the following scenario from the start of the CVM.
 * 
 * <p>
 * 	SETUP
 * </p>
 * <ul>
 *     <li>The {@link Broker} has :
 *         <ul>
 *             <li>4 normal threads</li>
 *             <li>2 FREE loading threads, 4 STANDARD loading threads, 8 PREMIUM loading threads</li>
 *             <li>4 unloading threads</li>
 *             <li>4 sending threads</li>
 *         </ul>
 *     </li>
 *     <li>1 {@link WindTurbine} accepting everything, with 4 threads</li>
 *     <li>3 FREE {@link MeteoStation}</li>
 *     <li>3 STANDARD {@link MeteoStation}</li>
 *     <li>3 PREMIUM {@link MeteoStation}</li>
 * </ul>
 * 
 * <p>
 *	SCENARIO, FIRST PART : multi-thread publications 
 * 	(messages sent from the channel one by one) :
 * </p>
 * <ul>
 * 	<li> - After 20 seconds, <strong>all 3 FREE MeteoStations</strong> each send 30 wind datas to 'channel0'.</li>
 * 	<li> - After 40 seconds, <strong>WindTurbine</strong> checks that it has received 90 wind datas.</li>
 * 	<li> - After 60 seconds, <strong>all 3 STANDARD MeteoStations</strong> each send 30 wind datas to 'channel0'.</li>
 * 	<li> - After 80 seconds, <strong>WindTurbine</strong> checks that it has received a total of 180 wind datas.</li>
 * 	<li> - After 100 seconds, <strong>all 3 PREMIUM MeteoStations</strong> each send 30 wind datas to 'channel0'.</li>
 * 	<li> - After 120 seconds, <strong>WindTurbine</strong> checks that it has received a total of 270 wind datas.</li>
 * </ul>
 * 
 * <p>
 * 	SECOND PART : multi-thread publications on different channels 
 * 	(messages sent from the channel in groups of max 5 or 10, depending on creator registration class) :
 * </p>
 * <ul>
 * 	<li> - After 130 seconds, <strong>MeteoStation4 (STANDARD)</strong> creates 'testChannel0' 
 * 			+ <strong>MeteoStation7 (PREMIUM)</strong> creates 'testChannel1'.</li>
 * 	<li> - After 140 seconds, <strong>WindTurbine</strong> subscribes to 'testChannel0' and 'testChannel1'</li>
 * 	<li> - After 150 seconds, <strong>all 3 STANDARD MeteoStations</strong> each send 29 wind datas to 'testChannel0' (created by a STANDARD).
 * 			Messages are sent from the channel in <strong>groups of max 5</strong>.</li>
 * 	<li> - After 170 seconds, <strong>WindTurbine</strong> checks that it has received a total of 357 wind datas.</li>
 * 	<li> - After 190 seconds, <strong>all 3 STANDARD MeteoStations</strong> each send 29 wind datas to 'testChannel1' (created by a PREMIUM).
 * 			Messages are sent from the channel in <strong>groups of max 10</strong>.</li>
 * 	<li> - After 210 seconds, <strong>WindTurbine</strong> checks that it has received a total of 444 wind datas.</li>
 * </ul>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class CVMConcurrentPublicationsTest extends AbstractCVM {
	
	// -------------------------------------------------------------------------
	// Test constants and variables
	// -------------------------------------------------------------------------

	public static String CLOCK_URI = "test-clock";
	public static String START_INSTANT = "2026-01-30T09:00:00.00Z";
	public static String END_INSTANT = "2026-01-30T09:03:30.00Z";

	public static long DELAY_TO_START = 1000L;
	public static long END_SLEEP_DURATION = 20000L;
	public static double ACCELERATION_FACTOR = 2.0;
	public static long EXECUTION_DURATION =
		(long) (DELAY_TO_START + 1000L + ((Instant.parse(END_INSTANT).toEpochMilli() - Instant.parse(START_INSTANT).toEpochMilli())
														/ACCELERATION_FACTOR));
	
	// Delay after the start when steps be executed, in seconds
	public static long[] STEPS = new long[] {20, 40, 60, 80, 100, 120, 130, 140, 150, 170,
			190, 210};
	public static final int NBSTEPS = STEPS.length;
	
	// -------------------------------------------------------------------------
	// Component datas
	// -------------------------------------------------------------------------

	// Number of threads
	protected final int brokerNbThreads = 4;
	protected final int brokerNbSchedulableThreads = 0;
	protected final int brokerNbFreeLoadersThreads = 2; // Broker's constructor automatically scales at 4 and 8 threads for Standard and Premium senders.
	protected final int brokerNbUnloadersThreads = 4;
	protected final int brokerNbSendersThreads = 4;
	protected final int brokerNbGossipThreads = 1; // unused
	
	protected final int clientNbThreads = 4;
	protected final int clientNbSchedulableThreads = 1;
	
	// MeteoStation general URIs
	protected final String MSReceivingInboundPortURI = "inbound-port-client-MS";
	
	protected static final PositionData FREEstationPosition = new PositionData(1, 2);
	protected static final PositionData STANDARDstationPosition = new PositionData(2, 2);
	protected static final PositionData PREMIUMstationPosition = new PositionData(3, 2);
	
	// WindTurbine
	protected final String WindTurbineReceivingInboundPortURI = "inbound-port-client-WindTurbine";
	protected final PositionData windTurbinePosition = new PositionData(2, 0);
	
    protected final static TimeFilterI timeFilter = new JokerTimeFilter();
    protected final static MessageFilterI windTurbineWindFilter = new MessageFilter(new PropertyFilter[0], new PropertiesFilter[0], timeFilter);
    protected final MessageFilterI windTurbineAlertFilter = new MessageFilter(new PropertyFilter[0], new PropertiesFilter[0], timeFilter);
	
	// Broker parameters
	protected final int nbDefaultChannels = 2;
	protected final int nbMaxChannelsStd = 2;
	protected final int nbMaxChannelsPrem = 4;
    
	// Component URIs for tests (without the Broker)
	protected static String MS_URI = "MeteoStation-URI";
	protected static String WT_URI = "WindTurbine-URI";
    
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
		
		// Wind datas
		List<WindData> ms1_winds = new ArrayList<>(); List<WindData> ms2_winds = new ArrayList<>(); List<WindData> ms3_winds = new ArrayList<>();
		for (int j=0; j<90; j++) {
			if (j<30) ms1_winds.add(new WindData(FREEstationPosition, j, j));
			else if (j<60) ms2_winds.add(new WindData(FREEstationPosition, j, j));
			else ms3_winds.add(new WindData(FREEstationPosition, j, j));
		}
		
		List<WindData> ms4_winds = new ArrayList<>(); List<WindData> ms5_winds = new ArrayList<>(); List<WindData> ms6_winds = new ArrayList<>();
		for (int j=0; j<90; j++) {
			if (j<30) ms4_winds.add(new WindData(STANDARDstationPosition, j, j));
			else if (j<60) ms5_winds.add(new WindData(STANDARDstationPosition, j, j));
			else ms6_winds.add(new WindData(STANDARDstationPosition, j, j));
		}
		
		List<WindData> ms7_winds = new ArrayList<>(); List<WindData> ms8_winds = new ArrayList<>(); List<WindData> ms9_winds = new ArrayList<>();
		for (int j=0; j<90; j++) {
			if (j<30) ms7_winds.add(new WindData(PREMIUMstationPosition, j, j));
			else if (j<60) ms8_winds.add(new WindData(PREMIUMstationPosition, j, j));
			else ms9_winds.add(new WindData(PREMIUMstationPosition, j, j));
		}
		
		return new TestScenario(
			CLOCK_URI,
			startInstant,
			endInstant,
			new TestStepI[] {
				// all 3 FREE MeteoStations each send 30 wind datas to 'channel0'.
				new TestStep(
					CLOCK_URI, MS_URI+1, step_Instants[0],
					owner -> {
						try {
							System.out.println("STEP 0/"+(NBSTEPS-1)+" ("+STEPS[0]+"s) : all 3 FREE MeteoStations each send 30 wind datas to 'channel0'");
							((MeteoStation)owner).publishWindDatas(ms1_winds, "channel0", false);
						} catch (Exception e) {
							System.err.println("MeteoStation1, step 0 wind datas publication failed with exception " + e);
						}
					}),
				new TestStep(
						CLOCK_URI, MS_URI+2, step_Instants[0],
						owner -> {
							try {
								((MeteoStation)owner).publishWindDatas(ms2_winds, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation2, step 0 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+3, step_Instants[0],
						owner -> {
							try {
								((MeteoStation)owner).publishWindDatas(ms3_winds, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation3, step 0 wind datas publication failed with exception " + e);
							}
						}),
				// WindTurbine checks that it has received 90 wind datas.
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[1],
						owner -> {
							try {
								System.out.println("STEP 1/"+(NBSTEPS-1)+" ("+STEPS[1]+"s) : WindTurbine checks that it has received 90 wind datas");
								((WindTurbine)owner).verifyNbWindDatasReceived(90);
							} catch (Exception e) {
								System.err.println("Step 1 wind datas account check failed with exception " + e);
							}
						}),
				// all 3 STANDARD MeteoStations each send 30 wind datas to 'channel0'.
				new TestStep(
						CLOCK_URI, MS_URI+4, step_Instants[2],
						owner -> {
							try {
								System.out.println("STEP 2/"+(NBSTEPS-1)+" ("+STEPS[2]+"s) : all 3 STANDARD MeteoStations each send 30 wind datas to 'channel0'");
								((MeteoStation)owner).publishWindDatas(ms4_winds, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation4, step 2 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+5, step_Instants[2],
						owner -> {
							try {
								((MeteoStation)owner).publishWindDatas(ms5_winds, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation5, step 2 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+6, step_Instants[2],
						owner -> {
							try {
								((MeteoStation)owner).publishWindDatas(ms6_winds, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation6, step 2 wind datas publication failed with exception " + e);
							}
						}),
				// WindTurbine checks that it has received 180 wind datas.
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[3],
						owner -> {
							try {
								System.out.println("STEP 3/"+(NBSTEPS-1)+" ("+STEPS[3]+"s) : WindTurbine checks that it has received 180 wind datas");
								((WindTurbine)owner).verifyNbWindDatasReceived(180);
							} catch (Exception e) {
								System.err.println("Step 3 wind datas account check failed with exception " + e);
							}
						}),
				// all 3 PREMIUM MeteoStations each send 30 wind datas to 'channel0'.
				new TestStep(
						CLOCK_URI, MS_URI+7, step_Instants[4],
						owner -> {
							try {
								System.out.println("STEP 4/"+(NBSTEPS-1)+" ("+STEPS[4]+"s) : all 3 PREMIUM MeteoStations each send 30 wind datas to 'channel0'");
								((MeteoStation)owner).publishWindDatas(ms7_winds, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation7, step 4 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+8, step_Instants[4],
						owner -> {
							try {
								((MeteoStation)owner).publishWindDatas(ms8_winds, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation8, step 4 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+9, step_Instants[4],
						owner -> {
							try {
								((MeteoStation)owner).publishWindDatas(ms9_winds, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation9, step 4 wind datas publication failed with exception " + e);
							}
						}),
				// WindTurbine checks that it has received 270 wind datas.
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[5],
						owner -> {
							try {
								System.out.println("STEP 5/"+(NBSTEPS-1)+" ("+STEPS[5]+"s) : WindTurbine checks that it has received 270 wind datas");
								((WindTurbine)owner).verifyNbWindDatasReceived(270);
							} catch (Exception e) {
								System.err.println("Step 5 wind datas account check failed with exception " + e);
							}
						}),
				// MeteoStation4 (STANDARD) creates 'testChannel0' + MeteoStation7 (PREMIUM) creates 'testChannel1'.
				new TestStep(
						CLOCK_URI, MS_URI+4, step_Instants[6],
						owner -> {
							try {
								System.out.println("STEP 6/"+(NBSTEPS-1)+" ("+STEPS[6]+"s) : MeteoStation4 (STANDARD) creates 'testChannel0' + MeteoStation7 (PREMIUM) creates 'testChannel1'");
								((MeteoStation)owner).createNewChannel("testChannel0", ".*");
							} catch (Exception e) {
								System.err.println("Step 6 'testChannel0' creation failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+7, step_Instants[6],
						owner -> {
							try {
								((MeteoStation)owner).createNewChannel("testChannel1", ".*");
							} catch (Exception e) {
								System.err.println("Step 6 'testChannel1' creation failed with exception " + e);
							}
						}),
				// WindTurbine subscribes to channels 'testChannel0' and 'testChannel1'.
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[7],
						owner -> {
							try {
								System.out.println("STEP 7/"+(NBSTEPS-1)+" ("+STEPS[7]+"s) : WindTurbine subscribes to channels 'testChannel0' and 'testChannel1'");
								((WindTurbine)owner).subscribeToChannel("testChannel0", windTurbineWindFilter);
								((WindTurbine)owner).subscribeToChannel("testChannel1", windTurbineWindFilter);
							} catch (Exception e) {
								System.err.println("Step 7 WindTurbine subscrition failed with exception " + e);
							}
						}),
				// all 3 STANDARD MeteoStations each send 30 wind datas to 'testChannel0'.
				new TestStep(
						CLOCK_URI, MS_URI+4, step_Instants[8],
						owner -> {
							try {
								System.out.println("STEP 8/"+(NBSTEPS-1)+" ("+STEPS[8]+"s) : all 3 STANDARD MeteoStations each send 29 wind datas to 'testChannel0' (created by a STANDARD).\n"
										+ "Messages are sent from the channel in groups of max 5.");
								((MeteoStation)owner).publishWindDatas(ms4_winds.subList(0, ms4_winds.size() - 1), "testChannel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation4, step 8 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+5, step_Instants[8],
						owner -> {
							try {
								((MeteoStation)owner).publishWindDatas(ms5_winds.subList(0, ms5_winds.size() - 1), "testChannel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation5, step 8 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+6, step_Instants[8],
						owner -> {
							try {
								((MeteoStation)owner).publishWindDatas(ms6_winds.subList(0, ms6_winds.size() - 1), "testChannel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation6, step 8 wind datas publication failed with exception " + e);
							}
						}),
				// WindTurbine checks that it has received 357 wind datas.
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[9],
						owner -> {
							try {
								System.out.println("STEP 9/"+(NBSTEPS-1)+" ("+STEPS[9]+"s) : WindTurbine checks that it has received 357 wind datas");
								((WindTurbine)owner).verifyNbWindDatasReceived(357);
							} catch (Exception e) {
								System.err.println("Step 9 wind datas account check failed with exception " + e);
							}
						}),
				// all 3 STANDARD MeteoStations each send 30 wind datas to 'testChannel1'.
				new TestStep(
						CLOCK_URI, MS_URI+4, step_Instants[10],
						owner -> {
							try {
								System.out.println("STEP 10/"+(NBSTEPS-1)+" ("+STEPS[10]+"s) : all 3 STANDARD MeteoStations each send 29 wind datas to 'testChannel1' (created by a PREMIUM).\n"
										+ "Messages are sent from the channel in groups of max 10.");
								((MeteoStation)owner).publishWindDatas(ms4_winds.subList(0, ms4_winds.size() - 1), "testChannel1", false);
							} catch (Exception e) {
								System.err.println("MeteoStation4, step 10 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+5, step_Instants[10],
						owner -> {
							try {
								((MeteoStation)owner).publishWindDatas(ms5_winds.subList(0, ms5_winds.size() - 1), "testChannel1", false);
							} catch (Exception e) {
								System.err.println("MeteoStation5, step 10 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+6, step_Instants[10],
						owner -> {
							try {
								((MeteoStation)owner).publishWindDatas(ms6_winds.subList(0, ms6_winds.size() - 1), "testChannel1", false);
							} catch (Exception e) {
								System.err.println("MeteoStation6, step 10 wind datas publication failed with exception " + e);
							}
						}),
				// WindTurbine checks that it has received 444 wind datas.
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[11],
						owner -> {
							try {
								System.out.println("STEP 11/"+(NBSTEPS-1)+" ("+STEPS[11]+"s) : WindTurbine checks that it has received 444 wind datas");
								((WindTurbine)owner).verifyNbWindDatasReceived(444);
							} catch (Exception e) {
								System.err.println("Step 11 wind datas account check failed with exception " + e);
							}
						}),
				});
	}

	public CVMConcurrentPublicationsTest() throws Exception{
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
		String broker_URI = AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] {1, new PositionData(0, 4), nbDefaultChannels, nbMaxChannelsStd, nbMaxChannelsPrem, brokerNbThreads, 
						brokerNbSchedulableThreads, brokerNbFreeLoadersThreads, brokerNbUnloadersThreads, brokerNbSendersThreads, 
						brokerNbGossipThreads, "BrokerGossipInboundPortURI", new ArrayList<>(), new ArrayList<>()}); // Gossip protocol unused here
		assert this.isDeployedComponent(broker_URI);
		this.toggleTracing(broker_URI);
		
		// Creation of 9 MeteoStations
		for (int id=1; id<10; id++) {
			
			RegistrationClass rc = id <= 3 ? RegistrationClass.FREE : (id <= 6 ? RegistrationClass.STANDARD : RegistrationClass.PREMIUM);
			PositionData pos = id <= 3 ? FREEstationPosition : (id <= 6 ? STANDARDstationPosition : PREMIUMstationPosition);
			
			String meteoStation_URI_verif = AbstractComponent.createComponent(
					MeteoStation.class.getCanonicalName(),
					new Object[] {id, MS_URI+id, MSReceivingInboundPortURI+id, rc, clientNbThreads, clientNbSchedulableThreads, 
							pos, testScenario});
			assert (MS_URI+id).equals(meteoStation_URI_verif);
			assert this.isDeployedComponent(MS_URI+id);
			this.toggleTracing(MS_URI+id);
		}
		
		// Creation of 1 WindTurbine
		WT_URI = AbstractComponent.createComponent(WindTurbine.class.getCanonicalName(),
				new Object[] {1, WT_URI, WindTurbineReceivingInboundPortURI, RegistrationClass.STANDARD, clientNbThreads, 
						clientNbSchedulableThreads, windTurbinePosition, MeteoAlertI.Level.ORANGE, 
						true, windTurbineWindFilter, windTurbineAlertFilter, testScenario});
		assert this.isDeployedComponent(WT_URI);
		this.toggleTracing(WT_URI);
		
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
			VerboseException.VERBOSE = false;
			VerboseException.PRINT_STACK_TRACE = false;
			
			CVMConcurrentPublicationsTest cvm = new CVMConcurrentPublicationsTest() ;
			cvm.startStandardLifeCycle(EXECUTION_DURATION);
			Thread.sleep(END_SLEEP_DURATION);
			System.exit(0) ;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
