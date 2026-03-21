package fr.sorbonne_u.meteo.tests;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import fr.sorbonne_u.broker.components.Broker;
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
 *     <li>1 (STANDARD) {@link WindTurbineTest} accepting everything, with 4 threads</li>
 *     <li>3 FREE {@link MeteoStationTest}, each with 4 threads</li>
 *     <li>3 STANDARD {@link MeteoStationTest}, each with 4 threads</li>
 *     <li>3 PREMIUM {@link MeteoStationTest}, each with 4 threads</li>
 * </ul>
 * 
 * <p>
 * 	SCENARIO, FIRST PART : multi-thread publications
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
 * 	SECOND PART : multi-thread publications on different channels (depending creator registration classes)
 * </p>
 * <ul>
 * 	<li> - After 130 seconds, <strong>MeteoStation4 (STANDARD)</strong> creates 'testChannel0' 
 * 			+ <strong>MeteoStation7 (PREMIUM)</strong> creates 'testChannel1'.</li>
 * 	<li> - After 140 seconds, <strong>WindTurbine</strong> subscribes to 'testChannel0' and 'testChannel1'</li>
 * 	<li> - After 150 seconds, <strong>all 3 STANDARD MeteoStations</strong> each send 29 wind datas to 'testChannel0'.</li>
 * 	<li> - After 170 seconds, <strong>WindTurbine</strong> checks that it has received a total of 357 wind datas.</li>
 * 	<li> - After 190 seconds, <strong>all 3 STANDARD MeteoStations</strong> each send 29 wind datas to 'testChannel1'.</li>
 * 	<li> - After 210 seconds, <strong>WindTurbine</strong> checks that it has received a total of 444 wind datas.</li>
 * </ul>
 * 
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class CVMConcurrentTest extends AbstractCVM {
	
	// -------------------------------------------------------------------------
	// Test constants and variables
	// -------------------------------------------------------------------------

	public static String CLOCK_URI = "test-clock";
	public static String START_INSTANT = "2026-01-30T09:00:00.00Z";
	public static String END_INSTANT = "2026-01-30T09:03:40.00Z";

	public static long DELAY_TO_START = 1000L;
	public static long END_SLEEP_DURATION = 10000L;
	public static double ACCELERATION_FACTOR = 2.0;
	public static long EXECUTION_DURATION =
		(long) (DELAY_TO_START + 1000L + ((Instant.parse(END_INSTANT).toEpochMilli() - Instant.parse(START_INSTANT).toEpochMilli())
														/ACCELERATION_FACTOR));
	
	// Delay after the start when the step will be executed, in seconds
	public static long STEP0 = 20; // all 3 FREE MeteoStations each send 30 wind datas to 'channel0'
	public static long STEP1 = 40; // WindTurbine checks that it has received 90 wind datas.
	public static long STEP2 = 60; // all 3 STANDARD MeteoStations each send 30 wind datas to 'channel0'
	public static long STEP3 = 80; // WindTurbine checks that it has received 180 wind datas.
	public static long STEP4 = 100; // all 3 PREMIUM MeteoStations each send 30 wind datas to 'channel0'
	public static long STEP5 = 120; // WindTurbine checks that it has received 270 wind datas.
	public static long STEP6 = 130; // MeteoStation4 (STANDARD) creates 'testChannel0' + MeteoStation7 (PREMIUM) creates 'testChannel1'.
	public static long STEP7 = 140; // WindTurbine subscribes to channels 'testChannel0' and 'testChannel1'.
	public static long STEP8 = 150; // all 3 STANDARD MeteoStations each send 29 wind datas to 'testChannel0'.
	public static long STEP9 = 170; // WindTurbine checks that it has received 357 wind datas.
	public static long STEP10 = 190; // all 3 STANDARD MeteoStations each send 29 wind datas to 'testChannel1'.
	public static long STEP11 = 210; // WindTurbine checks that it has received 444 wind datas.
	
	// -------------------------------------------------------------------------
	// Component datas
	// -------------------------------------------------------------------------
	
	// Broker
	protected static final int brokerNbThreads = 4;
	protected static final int brokerNbFreeLoadersThreads = 2; // Broker's constructor automatically scales at 4 and 8 threads for Standard and Premium senders.
	protected static final int brokerNbUnloadersThreads = 4;
	protected static final int brokerNbSendersThreads = 4;
	
	// MeteoStation general URIs
	protected final static String MS_URI = "MeteoStation-URI";
	protected final String MSReceivingInboundPortURI = "inbound-port-client-MS";
	protected final String MSRegistrationOutboundPortURI = "registration-outbound-port-MS";
	protected final String MSPublishingOutboundPortURI = "publishing-outbound-port-MS";
	protected final String MSPrivilegedOutboundPortURI = "privileged-outbound-port-MS";
	
	protected static final int stationNbThreads = 4;
	protected static final PositionData FREEstationPosition = new PositionData(1, 2);
	protected static final PositionData STANDARDstationPosition = new PositionData(2, 2);
	protected static final PositionData PREMIUMstationPosition = new PositionData(3, 2);
	
	// WindTurbine
	protected final static String WT_URI = "WindTurbine-URI";
	protected final String WindTurbineReceivingInboundPortURI = "inbound-port-client-WindTurbine";
	protected final String WindTurbineRegistrationOutboundPortURI = "registration-outbound-port-WindTurbine";
	protected final String WindTurbinePublishingOutboundPortURI = "publishing-outbound-port-WindTurbine";
	
	protected static final int windTurbineNbThreads = 4;
	protected static final PositionData windTurbinePosition = new PositionData(2, 0);
	
    protected static final TimeFilterI timeFilter = new JokerTimeFilter();
    protected static final MessageFilterI windTurbineWindFilter = new MessageFilter(new PropertyFilter[0], new PropertiesFilter[0], timeFilter);
    protected static final MessageFilterI windTurbineAlertFilter = new MessageFilter(new PropertyFilter[0], new PropertiesFilter[0], timeFilter);
	
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
		
		Instant step0_Instant = startInstant.plusSeconds(STEP0);
		Instant step1_Instant = startInstant.plusSeconds(STEP1);
		Instant step2_Instant = startInstant.plusSeconds(STEP2);
		Instant step3_Instant = startInstant.plusSeconds(STEP3);
		Instant step4_Instant = startInstant.plusSeconds(STEP4);
		Instant step5_Instant = startInstant.plusSeconds(STEP5);
		Instant step6_Instant = startInstant.plusSeconds(STEP6);
		Instant step7_Instant = startInstant.plusSeconds(STEP7);
		Instant step8_Instant = startInstant.plusSeconds(STEP8);
		Instant step9_Instant = startInstant.plusSeconds(STEP9);
		Instant step10_Instant = startInstant.plusSeconds(STEP10);
		Instant step11_Instant = startInstant.plusSeconds(STEP11);
		
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
					CLOCK_URI, MS_URI+1, step0_Instant,
					owner -> {
						try {
							System.out.println("STEP 0/10 (20s) : all 3 FREE MeteoStations each send 30 wind datas to 'channel0'");
							((MeteoStationTest)owner).publishWindDatas(ms1_winds, "channel0");
						} catch (Exception e) {
							System.err.println("MeteoStation1, step 0 wind datas publication failed with exception " + e);
						}
					}),
				new TestStep(
						CLOCK_URI, MS_URI+2, step0_Instant,
						owner -> {
							try {
								((MeteoStationTest)owner).publishWindDatas(ms2_winds, "channel0");
							} catch (Exception e) {
								System.err.println("MeteoStation2, step 0 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+3, step0_Instant,
						owner -> {
							try {
								((MeteoStationTest)owner).publishWindDatas(ms3_winds, "channel0");
							} catch (Exception e) {
								System.err.println("MeteoStation3, step 0 wind datas publication failed with exception " + e);
							}
						}),
				// WindTurbine checks that it has received 90 wind datas.
				new TestStep(
						CLOCK_URI, WT_URI, step1_Instant,
						owner -> {
							try {
								System.out.println("STEP 1/11 (40s) : WindTurbine checks that it has received 90 wind datas");
								((WindTurbineTest)owner).verifyNbMessagesReceived(90);
							} catch (Exception e) {
								System.err.println("Step 1 wind datas account check failed with exception " + e);
							}
						}),
				// all 3 STANDARD MeteoStations each send 30 wind datas to 'channel0'.
				new TestStep(
						CLOCK_URI, MS_URI+4, step2_Instant,
						owner -> {
							try {
								System.out.println("STEP 2/11 (60s) : all 3 STANDARD MeteoStations each send 30 wind datas to 'channel0'");
								((MeteoStationTest)owner).publishWindDatas(ms4_winds, "channel0");
							} catch (Exception e) {
								System.err.println("MeteoStation4, step 2 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+5, step2_Instant,
						owner -> {
							try {
								((MeteoStationTest)owner).publishWindDatas(ms5_winds, "channel0");
							} catch (Exception e) {
								System.err.println("MeteoStation5, step 2 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+6, step2_Instant,
						owner -> {
							try {
								((MeteoStationTest)owner).publishWindDatas(ms6_winds, "channel0");
							} catch (Exception e) {
								System.err.println("MeteoStation6, step 2 wind datas publication failed with exception " + e);
							}
						}),
				// WindTurbine checks that it has received 180 wind datas.
				new TestStep(
						CLOCK_URI, WT_URI, step3_Instant,
						owner -> {
							try {
								System.out.println("STEP 3/11 (80s) : WindTurbine checks that it has received 180 wind datas");
								((WindTurbineTest)owner).verifyNbMessagesReceived(180);
							} catch (Exception e) {
								System.err.println("Step 3 wind datas account check failed with exception " + e);
							}
						}),
				// all 3 PREMIUM MeteoStations each send 30 wind datas to 'channel0'.
				new TestStep(
						CLOCK_URI, MS_URI+7, step4_Instant,
						owner -> {
							try {
								System.out.println("STEP 4/11 (100s) : all 3 PREMIUM MeteoStations each send 30 wind datas to 'channel0'");
								((MeteoStationTest)owner).publishWindDatas(ms7_winds, "channel0");
							} catch (Exception e) {
								System.err.println("MeteoStation7, step 4 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+8, step4_Instant,
						owner -> {
							try {
								((MeteoStationTest)owner).publishWindDatas(ms8_winds, "channel0");
							} catch (Exception e) {
								System.err.println("MeteoStation8, step 4 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+9, step4_Instant,
						owner -> {
							try {
								((MeteoStationTest)owner).publishWindDatas(ms9_winds, "channel0");
							} catch (Exception e) {
								System.err.println("MeteoStation9, step 4 wind datas publication failed with exception " + e);
							}
						}),
				// WindTurbine checks that it has received 270 wind datas.
				new TestStep(
						CLOCK_URI, WT_URI, step5_Instant,
						owner -> {
							try {
								System.out.println("STEP 5/11 (120s) : WindTurbine checks that it has received 270 wind datas");
								((WindTurbineTest)owner).verifyNbMessagesReceived(270);
							} catch (Exception e) {
								System.err.println("Step 5 wind datas account check failed with exception " + e);
							}
						}),
				// MeteoStation4 (STANDARD) creates 'testChannel0' + MeteoStation7 (PREMIUM) creates 'testChannel1'.
				new TestStep(
						CLOCK_URI, MS_URI+4, step6_Instant,
						owner -> {
							try {
								System.out.println("STEP 6/11 (130s) : MeteoStation4 (STANDARD) creates 'testChannel0' + MeteoStation7 (PREMIUM) creates 'testChannel1'");
								((MeteoStationTest)owner).createNewChannel("testChannel0", ".*");
							} catch (Exception e) {
								System.err.println("Step 6 'testChannel0' creation failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+7, step6_Instant,
						owner -> {
							try {
								((MeteoStationTest)owner).createNewChannel("testChannel1", ".*");
							} catch (Exception e) {
								System.err.println("Step 6 'testChannel1' creation failed with exception " + e);
							}
						}),
				// WindTurbine subscribes to channels 'testChannel0' and 'testChannel1'.
				new TestStep(
						CLOCK_URI, WT_URI, step7_Instant,
						owner -> {
							try {
								System.out.println("STEP 7/11 (140s) : WindTurbine subscribes to channels 'testChannel0' and 'testChannel1'");
								((WindTurbineTest)owner).subscibeToChannel("testChannel0", windTurbineWindFilter);
								((WindTurbineTest)owner).subscibeToChannel("testChannel1", windTurbineWindFilter);
							} catch (Exception e) {
								System.err.println("Step 7 WindTurbine subscrition failed with exception " + e);
							}
						}),
				// all 3 STANDARD MeteoStations each send 30 wind datas to 'testChannel0'.
				new TestStep(
						CLOCK_URI, MS_URI+4, step8_Instant,
						owner -> {
							try {
								System.out.println("STEP 8/11 (150s) : all 3 STANDARD MeteoStations each send 29 wind datas to 'testChannel0'");
								((MeteoStationTest)owner).publishWindDatas(ms4_winds.subList(0, ms4_winds.size() - 1), "testChannel0");
							} catch (Exception e) {
								System.err.println("MeteoStation4, step 8 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+5, step8_Instant,
						owner -> {
							try {
								((MeteoStationTest)owner).publishWindDatas(ms5_winds.subList(0, ms5_winds.size() - 1), "testChannel0");
							} catch (Exception e) {
								System.err.println("MeteoStation5, step 8 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+6, step8_Instant,
						owner -> {
							try {
								((MeteoStationTest)owner).publishWindDatas(ms6_winds.subList(0, ms6_winds.size() - 1), "testChannel0");
							} catch (Exception e) {
								System.err.println("MeteoStation6, step 8 wind datas publication failed with exception " + e);
							}
						}),
				// WindTurbine checks that it has received 357 wind datas.
				new TestStep(
						CLOCK_URI, WT_URI, step9_Instant,
						owner -> {
							try {
								System.out.println("STEP 9/11 (170s) : WindTurbine checks that it has received 357 wind datas");
								((WindTurbineTest)owner).verifyNbMessagesReceived(357);
							} catch (Exception e) {
								System.err.println("Step 9 wind datas account check failed with exception " + e);
							}
						}),
				// all 3 STANDARD MeteoStations each send 30 wind datas to 'testChannel1'.
				new TestStep(
						CLOCK_URI, MS_URI+4, step10_Instant,
						owner -> {
							try {
								System.out.println("STEP 10/11 (190s) : all 3 STANDARD MeteoStations each send 29 wind datas to 'testChannel1'");
								((MeteoStationTest)owner).publishWindDatas(ms4_winds.subList(0, ms4_winds.size() - 1), "testChannel1");
							} catch (Exception e) {
								System.err.println("MeteoStation4, step 10 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+5, step10_Instant,
						owner -> {
							try {
								((MeteoStationTest)owner).publishWindDatas(ms5_winds.subList(0, ms5_winds.size() - 1), "testChannel1");
							} catch (Exception e) {
								System.err.println("MeteoStation5, step 10 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI+6, step10_Instant,
						owner -> {
							try {
								((MeteoStationTest)owner).publishWindDatas(ms6_winds.subList(0, ms6_winds.size() - 1), "testChannel1");
							} catch (Exception e) {
								System.err.println("MeteoStation6, step 10 wind datas publication failed with exception " + e);
							}
						}),
				// WindTurbine checks that it has received 444 wind datas.
				new TestStep(
						CLOCK_URI, WT_URI, step11_Instant,
						owner -> {
							try {
								System.out.println("STEP 11/11 (210s) : WindTurbine checks that it has received 444 wind datas");
								((WindTurbineTest)owner).verifyNbMessagesReceived(444);
							} catch (Exception e) {
								System.err.println("Step 11 wind datas account check failed with exception " + e);
							}
						})
				});
	}

	public CVMConcurrentTest() throws Exception{
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
				new Object[] {2, nbMaxChannelsSTD, nbMaxChannelsPREM, brokerNbThreads, 0, 
						brokerNbFreeLoadersThreads, brokerNbUnloadersThreads, brokerNbSendersThreads});
		assert this.isDeployedComponent(broker_URI);
		this.toggleTracing(broker_URI);
		
		// Creation of 9 MeteoStations
		for (int id=1; id<10; id++) {
			
			RegistrationClass rc = id <= 3 ? RegistrationClass.FREE : (id <= 6 ? RegistrationClass.STANDARD : RegistrationClass.PREMIUM);
			PositionData pos = id <= 3 ? FREEstationPosition : (id <= 6 ? STANDARDstationPosition : PREMIUMstationPosition);
			
			String meteoStation_URI_verif = AbstractComponent.createComponent(
					MeteoStationTest.class.getCanonicalName(),
					new Object[] { MSReceivingInboundPortURI+id, MSRegistrationOutboundPortURI+id, MSPublishingOutboundPortURI+id, MSPrivilegedOutboundPortURI+id,
							rc, stationNbThreads, 1, pos, MS_URI+id, testScenario, id});
			assert (MS_URI+id).equals(meteoStation_URI_verif);
			assert this.isDeployedComponent(MS_URI+id);
			this.toggleTracing(MS_URI+id);
		}
		
		// Creation of 1 WindTurbine
		String tmp_windTurbine_URI = AbstractComponent.createComponent(WindTurbineTest.class.getCanonicalName(),
				new Object[] { WindTurbineReceivingInboundPortURI, WindTurbineRegistrationOutboundPortURI, WindTurbinePublishingOutboundPortURI,
						RegistrationClass.STANDARD, 1, 1, windTurbinePosition, MeteoAlertI.Level.ORANGE, WT_URI, testScenario, 1, windTurbineWindFilter, windTurbineAlertFilter});
		this.toggleTracing(tmp_windTurbine_URI);
		
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
			
			CVMConcurrentTest cvm = new CVMConcurrentTest() ;
			cvm.startStandardLifeCycle(EXECUTION_DURATION);
			Thread.sleep(END_SLEEP_DURATION);
			System.exit(0) ;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
