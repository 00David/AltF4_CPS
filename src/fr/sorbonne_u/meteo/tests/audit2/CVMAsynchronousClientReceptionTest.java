package fr.sorbonne_u.meteo.tests.audit2;

import java.time.Duration;
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
 * 	<li> - After 150 seconds, <strong>all 3 STANDARD MeteoStations</strong> each send 29 wind datas to 'testChannel0' (created by a STANDARD).</li>
 * 	<li> - After 170 seconds, <strong>WindTurbine</strong> checks that it has received a total of 357 wind datas.</li>
 * 	<li> - After 190 seconds, <strong>all 3 STANDARD MeteoStations</strong> each send 29 wind datas to 'testChannel1' (created by a PREMIUM).</li>
 * 	<li> - After 210 seconds, <strong>WindTurbine</strong> checks that it has received a total of 444 wind datas.</li>
 * </ul>
 * 
 * <p>
 * 	THIRD PART : asynchronous error management
 * </p>
 * <ul>
 * 	<li> - After 230 seconds, <strong>MeteoStation1</strong> tries publishing with a notification a message on a non existing channel, 'testChannel7'.</li>
 * </ul>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class CVMAsynchronousClientReceptionTest extends AbstractCVM {
	
	// -------------------------------------------------------------------------
	// Test constants and variables
	// -------------------------------------------------------------------------

	public static String CLOCK_URI = "test-clock";
	public static String START_INSTANT = "2026-01-30T09:00:00.00Z";
	public static String END_INSTANT = "2026-01-30T09:04:00.00Z";

	public static long DELAY_TO_START = 1000L;
	public static long END_SLEEP_DURATION = 10000L;
	public static double ACCELERATION_FACTOR = 2.0;
	public static long EXECUTION_DURATION =
		(long) (DELAY_TO_START + 1000L + ((Instant.parse(END_INSTANT).toEpochMilli() - Instant.parse(START_INSTANT).toEpochMilli())
														/ACCELERATION_FACTOR));
	
	// Delay after the start when steps be executed, in seconds
	public static long[] STEPS = new long[] {20, 40, 60, 80, 100, 120, 140, 160};
	public static final int NBSTEPS = STEPS.length;
	
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
	
	protected static final int stationNbThreads = 4; // useless, meteo stations does not receive
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
		Instant[] step_Instants = new Instant[NBSTEPS];
		for (int i=0; i<NBSTEPS; i++) step_Instants[i] = startInstant.plusSeconds(STEPS[i]);
		
		List<WindData> ms1_winds = new ArrayList<>(); List<WindData> ms2_winds = new ArrayList<>(); List<WindData> ms3_winds = new ArrayList<>();
		for (int j=0; j<20; j++) {
			if (j<5) ms1_winds.add(new WindData(FREEstationPosition, j, j));
			else if (j<10) ms2_winds.add(new WindData(FREEstationPosition, j, j));
			else ms3_winds.add(new WindData(FREEstationPosition, j, j));
		}
		
		List<WindData> ms4_winds = new ArrayList<>(); List<WindData> ms5_winds = new ArrayList<>(); List<WindData> ms6_winds = new ArrayList<>();
		for (int j=0; j<20; j++) {
			if (j<5) ms4_winds.add(new WindData(STANDARDstationPosition, j, j));
			else if (j<10) ms5_winds.add(new WindData(STANDARDstationPosition, j, j));
			else ms6_winds.add(new WindData(STANDARDstationPosition, j, j));
		}
		
		List<WindData> ms7_winds = new ArrayList<>(); List<WindData> ms8_winds = new ArrayList<>(); List<WindData> ms9_winds = new ArrayList<>();
		for (int j=0; j<20; j++) {
			if (j<5) ms7_winds.add(new WindData(PREMIUMstationPosition, j, j));
			else if (j<10) ms8_winds.add(new WindData(PREMIUMstationPosition, j, j));
			else ms9_winds.add(new WindData(PREMIUMstationPosition, j, j));
		}

		
		return new TestScenario(
			CLOCK_URI,
			startInstant,
			endInstant,
			new TestStepI[] {
				
				new TestStep(
					CLOCK_URI, WT_URI, step_Instants[0],
					owner -> {
						try {
							System.out.println("STEP 0/"+(NBSTEPS-1)+" ("+STEPS[0]+"s) : WindTurbine gettingNextMessage message at 'channel0'");
							((WindTurbineTest)owner).gettingNextMessage("channel0");
						} catch (Exception e) {
							System.err.println("WindTurbine, step 0 wind failed with exception " + e);
						}
					}),
				new TestStep(
						CLOCK_URI, MS_URI, step_Instants[1],
						owner -> {
							try {
								System.out.println("STEP 1/"+(NBSTEPS-1)+" ("+STEPS[1]+"s) : MeteoStation publish message at 'channel0'");
								((MeteoStationTest)owner).publishWindData(2, 2, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation, step 1 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[2],
						owner -> {
							try {
								System.out.println("STEP 2/"+(NBSTEPS-1)+" ("+STEPS[2]+"s) : WindTurbine gettingNextMessageTime message at 'channel0'");
								Duration d = Duration.ofSeconds(30);
								((WindTurbineTest)owner).gettingNextMessageTime("channel0",d);
							} catch (Exception e) {
								System.err.println("WindTurbine, step 2 wind failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI, step_Instants[3],
						owner -> {
							try {
								System.out.println("STEP 3/"+(NBSTEPS-1)+" ("+STEPS[3]+"s) : MeteoStation publish message at 'channel0'");
								((MeteoStationTest)owner).publishWindData(2, 2, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation, step 3 wind failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[4],
						owner -> {
							try {
								System.out.println("STEP 4/"+(NBSTEPS-1)+" ("+STEPS[4]+"s) : WindTurbine gettingNextMessageFuture future message at 'channel0'");
								((WindTurbineTest)owner).gettingNextMessageFuture("channel0");
							} catch (Exception e) {
								System.err.println("WindTurbine, step 4 wind failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI, step_Instants[5],
						owner -> {
							try {
								System.out.println("STEP 5/"+(NBSTEPS-1)+" ("+STEPS[5]+"s) : MeteoStation publish message at 'channel0'");
								((MeteoStationTest)owner).publishWindData(2, 2, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation, step 5 wind failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[6],
						owner -> {
							try {
								System.out.println("STEP 6/"+(NBSTEPS-1)+" ("+STEPS[6]+"s) : WindTurbine gettingNextMessageTime message at 'channel0'");
								Duration d1 = Duration.ofSeconds(10);
								((WindTurbineTest)owner).gettingNextMessageTime("channel0",d1);
							} catch (Exception e) {
								System.err.println("WindTurbine, step 6 wind datas publication failed with exception " + e);
							}
						}),
				new TestStep(
						CLOCK_URI, MS_URI, step_Instants[7],
						owner -> {
							try {
								System.out.println("STEP 7/7 (160s) : MeteoStation publish message at 'channel0'");
								((MeteoStationTest)owner).publishWindData(2, 2, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation, step 7 wind failed with exception " + e);
							}
						}),
			});
				
						
					
				
	}

	public CVMAsynchronousClientReceptionTest() throws Exception{
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
		
//		// Creation of 9 MeteoStations
//		for (int id=1; id<10; id++) {
//			
//			RegistrationClass rc = id <= 3 ? RegistrationClass.FREE : (id <= 6 ? RegistrationClass.STANDARD : RegistrationClass.PREMIUM);
//			PositionData pos = id <= 3 ? FREEstationPosition : (id <= 6 ? STANDARDstationPosition : PREMIUMstationPosition);
//			
//			String meteoStation_URI_verif = AbstractComponent.createComponent(
//					MeteoStationTest.class.getCanonicalName(),
//					new Object[] { MSReceivingInboundPortURI+id, MSRegistrationOutboundPortURI+id, MSPublishingOutboundPortURI+id, MSPrivilegedOutboundPortURI+id,
//							rc, stationNbThreads, 1, pos, MS_URI+id, testScenario, id});
//			assert (MS_URI+id).equals(meteoStation_URI_verif);
//			assert this.isDeployedComponent(MS_URI+id);
//			this.toggleTracing(MS_URI+id);
//		}
		
		
		RegistrationClass rc = RegistrationClass.STANDARD;
		PositionData posD = STANDARDstationPosition;		String meteoStation_URI_verif = AbstractComponent.createComponent(
				MeteoStationTest.class.getCanonicalName(),
				new Object[] { MSReceivingInboundPortURI, MSRegistrationOutboundPortURI, MSPublishingOutboundPortURI, MSPrivilegedOutboundPortURI,
						rc, stationNbThreads, 1, posD, MS_URI, testScenario, 1});
		
		assert (MS_URI).equals(meteoStation_URI_verif);
		assert this.isDeployedComponent(MS_URI);
		this.toggleTracing(MS_URI);
		
		// Creation of 1 WindTurbine
		String tmp_windTurbine_URI = AbstractComponent.createComponent(WindTurbineTest.class.getCanonicalName(),
				new Object[] { WindTurbineReceivingInboundPortURI, WindTurbineRegistrationOutboundPortURI, WindTurbinePublishingOutboundPortURI,
						RegistrationClass.STANDARD, windTurbineNbThreads, 1, windTurbinePosition, MeteoAlertI.Level.ORANGE, WT_URI, testScenario, 1, windTurbineWindFilter, windTurbineAlertFilter});
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
			VerboseException.VERBOSE = false;
			VerboseException.PRINT_STACK_TRACE = false;
			
			CVMAsynchronousClientReceptionTest cvm = new CVMAsynchronousClientReceptionTest() ;
			cvm.startStandardLifeCycle(EXECUTION_DURATION);
			Thread.sleep(END_SLEEP_DURATION);
			System.exit(0) ;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}