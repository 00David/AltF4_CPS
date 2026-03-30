package fr.sorbonne_u.meteo.tests.audit2;

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
 *     <li>1 {@link WindTurbineTest} accepting everything, with 4 threads</li>
 *     <li>2 STANDARD {@link MeteoStationTest}</li>
 * </ul>
 * 
 * <p>
 * 	Asynchronous error management, while publishing on a channel :
 * </p>
 * <ul>
 * <li> - After 5 seconds, <strong>MeteoStation1 (STANDARD)</strong> creates 'testChannel0' and 'testChannel1'.</li>
 * <li> - After 10 seconds, <strong>WindTurbine</strong> subscribes to 'testChannel0' and 'testChannel1'</li>
 * <li> - After 20 seconds, <strong>MeteoStation1</strong> attempts to publish a message with a notification to a non-existing channel, 'testChannel7'.</li>
 * <li> - After 40 seconds, <strong>MeteoStation1</strong> sends {@code NBTOOMUCHWINDS} wind datas (20000 by default) to 'testChannel0'.</li>
 * <li> - After 41 seconds, <strong>MeteoStation2</strong> destroys 'testChannel0', interrupting publishing.</li>
 * <li> - After 50 seconds, <strong>WindTurbine</strong> prints its current number of wind datas (impossible to predict).</li>
 * <li> - After 70 seconds, <strong>MeteoStation1</strong> sends {@code NBTOOMUCHWINDS} wind datas (20000 by default) to 'testChannel1'.</li>
 * <li> - After 71 seconds, <strong>MeteoStation2</strong> destroys NOW 'testChannel1', interrupting publishing.</li>
 * <li> - After 80 seconds, <strong>WindTurbine</strong> prints its current number of wind datas (impossible to predict).</li>
 * </ul>
 * 
 * <p>
 * 	{@code NBTOOMUCHWINDS} has been chosen in order to have a sufficient number of wind datas that will make Broker loader/unloader 
 * 	threads interrupted by the channel destruction, <strong>if there is no "Unloader error" appearing on the console, you can increase it</strong>.
 * </p>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class CVMAbnormalConcurrentPublicationsTest extends AbstractCVM {
	
	// -------------------------------------------------------------------------
	// Test constants and variables
	// -------------------------------------------------------------------------

	public static String CLOCK_URI = "test-clock";
	public static String START_INSTANT = "2026-01-30T09:00:00.00Z";
	public static String END_INSTANT = "2026-01-30T09:01:30.00Z";

	public static long DELAY_TO_START = 1000L;
	public static long END_SLEEP_DURATION = 20000L;
	public static double ACCELERATION_FACTOR = 2.0;
	public static long EXECUTION_DURATION =
		(long) (DELAY_TO_START + 1000L + ((Instant.parse(END_INSTANT).toEpochMilli() - Instant.parse(START_INSTANT).toEpochMilli())
														/ACCELERATION_FACTOR));
	
	// Delay after the start when steps be executed, in seconds
	public static long[] STEPS = new long[] {5, 10, 20, 40, 41, 50, 70, 71, 80};
	public static final int NBSTEPS = STEPS.length;
	
	public static final int NBTOOMUCHWINDS = 20000;
	
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
	protected static final PositionData stationPosition1 = new PositionData(1, 2);
	protected static final PositionData stationPosition2 = new PositionData(2, 2);
	
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
		
		// Wind datas
		List<WindData> ms1_too_much_winds = new ArrayList<>();
		WindData wd = new WindData(stationPosition1, 2, 2);
		for (int k=0; k<NBTOOMUCHWINDS; k++) ms1_too_much_winds.add(wd);
		
		return new TestScenario(
			CLOCK_URI,
			startInstant,
			endInstant,
			new TestStepI[] {
				// MeteoStation1 (STANDARD) creates 'testChannel0' and 'testChannel1'.
				new TestStep(
						CLOCK_URI, MS_URI+1, step_Instants[0],
						owner -> {
							try {
								System.out.println("STEP 0/"+(NBSTEPS-1)+" ("+STEPS[0]+"s) : MeteoStation1 (STANDARD) creates 'testChannel0' and 'testChannel1'");
								((MeteoStationTest)owner).createNewChannel("testChannel0", ".*");
								((MeteoStationTest)owner).createNewChannel("testChannel1", ".*");
							} catch (Exception e) {
								System.err.println("Step 0 'testChannel0' and 'testChannel1' creation failed with exception " + e);
							}
						}),
				// WindTurbine subscribes to channels 'testChannel0' and 'testChannel1'.
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[1],
						owner -> {
							try {
								System.out.println("STEP 1/"+(NBSTEPS-1)+" ("+STEPS[1]+"s) : WindTurbine subscribes to channels 'testChannel0' and 'testChannel1'");
								((WindTurbineTest)owner).subscibeToChannel("testChannel0", windTurbineWindFilter);
								((WindTurbineTest)owner).subscibeToChannel("testChannel1", windTurbineWindFilter);
							} catch (Exception e) {
								System.err.println("Step 1 WindTurbine subscrition failed with exception " + e);
							}
						}),
				// MeteoStation1 attempts to publish a message with a notification to a non-existing channel, 'testChannel7'.
				new TestStep(
						CLOCK_URI, MS_URI+1, step_Instants[2],
						owner -> {
							try {
								System.out.println("STEP 2/"+(NBSTEPS-1)+" ("+STEPS[2]+"s) : MeteoStation1 attempts to publish a message with a notification to a non-existing channel, 'testChannel7'");
								((MeteoStationTest)owner).publishWindData(2, 2, "testChannel7", true);
							} catch (Exception e) {
								System.err.println("Step 2 wind data publishing failed with exception " + e);
							}
						}),
				// MeteoStation1 sends 10000 wind datas to 'testChannel0'
				new TestStep(
						CLOCK_URI, MS_URI+1, step_Instants[3],
						owner -> {
							try {
								System.out.println("STEP 3/"+(NBSTEPS-1)+" ("+STEPS[3]+"s) : MeteoStation1 sends "+NBTOOMUCHWINDS+" wind datas to 'testChannel0'");
								((MeteoStationTest)owner).publishWindDatas(ms1_too_much_winds, "testChannel0", false);
							} catch (Exception e) {
								System.err.println("Step 3 wind data publishing failed with exception " + e);
							}
						}),
				// MeteoStation2 almost simultaneously destroys 'testChannel0', interrupting publishing
				new TestStep(
						CLOCK_URI, MS_URI+2, step_Instants[4],
						owner -> {
							try {
								System.out.println("STEP 4/"+(NBSTEPS-1)+" ("+STEPS[4]+"s) : MeteoStation2 almost simultaneously destroys 'testChannel0', interrupting publishing");
								((MeteoStationTest)owner).destroyChannelAndVerifies("testChannel0", false);
							} catch (Exception e) {
								System.err.println("Step 4 channel destruction failed with exception " + e);
							}
						}),
				// WindTurbine prints its current number of wind datas (impossible to predict).
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[5],
						owner -> {
							try {
								System.out.println("STEP 5/"+(NBSTEPS-1)+" ("+STEPS[5]+"s) : we print WindTurbine current number of wind datas (impossible to predict)");
								int nbWTdatas = ((WindTurbineTest)owner).getNbWindDatasReceived();
								System.out.println("WindTurbine has "+nbWTdatas+" wind datas, got everything since start ? "+(nbWTdatas == NBTOOMUCHWINDS));
							} catch (Exception e) {
								System.err.println("Step 5 wind datas number check failed with exception " + e);
							}
						}),
				// MeteoStation1 sends 10000 wind datas to 'testChannel1'
				new TestStep(
						CLOCK_URI, MS_URI+1, step_Instants[6],
						owner -> {
							try {
								System.out.println("STEP 6/"+(NBSTEPS-1)+" ("+STEPS[6]+"s) : MeteoStation1 sends "+NBTOOMUCHWINDS+" wind datas to 'testChannel1'");
								((MeteoStationTest)owner).publishWindDatas(ms1_too_much_winds, "testChannel1", false);
							} catch (Exception e) {
								System.err.println("Step 6 wind data publishing failed with exception " + e);
							}
						}),
				// MeteoStation2 almost simultaneously destroys 'testChannel1', interrupting publishing
				new TestStep(
						CLOCK_URI, MS_URI+2, step_Instants[7],
						owner -> {
							try {
								System.out.println("STEP 7/"+(NBSTEPS-1)+" ("+STEPS[7]+"s) : MeteoStation2 almost simultaneously destroys NOW 'testChannel1', interrupting publishing");
								((MeteoStationTest)owner).destroyChannelAndVerifies("testChannel1", true);
							} catch (Exception e) {
								System.err.println("Step 7 channel destruction failed with exception " + e);
							}
						}),
				// WindTurbine prints its current number of wind datas (impossible to predict).
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[6],
						owner -> {
							try {
								System.out.println("STEP 8/"+(NBSTEPS-1)+" ("+STEPS[8]+"s) : we print WindTurbine current number of wind datas (impossible to predict)");
								int nbWTdatas = ((WindTurbineTest)owner).getNbWindDatasReceived();
								System.out.println("WindTurbine has "+nbWTdatas+" wind datas, got everything since start ? "+(nbWTdatas == (2*NBTOOMUCHWINDS)));
							} catch (Exception e) {
								System.err.println("Step 8 wind datas number check failed with exception " + e);
							}
						})
				});
	}

	public CVMAbnormalConcurrentPublicationsTest() throws Exception{
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
		
		// Creation of 2 MeteoStations
		String meteoStation1_URI_verif = AbstractComponent.createComponent(
				MeteoStationTest.class.getCanonicalName(),
				new Object[] { MSReceivingInboundPortURI+1, MSRegistrationOutboundPortURI+1, MSPublishingOutboundPortURI+1, MSPrivilegedOutboundPortURI+1,
						RegistrationClass.STANDARD, stationNbThreads, 1, stationPosition1, MS_URI+1, testScenario, 1});
		assert (MS_URI+1).equals(meteoStation1_URI_verif);
		assert this.isDeployedComponent(MS_URI+1);
		this.toggleTracing(MS_URI+1);
		
		String meteoStation2_URI_verif = AbstractComponent.createComponent(
				MeteoStationTest.class.getCanonicalName(),
				new Object[] { MSReceivingInboundPortURI+2, MSRegistrationOutboundPortURI+2, MSPublishingOutboundPortURI+2, MSPrivilegedOutboundPortURI+2,
						RegistrationClass.STANDARD, stationNbThreads, 1, stationPosition1, MS_URI+2, testScenario, 2});
		assert (MS_URI+2).equals(meteoStation2_URI_verif);
		assert this.isDeployedComponent(MS_URI+2);
		this.toggleTracing(MS_URI+2);
		
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
			
			CVMAbnormalConcurrentPublicationsTest cvm = new CVMAbnormalConcurrentPublicationsTest() ;
			cvm.startStandardLifeCycle(EXECUTION_DURATION);
			Thread.sleep(END_SLEEP_DURATION);
			System.exit(0) ;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
