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
 *     <li>2 STANDARD {@link MeteoStation}</li>
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
 * <li> - After 60 seconds, <strong>MeteoStation1</strong> sends {@code NBTOOMUCHWINDS} wind datas (20000 by default) to 'testChannel1'.</li>
 * <li> - After 61 seconds, <strong>MeteoStation2</strong> destroys NOW 'testChannel1', interrupting publishing.</li>
 * <li> - After 80 seconds, <strong>WindTurbine</strong> prints its current number of wind datas (impossible to predict).</li>
 * </ul>
 * 
 * <p>
 * 	{@code NBTOOMUCHWINDS} has been chosen in order to have a sufficient number of wind datas that will make Broker loader/unloader 
 * 	threads interrupted by <strong>both</strong> channel destructions, <strong>if there is no "UnknownChannelException" appearing on the console, you can increase it</strong>.
 * </p>
 * 
 * <p>
 * 	It is also possible that the number of WindTurbine wind datas received at the end is exactly {@code NBTOOMUCHWINDS} :
 * 	this case happens when the first sending has been completely sent, and the second channel destruction (now) happens before any
 * 	unloader has unloaded anything from the channel.<br>
 * 	(If the first channel destruction (normal) also happens before any unloader has unloaded anything from the channel, 
 * 	then all channel datas are unloaded and sent by the Broker thread destroying the channel)
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
	public static long[] STEPS = new long[] {5, 10, 20, 40, 41, 60, 61, 80};
	public static final int NBSTEPS = STEPS.length;
	
	public static final int NBTOOMUCHWINDS = 20000;
	
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
	
	// MeteoStation 1
	protected final String MS1ReceivingInboundPortURI = "inbound-port-client-MS1";
	protected final static PositionData stationPosition1 = new PositionData(1, 2);
	
	// MeteoStation 2
	protected final String MS2ReceivingInboundPortURI = "inbound-port-client-MS2";
	protected final PositionData stationPosition2 = new PositionData(2, 2);
	
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
	protected static String MS1_URI = "MeteoStation1-URI";
	protected static String MS2_URI = "MeteoStation2-URI";
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
						CLOCK_URI, MS1_URI, step_Instants[0],
						owner -> {
							try {
								System.out.println("STEP 0/"+(NBSTEPS-1)+" ("+STEPS[0]+"s) : MeteoStation1 (STANDARD) creates 'testChannel0' and 'testChannel1'");
								((MeteoStation)owner).createNewChannel("testChannel0", ".*");
								((MeteoStation)owner).createNewChannel("testChannel1", ".*");
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
								((WindTurbine)owner).subscribeToChannel("testChannel0", windTurbineWindFilter);
								((WindTurbine)owner).subscribeToChannel("testChannel1", windTurbineWindFilter);
							} catch (Exception e) {
								System.err.println("Step 1 WindTurbine subscrition failed with exception " + e);
							}
						}),
				// MeteoStation1 attempts to publish a message with a notification to a non-existing channel, 'testChannel7'.
				new TestStep(
						CLOCK_URI, MS1_URI, step_Instants[2],
						owner -> {
							try {
								System.out.println("STEP 2/"+(NBSTEPS-1)+" ("+STEPS[2]+"s) : MeteoStation1 attempts to publish a message with a notification to a non-existing channel, 'testChannel7'");
								((MeteoStation)owner).publishWindData(2, 2, "testChannel7", true);
							} catch (Exception e) {
								System.err.println("Step 2 wind data publishing failed with exception " + e);
							}
						}),
				// MeteoStation1 sends 20000 wind datas to 'testChannel0'
				new TestStep(
						CLOCK_URI, MS1_URI, step_Instants[3],
						owner -> {
							try {
								System.out.println("STEP 3/"+(NBSTEPS-1)+" ("+STEPS[3]+"s) : MeteoStation1 sends "+NBTOOMUCHWINDS+" wind datas to 'testChannel0'");
								((MeteoStation)owner).publishWindDatas(ms1_too_much_winds, "testChannel0", false);
							} catch (Exception e) {
								System.err.println("Step 3 wind data publishing failed with exception " + e);
							}
						}),
				// MeteoStation2 almost simultaneously destroys 'testChannel0', interrupting publishing
				new TestStep(
						CLOCK_URI, MS2_URI, step_Instants[4],
						owner -> {
							try {
								System.out.println("STEP 4/"+(NBSTEPS-1)+" ("+STEPS[4]+"s) : MeteoStation2 almost simultaneously destroys 'testChannel0', interrupting publishing");
								((MeteoStation)owner).destroyChannelAndVerifies("testChannel0", false);
							} catch (Exception e) {
								System.err.println("Step 4 channel destruction failed with exception " + e);
							}
						}),
				// MeteoStation1 sends 20000 wind datas to 'testChannel1'
				new TestStep(
						CLOCK_URI, MS1_URI, step_Instants[5],
						owner -> {
							try {
								System.out.println("STEP 5/"+(NBSTEPS-1)+" ("+STEPS[5]+"s) : MeteoStation1 sends "+NBTOOMUCHWINDS+" wind datas to 'testChannel1'");
								((MeteoStation)owner).publishWindDatas(ms1_too_much_winds, "testChannel1", false);
							} catch (Exception e) {
								System.err.println("Step 5 wind data publishing failed with exception " + e);
							}
						}),
				// MeteoStation2 almost simultaneously destroys 'testChannel1', interrupting publishing
				new TestStep(
						CLOCK_URI, MS2_URI, step_Instants[6],
						owner -> {
							try {
								System.out.println("STEP 6/"+(NBSTEPS-1)+" ("+STEPS[6]+"s) : MeteoStation2 almost simultaneously destroys NOW 'testChannel1', interrupting publishing");
								((MeteoStation)owner).destroyChannelAndVerifies("testChannel1", true);
							} catch (Exception e) {
								System.err.println("Step 6 channel destruction failed with exception " + e);
							}
						}),
				// WindTurbine prints its current number of wind datas (impossible to predict).
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[7],
						owner -> {
							try {
								System.out.println("STEP 7/"+(NBSTEPS-1)+" ("+STEPS[7]+"s) : we print WindTurbine current number of wind datas (impossible to predict)");
								int nbWTdatas = ((WindTurbine)owner).getNbWindDatasReceived();
								System.out.println("WindTurbine has "+nbWTdatas+" wind datas,\n"
										+ "got everything for the first sending ? "+(nbWTdatas >= NBTOOMUCHWINDS)+"\n"
										+ "got everything whith the second sending ? "+(nbWTdatas == 2*NBTOOMUCHWINDS));
							} catch (Exception e) {
								System.err.println("Step 7 wind datas number check failed with exception " + e);
							}
						}),
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
		String broker_URI = AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] {1, new PositionData(0, 4), nbDefaultChannels, nbMaxChannelsStd, nbMaxChannelsPrem, brokerNbThreads, 
						brokerNbSchedulableThreads, brokerNbFreeLoadersThreads, brokerNbUnloadersThreads, brokerNbSendersThreads, 
						brokerNbGossipThreads, "BrokerGossipInboundPortURI", new ArrayList<>(), new ArrayList<>()}); // Gossip protocol unused here
		assert this.isDeployedComponent(broker_URI);
		this.toggleTracing(broker_URI);
		
		// Creation of 2 MeteoStations
		MS1_URI = AbstractComponent.createComponent(
				MeteoStation.class.getCanonicalName(),
				new Object[] {1, MS1_URI, MS1ReceivingInboundPortURI, RegistrationClass.STANDARD, clientNbThreads, clientNbSchedulableThreads, 
						stationPosition1, testScenario});
		assert this.isDeployedComponent(MS1_URI);
		this.toggleTracing(MS1_URI);
		
		MS2_URI = AbstractComponent.createComponent(
				MeteoStation.class.getCanonicalName(),
				new Object[] {2, MS2_URI, MS2ReceivingInboundPortURI, RegistrationClass.STANDARD, clientNbThreads, clientNbSchedulableThreads, 
						stationPosition2, testScenario});
		assert this.isDeployedComponent(MS2_URI);
		this.toggleTracing(MS2_URI);
		
		// Creation of 1 WindTurbine
		WT_URI = AbstractComponent.createComponent(WindTurbine.class.getCanonicalName(),
				new Object[] {1, WT_URI, WindTurbineReceivingInboundPortURI, RegistrationClass.STANDARD, clientNbThreads, clientNbSchedulableThreads, 
						windTurbinePosition, MeteoAlertI.Level.ORANGE, true, windTurbineWindFilter, windTurbineAlertFilter, testScenario});
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
			
			CVMAbnormalConcurrentPublicationsTest cvm = new CVMAbnormalConcurrentPublicationsTest() ;
			cvm.startStandardLifeCycle(EXECUTION_DURATION);
			Thread.sleep(END_SLEEP_DURATION);
			System.exit(0) ;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
