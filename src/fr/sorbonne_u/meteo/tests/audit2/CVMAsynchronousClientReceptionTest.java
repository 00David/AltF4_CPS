package fr.sorbonne_u.meteo.tests.audit2;

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
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.MessageFilterI.TimeFilterI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI.RegistrationClass;
import fr.sorbonne_u.exceptions.VerboseException;
import fr.sorbonne_u.filters.MessageFilter;
import fr.sorbonne_u.filters.property.PropertiesFilter;
import fr.sorbonne_u.filters.property.PropertyFilter;
import fr.sorbonne_u.filters.time.JokerTimeFilter;
import fr.sorbonne_u.meteo.data.PositionData;
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
 *     <li>1 STANDARD {@link MeteoStationTest}</li>
 * </ul>
 * 
 * <p>
 * 	Asynchronous client reception methods testing :
 * </p>
 * <ul>
 * 	<li> - After 20 seconds, <strong>WindTurbine</strong> waits a message from 'channel0'.</li>
 * 	<li> - After 40 seconds, <strong>MeteoStation</strong> publishes a message on 'channel0'.</li>
 * 	<li> - After 60 seconds, <strong>WindTurbine</strong> waits a message from 'channel0' with a 
 * 			timeout ending after the next message reception (gets the message).</li>
 * 	<li> - After 80 seconds, <strong>MeteoStation</strong> publishes a message on 'channel0'.</li>
 * 	<li> - After 100 seconds, <strong>WindTurbine</strong> waits a message from 'channel0' with a 
 * 			timeout ending without a message reception.</li>
 * 	<li> - After 120 seconds, <strong>WindTurbine</strong> gets a message future for 'channel0', 
 * 			sleeps and gets the result (blocking) before the next reception.</li>
 * 	<li> - After 140 seconds, <strong>MeteoStation</strong> publishes a message on 'channel0'.</li>
 * 	<li> - After 160 seconds, <strong>WindTurbine</strong> gets a message future for 'channel0', 
 * 			sleeps and gets the result after the next reception.</li>
 * 	<li> - After 180 seconds, <strong>MeteoStation</strong> publishes a message on 'channel0'.</li>
 * 	<li> - After 200 seconds, <strong>WindTurbine</strong> waits a message from 'channel0.</li>
 * 	<li> - After 220 seconds, <strong>WindTurbine</strong> gets a message future for 'channel0', 
 * 			sleeps 0 seconds and blocks on getting the result.</li>
 * 	<li> - After 240 seconds, <strong>MeteoStation</strong> publishes 3 messages on 'channel0', 
 * 			each got by each possible reception method.</li>
 * </ul>
 * 
 * <p>
 * 	<strong>For each WindTurbine reception wait, it is done by a classic wind turbine thread</strong> (the test schedulable thread delegates it). 
 * </p>
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
	public static String END_INSTANT = "2026-01-30T09:04:10.00Z";

	public static long DELAY_TO_START = 1000L;
	public static long END_SLEEP_DURATION = 20000L;
	public static double ACCELERATION_FACTOR = 2.0;
	public static long EXECUTION_DURATION =
		(long) (DELAY_TO_START + 1000L + ((Instant.parse(END_INSTANT).toEpochMilli() - Instant.parse(START_INSTANT).toEpochMilli())
														/ACCELERATION_FACTOR));
	
	// Delay after the start when steps be executed, in seconds
	public static long[] STEPS = new long[] {20, 40, 60, 80, 100, 120, 140, 160, 180, 200, 220, 240};
	public static final int NBSTEPS = STEPS.length;
	
	// -------------------------------------------------------------------------
	// Component datas
	// -------------------------------------------------------------------------
	
	// Broker
	protected static final int brokerNbThreads = 4;
	protected static final int brokerNbFreeLoadersThreads = 2; // Broker's constructor automatically scales at 4 and 8 threads for Standard and Premium senders.
	protected static final int brokerNbUnloadersThreads = 4;
	protected static final int brokerNbSendersThreads = 4;
	
	// MeteoStation URI
	protected final static String MS_URI = "MeteoStation-URI";
	protected final String MSReceivingInboundPortURI = "inbound-port-client-MS";
	
	protected static final int stationNbThreads = 4; // useless, meteo stations does not receive
	protected static final PositionData stationPosition = new PositionData(1, 2);
	
	// WindTurbine
	protected final static String WT_URI = "WindTurbine-URI";
	protected final String WindTurbineReceivingInboundPortURI = "inbound-port-client-WindTurbine";
	
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
		
		return new TestScenario(
			CLOCK_URI,
			startInstant,
			endInstant,
			new TestStepI[] {
				// WindTurbine waits a message from 'channel0'
				new TestStep(
					CLOCK_URI, WT_URI, step_Instants[0],
					owner -> {
						try {
							System.out.println("STEP 0/"+(NBSTEPS-1)+" ("+STEPS[0]+"s) : WindTurbine waits a message from 'channel0'");
							owner.runTask(c -> ((WindTurbineTest)owner).waitMessage("channel0"));
						} catch (Exception e) {
							System.err.println("WindTurbine, step 0 message waiting with exception " + e);
						}
					}),
				// MeteoStation publishes a message on 'channel0'
				new TestStep(
						CLOCK_URI, MS_URI, step_Instants[1],
						owner -> {
							try {
								System.out.println("STEP 1/"+(NBSTEPS-1)+" ("+STEPS[1]+"s) : MeteoStation publishes a message on 'channel0'");
								((MeteoStationTest)owner).publishWindData(2, 2, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation, step 1 wind data publication failed with exception " + e);
							}
						}),
				// WindTurbine waits a message from 'channel0' with a timeout ending after the next message reception (gets the message)
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[2],
						owner -> {
							try {
								double tmp = 20/ACCELERATION_FACTOR;
								Duration d = Duration.ofSeconds((long)(tmp+tmp/2));
								System.out.println("STEP 2/"+(NBSTEPS-1)+" ("+STEPS[2]+"s) : WindTurbine waits a message from 'channel0',"
										+ "\nwith a timeout of "+(d.getSeconds()*ACCELERATION_FACTOR)+" seconds, ending after the next message reception");
								owner.runTask(c -> ((WindTurbineTest)owner).waitMessageTimeout("channel0",d));
							} catch (Exception e) {
								System.err.println("WindTurbine, step 2 message waiting (timeout) with exception " + e);
							}
						}),
				// MeteoStation publishes a message on 'channel0'
				new TestStep(
						CLOCK_URI, MS_URI, step_Instants[3],
						owner -> {
							try {
								System.out.println("STEP 3/"+(NBSTEPS-1)+" ("+STEPS[3]+"s) : MeteoStation publishes a message on 'channel0'");
								((MeteoStationTest)owner).publishWindData(2, 2, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation, step 3 wind data publication failed with exception " + e);
							}
						}),
				// WindTurbine waits a message from 'channel0' with a timeout ending without a message reception
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[4],
						owner -> {
							try {
								double tmp = 20/ACCELERATION_FACTOR;
								Duration d = Duration.ofSeconds((long)(tmp-tmp/2));
								System.out.println("STEP 4/"+(NBSTEPS-1)+" ("+STEPS[4]+"s) : WindTurbine waits a message from 'channel0',"
										+ "\nwith a timeout of "+(d.getSeconds()*ACCELERATION_FACTOR)+" seconds, ending without a message reception");
								owner.runTask(c -> ((WindTurbineTest)owner).waitMessageTimeout("channel0",d));
							} catch (Exception e) {
								System.err.println("WindTurbine, step 4 message waiting (timeout) with exception " + e);
							}
						}),
				// WindTurbine gets a message future for 'channel0', sleeps and gets the result (blocking) before the next reception
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[5],
						owner -> {
							try {
								double tmp = 20/ACCELERATION_FACTOR;
								Duration d = Duration.ofSeconds((long)(tmp/2));
								System.out.println("STEP 5/"+(NBSTEPS-1)+" ("+STEPS[5]+"s) : WindTurbine gets a message future for 'channel0',"
										+ "\nsleeps for "+(d.getSeconds()*ACCELERATION_FACTOR)+" seconds and gets the result (blocking) before the next reception");
								owner.runTask(c -> ((WindTurbineTest)owner).waitMessageFuture("channel0",d));
							} catch (Exception e) {
								System.err.println("WindTurbine, step 5 message waiting (future) with exception " + e);
							}
						}),
				// MeteoStation publishes a message on 'channel0'
				new TestStep(
						CLOCK_URI, MS_URI, step_Instants[6],
						owner -> {
							try {
								System.out.println("STEP 6/"+(NBSTEPS-1)+" ("+STEPS[6]+"s) : MeteoStation publishes a message on 'channel0'");
								((MeteoStationTest)owner).publishWindData(2, 2, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation, step 6 wind data publication failed with exception " + e);
							}
						}),
				// WindTurbine gets a message future for 'channel0', sleeps and gets the result after the next reception
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[7],
						owner -> {
							try {
								double tmp = 20/ACCELERATION_FACTOR;
								Duration d = Duration.ofSeconds((long)(tmp+tmp/2));
								System.out.println("STEP 7/"+(NBSTEPS-1)+" ("+STEPS[7]+"s) : WindTurbine gets a message future for 'channel0',"
										+ "\nsleeps for "+(d.getSeconds()*ACCELERATION_FACTOR)+" seconds and gets the result after the next reception");
								owner.runTask(c -> ((WindTurbineTest)owner).waitMessageFuture("channel0",d));
							} catch (Exception e) {
								System.err.println("WindTurbine, step 7 message waiting (future) with exception " + e);
							}
						}),
				// MeteoStation publishes a message on 'channel0'
				new TestStep(
						CLOCK_URI, MS_URI, step_Instants[8],
						owner -> {
							try {
								System.out.println("STEP 8/"+(NBSTEPS-1)+" ("+STEPS[8]+"s) : MeteoStation publishes a message on 'channel0'");
								((MeteoStationTest)owner).publishWindData(2, 2, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation, step 8 wind data publication failed with exception " + e);
							}
						}),
				// WindTurbine waits a message from 'channel0'
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[9],
						owner -> {
							try {
								System.out.println("STEP 9/"+(NBSTEPS-1)+" ("+STEPS[9]+"s) : WindTurbine waits a message from 'channel0'");
								owner.runTask(c -> ((WindTurbineTest)owner).waitMessage("channel0"));
							} catch (Exception e) {
								System.err.println("WindTurbine, step 9 message waiting with exception " + e);
							}
						}),
				// WindTurbine gets a message future for 'channel0', sleeps 0 seconds and blocks on getting the result
				new TestStep(
						CLOCK_URI, WT_URI, step_Instants[10],
						owner -> {
							try {
								Duration d = Duration.ofSeconds(0);
								System.out.println("STEP 10/"+(NBSTEPS-1)+" ("+STEPS[10]+"s) : WindTurbine gets a message future for 'channel0',"
										+ "\nsleeps for "+(d.getSeconds()*ACCELERATION_FACTOR)+" seconds and blocks on getting the result");
								owner.runTask(c -> ((WindTurbineTest)owner).waitMessageFuture("channel0",d));
							} catch (Exception e) {
								System.err.println("WindTurbine, step 10 message waiting (future) with exception " + e);
							}
						}),
				// MeteoStation publishes 3 messages on 'channel0', each got by each possible reception method
				new TestStep(
						CLOCK_URI, MS_URI, step_Instants[11],
						owner -> {
							try {
								System.out.println("STEP 11/"+(NBSTEPS-1)+" ("+STEPS[11]+"s) : MeteoStation publishes 3 messages on 'channel0', each got by each possible reception method\n"
										+ "(normal reception appears in the log window)");
								((MeteoStationTest)owner).publishWindData(2, 2, "channel0", false);
								((MeteoStationTest)owner).publishWindData(2, 2, "channel0", false);
								((MeteoStationTest)owner).publishWindData(2, 2, "channel0", false);
							} catch (Exception e) {
								System.err.println("MeteoStation, step 11 wind datas publication failed with exception " + e);
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
				new Object[] {1, new PositionData(0, 4), 2, nbMaxChannelsSTD, nbMaxChannelsPREM, brokerNbThreads, 0, 
						brokerNbFreeLoadersThreads, brokerNbUnloadersThreads, brokerNbSendersThreads, 
						1, "BrokerGossipInboundPortURI", new ArrayList<>(), new ArrayList<>()}); // Gossip protocol unused here
		assert this.isDeployedComponent(broker_URI);
		this.toggleTracing(broker_URI);
		
		// Creation of 1 MeteoStation
		String meteoStation_URI_verif = AbstractComponent.createComponent(
				MeteoStationTest.class.getCanonicalName(),
				new Object[] { MSReceivingInboundPortURI, RegistrationClass.STANDARD, stationNbThreads, 1, 
						stationPosition, MS_URI, testScenario, 1});
		assert (MS_URI).equals(meteoStation_URI_verif);
		assert this.isDeployedComponent(MS_URI);
		this.toggleTracing(MS_URI);
		
		// Creation of 1 WindTurbine
		String tmp_windTurbine_URI = AbstractComponent.createComponent(WindTurbineTest.class.getCanonicalName(),
				new Object[] { WindTurbineReceivingInboundPortURI, RegistrationClass.STANDARD, windTurbineNbThreads, 1, 
						windTurbinePosition, MeteoAlertI.Level.ORANGE, WT_URI, testScenario, 1, windTurbineWindFilter, windTurbineAlertFilter});
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