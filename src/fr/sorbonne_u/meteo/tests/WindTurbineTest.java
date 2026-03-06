package fr.sorbonne_u.meteo.tests;

import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.utils.tests.TestScenario;
import fr.sorbonne_u.cps.meteo.interfaces.MeteoAlertI.LevelI;
import fr.sorbonne_u.meteo.components.WindTurbine;
import fr.sorbonne_u.meteo.data.PositionData;
import fr.sorbonne_u.utils.aclocks.ClocksServer;
import fr.sorbonne_u.cps.pubsub.interfaces.PublishingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.ReceivingCI;
import fr.sorbonne_u.cps.pubsub.interfaces.RegistrationCI;
import fr.sorbonne_u.exceptions.PreconditionException;

/**
 * Test class representing a {@link WindTurbine}, following a predefined scenario.
 * 
 * <p>The scenario is described in the {@link CVMTest} class.</p>
 * 
 * <p>The class does extend {@link WindTurbine} because it only listens by default, while here it has also defined actions to make.</p>
 * 
 * @author Kevin TANG
 * @author David VADIMON
 */
public class WindTurbineTest extends WindTurbine {
	
	// Scenario
	protected final TestScenario testScenario;

	//--------------------------------------------------------------------------
	// Constructor
	//--------------------------------------------------------------------------
	
	/**
     * Creates a wind turbine component for the publish/subscribe system.
     * 
     * <p>Test version extending {@link WindTurbine}, taking a {@code reflectionInboundPortURI} as a parameter.</p>
     * 
     * <p><strong>Contract</strong></p>
     * <pre>
     * pre  {@code registrationOutboundPortURI != null && !registrationOutboundPortURI.isEmpty()}
     * pre  {@code publishingOutboundPortURI != null && !publishingOutboundPortURI.isEmpty()}
     * pre  {@code receivingInboundPortURI != null && !receivingInboundPortURI.isEmpty()}
	 * pre	{@code nbThreads >= 0 && nbSchedulableThreads >= 0}
     * pre  {@code pos != null}
     * pre  {@code maxWorkingLevel != null}
	 * pre  {@code testScenario != null}
     * post {@code true} // no postcondition.
     * </pre>
     * 
     * @param registrationOutboundPortURI URI of the outbound port requesting {@link RegistrationCI}.
     * @param publishingOutboundPortURI   URI of the outbound port requesting {@link PublishingCI}.
     * @param receivingInboundPortURI     URI of the inbound port providing {@link ReceivingCI}.
	 * @param nbThreads					  Number of threads to be created in the component pool.
	 * @param nbSchedulableThreads		  Number of threads to be created in the component schedulable pool.
     * @param pos                         The position of the wind turbine.
     * @param maxWorkingLevel             Maximum alert level (inclusive) at which the turbine operates.
	 * @param testScenario					Test scenario in which this component participate.
	 * @param reflectionInboundPortURI		URI of the reflection inbound port to be created in this component.
     * @throws Exception                  If a problem occurs during initialization or port publication.
     */
	protected WindTurbineTest(String registrationOutboundPortURI, String publishingOutboundPortURI, String receivingInboundPortURI, 
			int nbThreads, int nbSchedulableThreads, PositionData pos,LevelI maxWorkingLevel, TestScenario testScenario, String reflectionInboundPortURI) throws Exception {
		super(registrationOutboundPortURI, publishingOutboundPortURI, receivingInboundPortURI, 1, 0, pos, maxWorkingLevel,
				reflectionInboundPortURI);
		// Some preconditions are checked inside the superconstructor while others are checked here
		assert(testScenario != null) : new PreconditionException("testScenario cannot be null");	
		
		this.testScenario = testScenario;
		
		// Invariant checks
		AbstractComponent.checkImplementationInvariant(this);
		AbstractComponent.checkInvariant(this);
	}

	// --------------------------------------------------------------------------
	// Component life-cycle
	// --------------------------------------------------------------------------

	@Override
	public void execute() throws Exception {
		super.execute();
		
		// Initialization of the clock
		this.initialiseClock(
				ClocksServer.STANDARD_INBOUNDPORT_URI,
				this.testScenario.getClockURI());
		// Component ready for executing the scenario
		this.executeTestScenario(this.testScenario);
	}
	
	
}
