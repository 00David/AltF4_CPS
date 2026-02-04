package fr.sorbonne_u;

import fr.sorbonne_u.broker.components.Broker;
import fr.sorbonne_u.clients.components.Client;
import fr.sorbonne_u.components.AbstractComponent;
import fr.sorbonne_u.components.cvm.AbstractCVM;
import fr.sorbonne_u.components.helpers.CVMDebugModes;

public class CVM extends AbstractCVM {

	protected final String clientRegistrationOutboundPortURI;
	protected final String clientPublishingOutboundPortURI;
	protected final String clientReceivingInboundPortURI;
	
	protected String uriBroker;
	protected String uriClient;
	
	public CVM() throws Exception{
		super();
		clientRegistrationOutboundPortURI = "registration-outbound-port-client";
		clientPublishingOutboundPortURI = "publishing-outbound-port-client";
		clientReceivingInboundPortURI = "client-port";
	}
	
	@Override
	public void	deploy() throws Exception{
		assert	!this.deploymentDone() ;
		// ---------------------------------------------------------------------
		// Configuration phase
		// ---------------------------------------------------------------------

		/*AbstractCVM.DEBUG_MODE.add(CVMDebugModes.LIFE_CYCLE);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.INTERFACES);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.PORTS);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.CONNECTING);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.CALLING);
		AbstractCVM.DEBUG_MODE.add(CVMDebugModes.EXECUTOR_SERVICES);*/
		
		// ---------------------------------------------------------------------
		// Creation phase
		// ---------------------------------------------------------------------
		
		// création du Broker
		this.uriBroker = AbstractComponent.createComponent(
				Broker.class.getCanonicalName(),
				new Object[] {});
		assert this.isDeployedComponent(this.uriBroker);

		
		this.toggleTracing(this.uriBroker);
		this.toggleLogging(this.uriBroker);

		// création d'un Client
		this.uriClient = AbstractComponent.createComponent(
				Client.class.getCanonicalName(),
				new Object[] { clientRegistrationOutboundPortURI, clientPublishingOutboundPortURI, clientReceivingInboundPortURI });
		assert this.isDeployedComponent(this.uriClient);

		this.toggleTracing(this.uriClient);
		this.toggleLogging(this.uriClient);

		// --------------------------------------------------------------------
		// Deployment done
		// --------------------------------------------------------------------

		super.deploy();
		assert this.deploymentDone();
	}
	
	public static void main(String[] args) {
		try {
			CVM a = new CVM() ;
			
			long duree = 20000L; // en ms, durée d'execution
			a.startStandardLifeCycle(duree) ; // démarrage

			System.exit(0) ;
		} catch (Throwable e) {
			e.printStackTrace();
		}
	}
}
