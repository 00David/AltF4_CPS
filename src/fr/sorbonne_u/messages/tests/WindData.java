package fr.sorbonne_u.messages.tests;

import fr.sorbonne_u.cps.meteo.interfaces.PositionI;
import fr.sorbonne_u.cps.meteo.interfaces.WindDataI;

public class WindData implements WindDataI {
	
	
	private static final long serialVersionUID = 1L;
	private PositionI position;

	@Override
	public PositionI getPosition() {
		return this.position;
	}

	@Override
	public double xComponent() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double yComponent() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double force() {
		// TODO Auto-generated method stub
		return 0;
	}

}
